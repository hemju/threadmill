package com.hemju.threadmill.core.engine;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobRelationship;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.store.JobSearch;
import com.hemju.threadmill.core.store.JobStore;

/**
 * Workflow / job-chaining interceptor.
 *
 * <p>When a job succeeds, this interceptor finds any successor jobs that
 * named it as their workflow predecessor (relationship kind
 * {@link JobRelationship.Kind#WORKFLOW_STEP}) and which are currently in
 * {@code AWAITING}, and transitions them to {@code ENQUEUED}. A successor
 * may already have started another path (e.g. been deleted); the
 * interceptor tolerates that.
 *
 * <p>When a workflow step fails permanently or is quarantined, still-waiting
 * successors are abandoned by transitioning them to {@code DELETED}. This
 * keeps workflow-root concurrency from being held forever by a descendant
 * that can no longer become runnable.
 *
 * <p>On batch members ({@link JobRelationship.Kind#BATCH_MEMBER}), the
 * interceptor does nothing: batch completion is signalled by the batch layer.
 */
public final class WorkflowInterceptor implements JobInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(WorkflowInterceptor.class);

    private final JobStore store;

    public WorkflowInterceptor(JobStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    @Override
    public void onProcessingSucceeded(Job job, JobExecutionContext ctx) {
        promoteAwaitingSuccessorsOf(job.id());
    }

    @Override
    public void onProcessingFailed(Job job, JobExecutionContext ctx, Throwable cause, FailureCause causeKind) {
        if (job.currentState() != JobState.FAILED && job.currentState() != JobState.QUARANTINED) {
            return;
        }
        abandonAwaitingSuccessorsOf(job.id());
    }

    /**
     * Recovery sweep for AWAITING workflow children whose predecessor already
     * reached a terminal state but whose promote/abandon never ran — e.g. the
     * node crashed in the window between the predecessor's terminal save and
     * this interceptor's hook. Without this, such children stay AWAITING
     * forever and their workflow-root concurrency key is held forever. Promotes
     * children of a SUCCEEDED predecessor, abandons children of a failed /
     * quarantined / deleted / vanished predecessor, and leaves children of a
     * still-active predecessor alone. Idempotent; safe to run on every node's
     * maintenance leader periodically.
     */
    public void reconcileOrphanedAwaitingChildren(int max) {
        if (store.capabilities().supportsExactCounts()
                && store.countsByState().getOrDefault(JobState.AWAITING, 0L) == 0L) {
            return;
        }
        List<Job> awaiting = store.searchJobs(new JobSearch(JobState.AWAITING, null, null, max, 0));
        var handledParents = new HashSet<JobId>();
        for (Job child : awaiting) {
            if (child.relationship().isEmpty()) continue;
            JobRelationship rel = child.relationship().get();
            if (rel.kind() != JobRelationship.Kind.WORKFLOW_STEP) continue;
            JobId parentId = rel.parentId();
            if (!handledParents.add(parentId)) continue;
            JobState parentState =
                    store.findById(parentId).map(Job::currentState).orElse(null);
            if (parentState == JobState.SUCCEEDED) {
                promoteAwaitingSuccessorsOf(parentId);
            } else if (parentState == null
                    || parentState == JobState.FAILED
                    || parentState == JobState.QUARANTINED
                    || parentState == JobState.DELETED) {
                // Failed (no pending retry), quarantined, deleted, or hard-deleted
                // by retention: the predecessor can never promote this child, so
                // abandon the subtree. ENQUEUED/SCHEDULED/PROCESSING/AWAITING all
                // mean the predecessor is still in flight — leave the child be.
                // (FAILED is not state.isTerminal() because a retry can resurrect
                // it, but a predecessor sitting in FAILED at this cadence is done.)
                abandonAwaitingSuccessorsOf(parentId);
            }
        }
    }

    /** Children are drained in batches of this size until exhausted. */
    private static final int CHILD_BATCH = 100;

    private void promoteAwaitingSuccessorsOf(JobId predecessorId) {
        drainAwaitingChildren(predecessorId, "promote", candidate -> {
            long v = candidate.version();
            candidate.transitionTo(JobState.ENQUEUED, Instant.now(), "engine.workflow-promote", null);
            store.saveAtomic(candidate, v);
        });
    }

    private void abandonAwaitingSuccessorsOf(JobId rootPredecessorId) {
        // Explicit work queue instead of per-level recursion: a deep chain
        // must not risk StackOverflowError inside an interceptor.
        var pending = new ArrayDeque<JobId>();
        pending.add(rootPredecessorId);
        while (!pending.isEmpty()) {
            JobId predecessorId = pending.poll();
            drainAwaitingChildren(predecessorId, "abandon", candidate -> {
                long v = candidate.version();
                candidate.transitionTo(JobState.DELETED, Instant.now(), "engine.workflow-abandon", null);
                store.saveAtomic(candidate, v);
                pending.add(candidate.id());
            });
        }
    }

    /**
     * Apply {@code action} to every AWAITING workflow-step child of
     * {@code predecessorId}, draining in batches until exhausted. Fan-out
     * beyond one batch is handled by refetching: every promoted / abandoned
     * child leaves {@code AWAITING}, so the loop terminates. An iteration
     * that makes no progress (e.g. persistent save failures) stops the drain
     * instead of spinning.
     */
    private void drainAwaitingChildren(JobId predecessorId, String what, Consumer<Job> action) {
        // The count short-circuit is only sound when counts are exact; a
        // store advertising approximate counts could report 0 while a
        // successor exists, permanently stranding it.
        if (store.capabilities().supportsExactCounts()
                && store.countsByState().getOrDefault(JobState.AWAITING, 0L) == 0L) {
            return;
        }
        while (true) {
            List<Job> batch = store.findAwaitingByParent(predecessorId, CHILD_BATCH);
            int progressed = 0;
            for (Job candidate : batch) {
                if (candidate.relationship().isEmpty()) continue;
                JobRelationship rel = candidate.relationship().get();
                if (rel.kind() != JobRelationship.Kind.WORKFLOW_STEP) continue;
                if (!rel.parentId().equals(predecessorId)) continue;
                try {
                    action.accept(candidate);
                    progressed++;
                } catch (StaleJobException ignored) {
                    // Another node beat us; the refetch sees the fresh state.
                    progressed++;
                } catch (Throwable t) {
                    LOG.warn("Failed to {} workflow successor {} of {}", what, candidate.id(), predecessorId, t);
                }
            }
            if (batch.size() < CHILD_BATCH || progressed == 0) {
                return;
            }
        }
    }
}
