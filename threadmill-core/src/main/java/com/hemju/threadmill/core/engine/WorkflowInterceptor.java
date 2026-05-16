package com.hemju.threadmill.core.engine;

import java.time.Instant;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobRelationship;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.handler.JobExecutionContext;
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

    private void promoteAwaitingSuccessorsOf(JobId predecessorId) {
        // Successors are persisted with relationship.parentId = predecessorId.
        // We rely on AWAITING being a small slice in steady state and use the
        // store-side findAwaitingByParent helper rather than a wider scan.
        for (Job candidate : findAwaitingChildrenOf(predecessorId)) {
            if (candidate.relationship().isEmpty()) continue;
            JobRelationship rel = candidate.relationship().get();
            if (rel.kind() != JobRelationship.Kind.WORKFLOW_STEP) continue;
            if (!rel.parentId().equals(predecessorId)) continue;
            try {
                long v = candidate.version();
                candidate.transitionTo(JobState.ENQUEUED, Instant.now(), "engine.workflow-promote", null);
                store.saveAtomic(candidate, v);
            } catch (StaleJobException ignored) {
                // Another node beat us; that's fine.
            } catch (Throwable t) {
                LOG.warn("Failed to promote workflow successor {} of {}", candidate.id(), predecessorId, t);
            }
        }
    }

    private void abandonAwaitingSuccessorsOf(JobId predecessorId) {
        for (Job candidate : findAwaitingChildrenOf(predecessorId)) {
            if (candidate.relationship().isEmpty()) continue;
            JobRelationship rel = candidate.relationship().get();
            if (rel.kind() != JobRelationship.Kind.WORKFLOW_STEP) continue;
            if (!rel.parentId().equals(predecessorId)) continue;
            try {
                long v = candidate.version();
                candidate.transitionTo(JobState.DELETED, Instant.now(), "engine.workflow-abandon", null);
                store.saveAtomic(candidate, v);
                abandonAwaitingSuccessorsOf(candidate.id());
            } catch (StaleJobException ignored) {
                // Another node beat us; that's fine.
            } catch (Throwable t) {
                LOG.warn("Failed to abandon workflow successor {} of {}", candidate.id(), predecessorId, t);
            }
        }
    }

    /**
     * Finds AWAITING jobs whose relationship's parentId equals {@code predecessorId}.
     * Implemented as a linear scan — workflows expect modest fan-out and this is
     * the simplest correct implementation for now. A store-side index can be
     * added later if profiling demands.
     */
    private Iterable<Job> findAwaitingChildrenOf(JobId predecessorId) {
        // We don't have a "by parent" SPI method. Iterate jobs by signature
        // is too narrow. Use a count-by-state hint to short-circuit when there
        // are no AWAITING jobs at all.
        if (store.countsByState().getOrDefault(JobState.AWAITING, 0L) == 0L) {
            return java.util.List.of();
        }
        // Fallback: ask the store for AWAITING jobs and filter client-side.
        // Most stores can answer this via findDueForPromotion semantics — but those
        // are SCHEDULED-specific. We add a small helper through the new SPI method
        // findAwaitingByParent later; for now, walk via a wide-ish call.
        return store.findAwaitingByParent(predecessorId, 100);
    }
}
