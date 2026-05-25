package com.hemju.threadmill.tracing;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobReplacement;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.store.JobSearch;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.core.store.JobStoreCapabilities;
import com.hemju.threadmill.core.store.NodeHeartbeat;

/** {@link JobStore} decorator that emits OpenTelemetry spans for store operations. */
public final class TracingJobStore implements JobStore {

    private final JobStore delegate;
    private final Tracer tracer;

    TracingJobStore(JobStore delegate, Tracer tracer) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.tracer = Objects.requireNonNull(tracer, "tracer");
    }

    @Override
    public JobStoreCapabilities capabilities() {
        return delegate.capabilities();
    }

    @Override
    public String describe() {
        return delegate.describe();
    }

    @Override
    public JobStore delegate() {
        return delegate;
    }

    @Override
    public void verifyWritable() {
        traceVoid("threadmill.store.verify_writable", span -> delegate.verifyWritable());
    }

    @Override
    public void insert(Job job) {
        traceVoid("threadmill.store.insert", span -> {
            tagJob(span, job);
            delegate.insert(job);
        });
    }

    @Override
    public List<JobId> insertAll(List<Job> jobs) {
        return trace("threadmill.store.insert_all", span -> {
            span.setAttribute(ThreadmillTracing.JOB_COUNT, jobs.size());
            return delegate.insertAll(jobs);
        });
    }

    @Override
    public EnqueueResult enqueueIfAbsent(Job job, String dedupKey, Duration ttl, Instant now) {
        return trace("threadmill.store.enqueue_if_absent", span -> {
            tagJob(span, job);
            return delegate.enqueueIfAbsent(job, dedupKey, ttl, now);
        });
    }

    @Override
    public Optional<Job> findById(JobId id) {
        return trace("threadmill.store.find_by_id", span -> {
            span.setAttribute(ThreadmillTracing.JOB_ID, id.toString());
            return delegate.findById(id);
        });
    }

    @Override
    public void saveAtomic(Job job, long expectedVersion) {
        traceVoid("threadmill.store.save_atomic", span -> {
            tagJob(span, job);
            delegate.saveAtomic(job, expectedVersion);
        });
    }

    @Override
    public boolean softDelete(JobId id) {
        return trace("threadmill.store.soft_delete", span -> {
            span.setAttribute(ThreadmillTracing.JOB_ID, id.toString());
            return delegate.softDelete(id);
        });
    }

    @Override
    public List<Job> claimReady(NodeId nodeId, String queue, int max, Instant heartbeatAt) {
        return trace("threadmill.store.claim_ready", span -> {
            span.setAttribute(ThreadmillTracing.NODE_ID, nodeId.toString());
            span.setAttribute(ThreadmillTracing.QUEUE, queue);
            var claimed = delegate.claimReady(nodeId, queue, max, heartbeatAt);
            span.setAttribute(ThreadmillTracing.CLAIMED_COUNT, claimed.size());
            return claimed;
        });
    }

    @Override
    public void pauseQueue(String queue, String reason) {
        traceVoid("threadmill.store.pause_queue", span -> {
            span.setAttribute(ThreadmillTracing.QUEUE, queue);
            delegate.pauseQueue(queue, reason);
        });
    }

    @Override
    public void resumeQueue(String queue) {
        traceVoid("threadmill.store.resume_queue", span -> {
            span.setAttribute(ThreadmillTracing.QUEUE, queue);
            delegate.resumeQueue(queue);
        });
    }

    @Override
    public Set<String> listPausedQueues() {
        return trace("threadmill.store.list_paused_queues", span -> delegate.listPausedQueues());
    }

    @Override
    public void touchOwnerHeartbeat(NodeId nodeId, Instant now) {
        traceVoid("threadmill.store.touch_owner_heartbeat", span -> {
            span.setAttribute(ThreadmillTracing.NODE_ID, nodeId.toString());
            delegate.touchOwnerHeartbeat(nodeId, now);
        });
    }

    @Override
    public boolean saveExecutionUpdate(Job job, NodeId nodeId) {
        return trace("threadmill.store.save_execution_update", span -> {
            tagJob(span, job);
            span.setAttribute(ThreadmillTracing.NODE_ID, nodeId.toString());
            return delegate.saveExecutionUpdate(job, nodeId);
        });
    }

    @Override
    public void recordNodeHeartbeat(NodeId nodeId, Instant now) {
        traceVoid("threadmill.store.record_node_heartbeat", span -> {
            span.setAttribute(ThreadmillTracing.NODE_ID, nodeId.toString());
            delegate.recordNodeHeartbeat(nodeId, now);
        });
    }

    @Override
    public Optional<Instant> readNodeHeartbeat(NodeId nodeId) {
        return trace("threadmill.store.read_node_heartbeat", span -> {
            span.setAttribute(ThreadmillTracing.NODE_ID, nodeId.toString());
            return delegate.readNodeHeartbeat(nodeId);
        });
    }

    @Override
    public boolean acquireOrRenewMaintenanceLease(NodeId nodeId, Duration leaseDuration) {
        return trace("threadmill.store.acquire_or_renew_maintenance_lease", span -> {
            span.setAttribute(ThreadmillTracing.NODE_ID, nodeId.toString());
            return delegate.acquireOrRenewMaintenanceLease(nodeId, leaseDuration);
        });
    }

    @Override
    public void releaseMaintenanceLease(NodeId nodeId) {
        traceVoid("threadmill.store.release_maintenance_lease", span -> {
            span.setAttribute(ThreadmillTracing.NODE_ID, nodeId.toString());
            delegate.releaseMaintenanceLease(nodeId);
        });
    }

    @Override
    public Optional<NodeId> readMaintenanceLeaseOwner() {
        return trace("threadmill.store.read_maintenance_lease_owner", span -> delegate.readMaintenanceLeaseOwner());
    }

    @Override
    public List<Job> findDueForPromotion(Instant now, int max) {
        return trace("threadmill.store.find_due_for_promotion", span -> delegate.findDueForPromotion(now, max));
    }

    @Override
    public List<Job> findOrphaned(Instant heartbeatExpiry, int max) {
        return trace("threadmill.store.find_orphaned", span -> delegate.findOrphaned(heartbeatExpiry, max));
    }

    @Override
    public Map<JobState, Long> countsByState() {
        return trace("threadmill.store.counts_by_state", span -> delegate.countsByState());
    }

    @Override
    public Map<String, Long> queueDepths() {
        return trace("threadmill.store.queue_depths", span -> delegate.queueDepths());
    }

    @Override
    public List<String> listEnqueuedQueues() {
        return trace("threadmill.store.list_enqueued_queues", span -> delegate.listEnqueuedQueues());
    }

    @Override
    public List<Job> searchJobs(JobSearch search) {
        return trace("threadmill.store.search_jobs", span -> {
            if (search.queue() != null) span.setAttribute(ThreadmillTracing.QUEUE, search.queue());
            if (search.handlerType() != null) span.setAttribute(ThreadmillTracing.HANDLER, search.handlerType());
            var jobs = delegate.searchJobs(search);
            span.setAttribute(ThreadmillTracing.JOB_COUNT, jobs.size());
            return jobs;
        });
    }

    @Override
    public Optional<Instant> oldestEnqueuedAt(String queue) {
        return trace("threadmill.store.oldest_enqueued_at", span -> {
            span.setAttribute(ThreadmillTracing.QUEUE, queue);
            return delegate.oldestEnqueuedAt(queue);
        });
    }

    @Override
    public Optional<Instant> oldestProcessingHeartbeat() {
        return trace("threadmill.store.oldest_processing_heartbeat", span -> delegate.oldestProcessingHeartbeat());
    }

    @Override
    public List<NodeHeartbeat> listNodeHeartbeats() {
        return trace("threadmill.store.list_node_heartbeats", span -> delegate.listNodeHeartbeats());
    }

    @Override
    public long deleteNodeHeartbeatsOlderThan(Instant cutoff) {
        return trace(
                "threadmill.store.delete_node_heartbeats_older_than",
                span -> delegate.deleteNodeHeartbeatsOlderThan(cutoff));
    }

    @Override
    public long deleteExpiredDedupKeys(Instant now, int max) {
        return trace("threadmill.store.delete_expired_dedup_keys", span -> delegate.deleteExpiredDedupKeys(now, max));
    }

    @Override
    public List<Job> findByHandlerSignature(String handlerType, int max) {
        return trace("threadmill.store.find_by_handler_signature", span -> {
            span.setAttribute(ThreadmillTracing.HANDLER, handlerType);
            return delegate.findByHandlerSignature(handlerType, max);
        });
    }

    @Override
    public long deleteFinishedOlderThan(Instant cutoff, JobState state, int max) {
        return trace("threadmill.store.delete_finished_older_than", span -> {
            span.setAttribute(ThreadmillTracing.FINAL_STATE, state.name());
            return delegate.deleteFinishedOlderThan(cutoff, state, max);
        });
    }

    @Override
    public List<Job> findAwaitingByParent(JobId parentId, int max) {
        return trace("threadmill.store.find_awaiting_by_parent", span -> {
            span.setAttribute(ThreadmillTracing.JOB_ID, parentId.toString());
            return delegate.findAwaitingByParent(parentId, max);
        });
    }

    @Override
    public boolean tryAcquireMutex(String name, String holder, Duration leaseDuration) {
        return trace(
                "threadmill.store.try_acquire_mutex", span -> delegate.tryAcquireMutex(name, holder, leaseDuration));
    }

    @Override
    public void releaseMutex(String name, String holder) {
        traceVoid("threadmill.store.release_mutex", span -> delegate.releaseMutex(name, holder));
    }

    @Override
    public boolean replaceJob(JobId id, long expectedVersion, JobReplacement replacement) {
        return trace("threadmill.store.replace_job", span -> {
            span.setAttribute(ThreadmillTracing.JOB_ID, id.toString());
            return delegate.replaceJob(id, expectedVersion, replacement);
        });
    }

    @Override
    public void upsertCronTask(CronTask task) {
        traceVoid("threadmill.store.upsert_cron_task", span -> delegate.upsertCronTask(task));
    }

    @Override
    public Optional<CronTask> findCronTask(String name) {
        return trace("threadmill.store.find_cron_task", span -> delegate.findCronTask(name));
    }

    @Override
    public List<CronTask> listCronTasks() {
        return trace("threadmill.store.list_cron_tasks", span -> delegate.listCronTasks());
    }

    @Override
    public void deleteCronTask(String name) {
        traceVoid("threadmill.store.delete_cron_task", span -> delegate.deleteCronTask(name));
    }

    @Override
    public void recordCronTaskOwnership(String namespace, String taskName) {
        traceVoid(
                "threadmill.store.record_cron_task_ownership",
                span -> delegate.recordCronTaskOwnership(namespace, taskName));
    }

    @Override
    public Set<String> listCronTaskNamesOwnedBy(String namespace) {
        return trace(
                "threadmill.store.list_cron_task_names_owned_by", span -> delegate.listCronTaskNamesOwnedBy(namespace));
    }

    @Override
    public void upsertCronTaskState(CronTaskScheduleState state) {
        traceVoid("threadmill.store.upsert_cron_task_state", span -> delegate.upsertCronTaskState(state));
    }

    @Override
    public Optional<CronTaskScheduleState> findCronTaskState(String name) {
        return trace("threadmill.store.find_cron_task_state", span -> delegate.findCronTaskState(name));
    }

    private <T> T trace(String name, SpanWork<T> work) {
        Span span = tracer.spanBuilder(name)
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute(ThreadmillTracing.STORE, delegate.describe())
                .startSpan();
        Scope scope = span.makeCurrent();
        try {
            return work.run(span);
        } catch (RuntimeException e) {
            span.recordException(e);
            span.setStatus(
                    StatusCode.ERROR, e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
            throw e;
        } finally {
            try {
                scope.close();
            } finally {
                span.end();
            }
        }
    }

    private void traceVoid(String name, SpanVoidWork work) {
        trace(name, span -> {
            work.run(span);
            return null;
        });
    }

    private static void tagJob(Span span, Job job) {
        span.setAttribute(ThreadmillTracing.JOB_ID, job.id().toString());
        span.setAttribute(ThreadmillTracing.QUEUE, job.queue());
        span.setAttribute(ThreadmillTracing.HANDLER, job.spec().handlerType());
    }

    @FunctionalInterface
    private interface SpanWork<T> {
        T run(Span span);
    }

    @FunctionalInterface
    private interface SpanVoidWork {
        void run(Span span);
    }
}
