package com.hemju.threadmill.test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobReplacement;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.RemoteWakeChannel;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.store.JobSearch;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.core.store.JobStoreCapabilities;
import com.hemju.threadmill.core.store.NodeHeartbeat;

/**
 * A {@link JobStore} decorator that forwards every operation to a delegate.
 * Test code subclasses it to inject faults or observe calls on individual
 * operations without re-implementing the whole SPI.
 */
public class ForwardingJobStore implements JobStore {

    private final JobStore delegate;

    public ForwardingJobStore(JobStore delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public JobStore delegate() {
        return delegate;
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
    public boolean supportsExternalTransactions() {
        return delegate.supportsExternalTransactions();
    }

    @Override
    public Optional<RemoteWakeChannel> createRemoteWakeChannel(String channelName) {
        return delegate.createRemoteWakeChannel(channelName);
    }

    @Override
    public void insert(Job job) {
        delegate.insert(job);
    }

    @Override
    public List<JobId> insertAll(List<Job> jobs) {
        return delegate.insertAll(jobs);
    }

    @Override
    public EnqueueResult enqueueIfAbsent(Job job, String dedupKey, Duration ttl, Instant now) {
        return delegate.enqueueIfAbsent(job, dedupKey, ttl, now);
    }

    @Override
    public Optional<Job> findById(JobId id) {
        return delegate.findById(id);
    }

    @Override
    public void saveAtomic(Job job, long expectedVersion) {
        delegate.saveAtomic(job, expectedVersion);
    }

    @Override
    public boolean softDelete(JobId id) {
        return delegate.softDelete(id);
    }

    @Override
    public List<Job> claimReady(NodeId nodeId, String queue, int max, Instant heartbeatAt) {
        return delegate.claimReady(nodeId, queue, max, heartbeatAt);
    }

    @Override
    public void pauseQueue(String queue, String reason) {
        delegate.pauseQueue(queue, reason);
    }

    @Override
    public void resumeQueue(String queue) {
        delegate.resumeQueue(queue);
    }

    @Override
    public Set<String> listPausedQueues() {
        return delegate.listPausedQueues();
    }

    @Override
    public void touchOwnerHeartbeat(NodeId nodeId, Instant now) {
        delegate.touchOwnerHeartbeat(nodeId, now);
    }

    @Override
    public boolean saveExecutionUpdate(Job job, NodeId nodeId) {
        return delegate.saveExecutionUpdate(job, nodeId);
    }

    @Override
    public void recordNodeHeartbeat(NodeId nodeId, Instant now) {
        delegate.recordNodeHeartbeat(nodeId, now);
    }

    @Override
    public Optional<Instant> readNodeHeartbeat(NodeId nodeId) {
        return delegate.readNodeHeartbeat(nodeId);
    }

    @Override
    public boolean acquireOrRenewMaintenanceLease(NodeId nodeId, Duration leaseDuration) {
        return delegate.acquireOrRenewMaintenanceLease(nodeId, leaseDuration);
    }

    @Override
    public void releaseMaintenanceLease(NodeId nodeId) {
        delegate.releaseMaintenanceLease(nodeId);
    }

    @Override
    public Optional<NodeId> readMaintenanceLeaseOwner() {
        return delegate.readMaintenanceLeaseOwner();
    }

    @Override
    public List<Job> findDueForPromotion(Instant now, int max) {
        return delegate.findDueForPromotion(now, max);
    }

    @Override
    public List<Job> findOrphaned(Instant heartbeatExpiry, int max) {
        return delegate.findOrphaned(heartbeatExpiry, max);
    }

    @Override
    public Map<JobState, Long> countsByState() {
        return delegate.countsByState();
    }

    @Override
    public Map<String, Long> queueDepths() {
        return delegate.queueDepths();
    }

    @Override
    public List<String> listEnqueuedQueues() {
        return delegate.listEnqueuedQueues();
    }

    @Override
    public List<Job> searchJobs(JobSearch search) {
        return delegate.searchJobs(search);
    }

    @Override
    public Optional<Instant> oldestEnqueuedAt(String queue) {
        return delegate.oldestEnqueuedAt(queue);
    }

    @Override
    public Optional<Instant> oldestProcessingHeartbeat() {
        return delegate.oldestProcessingHeartbeat();
    }

    @Override
    public List<NodeHeartbeat> listNodeHeartbeats() {
        return delegate.listNodeHeartbeats();
    }

    @Override
    public long deleteNodeHeartbeatsOlderThan(Instant cutoff) {
        return delegate.deleteNodeHeartbeatsOlderThan(cutoff);
    }

    @Override
    public long deleteExpiredDedupKeys(Instant now, int max) {
        return delegate.deleteExpiredDedupKeys(now, max);
    }

    @Override
    public List<Job> findByHandlerSignature(String handlerType, int max) {
        return delegate.findByHandlerSignature(handlerType, max);
    }

    @Override
    public long deleteFinishedOlderThan(Instant cutoff, JobState state, int max) {
        return delegate.deleteFinishedOlderThan(cutoff, state, max);
    }

    @Override
    public List<Job> findAwaitingByParent(JobId parentId, int max) {
        return delegate.findAwaitingByParent(parentId, max);
    }

    @Override
    public boolean tryAcquireMutex(String name, String holder, Duration leaseDuration) {
        return delegate.tryAcquireMutex(name, holder, leaseDuration);
    }

    @Override
    public void releaseMutex(String name, String holder) {
        delegate.releaseMutex(name, holder);
    }

    @Override
    public boolean replaceJob(JobId id, long expectedVersion, JobReplacement replacement) {
        return delegate.replaceJob(id, expectedVersion, replacement);
    }

    @Override
    public void upsertCronTask(CronTask task) {
        delegate.upsertCronTask(task);
    }

    @Override
    public Optional<CronTask> findCronTask(String name) {
        return delegate.findCronTask(name);
    }

    @Override
    public List<CronTask> listCronTasks() {
        return delegate.listCronTasks();
    }

    @Override
    public void deleteCronTask(String name) {
        delegate.deleteCronTask(name);
    }

    @Override
    public void recordCronTaskOwnership(String namespace, String taskName) {
        delegate.recordCronTaskOwnership(namespace, taskName);
    }

    @Override
    public Set<String> listCronTaskNamesOwnedBy(String namespace) {
        return delegate.listCronTaskNamesOwnedBy(namespace);
    }

    @Override
    public void upsertCronTaskState(CronTaskScheduleState state) {
        delegate.upsertCronTaskState(state);
    }

    @Override
    public Optional<CronTaskScheduleState> findCronTaskState(String name) {
        return delegate.findCronTaskState(name);
    }

    @Override
    public void verifyWritable() {
        delegate.verifyWritable();
    }
}
