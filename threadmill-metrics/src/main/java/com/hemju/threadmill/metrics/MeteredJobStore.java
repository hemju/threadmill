package com.hemju.threadmill.metrics;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

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

/** Store decorator used by {@link ThreadmillMetrics#meteredStore()}. */
final class MeteredJobStore implements JobStore {

  private final JobStore delegate;
  private final ThreadmillMetrics metrics;

  MeteredJobStore(JobStore delegate, ThreadmillMetrics metrics) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
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
    delegate.verifyWritable();
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
    writeVoid("insert", () -> delegate.insert(job));
  }

  @Override
  public List<JobId> insertAll(List<Job> jobs) {
    return write("insert_all", () -> delegate.insertAll(jobs));
  }

  @Override
  public EnqueueResult enqueueIfAbsent(Job job, String dedupKey, Duration ttl, Instant now) {
    return write("enqueue_if_absent", () -> delegate.enqueueIfAbsent(job, dedupKey, ttl, now));
  }

  @Override
  public Optional<Job> findById(JobId id) {
    return delegate.findById(id);
  }

  @Override
  public void saveAtomic(Job job, long expectedVersion) {
    writeVoid("save_atomic", () -> delegate.saveAtomic(job, expectedVersion));
  }

  @Override
  public boolean softDelete(JobId id) {
    return write("soft_delete", () -> delegate.softDelete(id));
  }

  @Override
  public List<Job> claimReady(NodeId nodeId, String queue, int max, Instant heartbeatAt) {
    var started = System.nanoTime();
    try {
      return delegate.claimReady(nodeId, queue, max, heartbeatAt);
    } catch (RuntimeException failure) {
      metrics.recordClaimFailure();
      throw failure;
    } finally {
      metrics.recordClaimLatency(Duration.ofNanos(System.nanoTime() - started));
    }
  }

  @Override
  public void pauseQueue(String queue, String reason) {
    writeVoid("pause_queue", () -> delegate.pauseQueue(queue, reason));
  }

  @Override
  public void resumeQueue(String queue) {
    writeVoid("resume_queue", () -> delegate.resumeQueue(queue));
  }

  @Override
  public Set<String> listPausedQueues() {
    return delegate.listPausedQueues();
  }

  @Override
  public void touchOwnerHeartbeat(NodeId nodeId, Instant now) {
    writeVoid("touch_owner_heartbeat", () -> delegate.touchOwnerHeartbeat(nodeId, now));
  }

  @Override
  public boolean saveExecutionUpdate(Job job, NodeId nodeId) {
    return write("save_execution_update", () -> delegate.saveExecutionUpdate(job, nodeId));
  }

  @Override
  public void recordNodeHeartbeat(NodeId nodeId, Instant now) {
    writeVoid("record_node_heartbeat", () -> delegate.recordNodeHeartbeat(nodeId, now));
  }

  @Override
  public Optional<Instant> readNodeHeartbeat(NodeId nodeId) {
    return delegate.readNodeHeartbeat(nodeId);
  }

  @Override
  public boolean acquireOrRenewMaintenanceLease(NodeId nodeId, Duration leaseDuration) {
    return write(
        "acquire_or_renew_maintenance_lease",
        () -> delegate.acquireOrRenewMaintenanceLease(nodeId, leaseDuration));
  }

  @Override
  public void releaseMaintenanceLease(NodeId nodeId) {
    writeVoid("release_maintenance_lease", () -> delegate.releaseMaintenanceLease(nodeId));
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
    return write(
        "delete_node_heartbeats_older_than", () -> delegate.deleteNodeHeartbeatsOlderThan(cutoff));
  }

  @Override
  public long deleteExpiredDedupKeys(Instant now, int max) {
    return write("delete_expired_dedup_keys", () -> delegate.deleteExpiredDedupKeys(now, max));
  }

  @Override
  public List<Job> findByHandlerSignature(String handlerType, int max) {
    return delegate.findByHandlerSignature(handlerType, max);
  }

  @Override
  public long deleteFinishedOlderThan(Instant cutoff, JobState state, int max) {
    return write(
        "delete_finished_older_than", () -> delegate.deleteFinishedOlderThan(cutoff, state, max));
  }

  @Override
  public List<Job> findAwaitingByParent(JobId parentId, int max) {
    return delegate.findAwaitingByParent(parentId, max);
  }

  @Override
  public boolean tryAcquireMutex(String name, String holder, Duration leaseDuration) {
    return write("try_acquire_mutex", () -> delegate.tryAcquireMutex(name, holder, leaseDuration));
  }

  @Override
  public void releaseMutex(String name, String holder) {
    writeVoid("release_mutex", () -> delegate.releaseMutex(name, holder));
  }

  @Override
  public boolean replaceJob(JobId id, long expectedVersion, JobReplacement replacement) {
    return write("replace_job", () -> delegate.replaceJob(id, expectedVersion, replacement));
  }

  @Override
  public void upsertCronTask(CronTask task) {
    writeVoid("upsert_cron_task", () -> delegate.upsertCronTask(task));
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
    writeVoid("delete_cron_task", () -> delegate.deleteCronTask(name));
  }

  @Override
  public void recordCronTaskOwnership(String namespace, String taskName) {
    writeVoid(
        "record_cron_task_ownership", () -> delegate.recordCronTaskOwnership(namespace, taskName));
  }

  @Override
  public Set<String> listCronTaskNamesOwnedBy(String namespace) {
    return delegate.listCronTaskNamesOwnedBy(namespace);
  }

  @Override
  public void upsertCronTaskState(CronTaskScheduleState state) {
    writeVoid("upsert_cron_task_state", () -> delegate.upsertCronTaskState(state));
  }

  @Override
  public Optional<CronTaskScheduleState> findCronTaskState(String name) {
    return delegate.findCronTaskState(name);
  }

  @Override
  public NudgeOutcome requestCronNudge(String taskName, Instant requestedAt) {
    return write("request_cron_nudge", () -> delegate.requestCronNudge(taskName, requestedAt));
  }

  @Override
  public void clearCronNudge(String taskName, long observedRevision) {
    writeVoid("clear_cron_nudge", () -> delegate.clearCronNudge(taskName, observedRevision));
  }

  private <T> T write(String operation, Supplier<T> action) {
    try {
      return action.get();
    } catch (RuntimeException rejected) {
      metrics.recordRejectedWrite(operation);
      throw rejected;
    }
  }

  private void writeVoid(String operation, Runnable action) {
    write(operation, () -> {
      action.run();
      return null;
    });
  }
}
