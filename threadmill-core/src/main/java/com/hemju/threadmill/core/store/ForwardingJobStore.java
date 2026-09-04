package com.hemju.threadmill.core.store;

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

/**
 * A {@link JobStore} decorator that forwards every SPI operation — including
 * the interface's {@code default} methods — to an immediate delegate.
 *
 * <p>This is the shared base for every store decorator: the tracing and
 * metrics decorators override only the operations they instrument, and test
 * code subclasses it to inject faults or observe individual calls without
 * re-implementing the whole SPI.
 *
 * <p>The forwarding of {@code default} methods is the load-bearing part. A
 * decorator that implements {@code JobStore} directly and forgets one of
 * {@link #describe()}, {@link #verifyWritable()},
 * {@link #supportsExternalTransactions()}, or
 * {@link #createRemoteWakeChannel(String)} still compiles, silently falls
 * back to the interface default, and therefore advertises different
 * capabilities than the store it wraps. Routing every decorator through this
 * class means a future SPI addition has exactly one place to be forwarded.
 *
 * <p>{@link #delegate()} always returns the immediate delegate and is final,
 * so framework integrations can unwrap a decorator chain one layer at a time.
 */
public class ForwardingJobStore implements JobStore {

  private final JobStore delegate;

  /**
   * Create a decorator around {@code delegate}.
   *
   * @param delegate the store every non-overridden operation forwards to
   */
  public ForwardingJobStore(JobStore delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  /** The immediate delegate; never {@code this}. */
  @Override
  public final JobStore delegate() {
    return delegate;
  }

  // ---------------------------------------------------------------- capabilities

  @Override
  public JobStoreCapabilities capabilities() {
    return delegate.capabilities();
  }

  @Override
  public String describe() {
    return delegate.describe();
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

  // ---------------------------------------------------------------- single-job ops

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

  // ---------------------------------------------------------------- claim & heartbeat

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

  // ---------------------------------------------------------------- housekeeping queries

  @Override
  public List<Job> findDueForPromotion(Instant now, int max) {
    return delegate.findDueForPromotion(now, max);
  }

  @Override
  public List<Job> findOrphaned(Instant heartbeatExpiry, int max) {
    return delegate.findOrphaned(heartbeatExpiry, max);
  }

  // ---------------------------------------------------------------- counts & search

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

  // ---------------------------------------------------------------- retention

  @Override
  public long deleteFinishedOlderThan(Instant cutoff, JobState state, int max) {
    return delegate.deleteFinishedOlderThan(cutoff, state, max);
  }

  // ---------------------------------------------------------------- relationships, mutexes,
  // replacement

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

  // ---------------------------------------------------------------- recurring tasks

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
  public NudgeOutcome requestCronNudge(String taskName, Instant requestedAt) {
    return delegate.requestCronNudge(taskName, requestedAt);
  }

  @Override
  public void clearCronNudge(String taskName, long observedRevision) {
    delegate.clearCronNudge(taskName, observedRevision);
  }
}
