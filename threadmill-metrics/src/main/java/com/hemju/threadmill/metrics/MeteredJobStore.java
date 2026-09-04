package com.hemju.threadmill.metrics;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobReplacement;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.OversizedJobException;
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.store.ForwardingJobStore;
import com.hemju.threadmill.core.store.JobStore;

/**
 * Store decorator used by {@link ThreadmillMetrics#meteredStore()}.
 *
 * <p>Extends {@link ForwardingJobStore} and overrides only the operations it
 * instruments: {@link #claimReady} for claim latency and failures, and every
 * write for rejected-write attempts. Reads and capability lookups are
 * inherited, so an SPI addition is forwarded by the shared base instead of
 * silently falling back to an interface default here.
 */
final class MeteredJobStore extends ForwardingJobStore {

  private final ThreadmillMetrics metrics;

  MeteredJobStore(JobStore delegate, ThreadmillMetrics metrics) {
    super(delegate);
    this.metrics = Objects.requireNonNull(metrics, "metrics");
  }

  @Override
  public void insert(Job job) {
    writeVoid("insert", true, () -> delegate().insert(job));
  }

  @Override
  public List<JobId> insertAll(List<Job> jobs) {
    return write("insert_all", true, () -> delegate().insertAll(jobs));
  }

  @Override
  public EnqueueResult enqueueIfAbsent(Job job, String dedupKey, Duration ttl, Instant now) {
    return write(
        "enqueue_if_absent", true, () -> delegate().enqueueIfAbsent(job, dedupKey, ttl, now));
  }

  @Override
  public void saveAtomic(Job job, long expectedVersion) {
    writeVoid("save_atomic", () -> delegate().saveAtomic(job, expectedVersion));
  }

  @Override
  public boolean softDelete(JobId id) {
    return write("soft_delete", () -> delegate().softDelete(id));
  }

  @Override
  public List<Job> claimReady(NodeId nodeId, String queue, int max, Instant heartbeatAt) {
    var started = System.nanoTime();
    try {
      return delegate().claimReady(nodeId, queue, max, heartbeatAt);
    } catch (RuntimeException failure) {
      metrics.recordClaimFailure();
      throw failure;
    } finally {
      metrics.recordClaimReadyLatency(Duration.ofNanos(System.nanoTime() - started));
    }
  }

  @Override
  public void pauseQueue(String queue, String reason) {
    writeVoid("pause_queue", () -> delegate().pauseQueue(queue, reason));
  }

  @Override
  public void resumeQueue(String queue) {
    writeVoid("resume_queue", () -> delegate().resumeQueue(queue));
  }

  @Override
  public void touchOwnerHeartbeat(NodeId nodeId, Instant now) {
    writeVoid("touch_owner_heartbeat", () -> delegate().touchOwnerHeartbeat(nodeId, now));
  }

  @Override
  public boolean saveExecutionUpdate(Job job, NodeId nodeId) {
    return write("save_execution_update", () -> delegate().saveExecutionUpdate(job, nodeId));
  }

  @Override
  public void recordNodeHeartbeat(NodeId nodeId, Instant now) {
    writeVoid("record_node_heartbeat", () -> delegate().recordNodeHeartbeat(nodeId, now));
  }

  @Override
  public boolean acquireOrRenewMaintenanceLease(NodeId nodeId, Duration leaseDuration) {
    return write(
        "acquire_or_renew_maintenance_lease",
        () -> delegate().acquireOrRenewMaintenanceLease(nodeId, leaseDuration));
  }

  @Override
  public void releaseMaintenanceLease(NodeId nodeId) {
    writeVoid("release_maintenance_lease", () -> delegate().releaseMaintenanceLease(nodeId));
  }

  @Override
  public long deleteNodeHeartbeatsOlderThan(Instant cutoff) {
    return write(
        "delete_node_heartbeats_older_than",
        () -> delegate().deleteNodeHeartbeatsOlderThan(cutoff));
  }

  @Override
  public long deleteExpiredDedupKeys(Instant now, int max) {
    return write("delete_expired_dedup_keys", () -> delegate().deleteExpiredDedupKeys(now, max));
  }

  @Override
  public long deleteFinishedOlderThan(Instant cutoff, JobState state, int max) {
    return write(
        "delete_finished_older_than", () -> delegate().deleteFinishedOlderThan(cutoff, state, max));
  }

  @Override
  public boolean tryAcquireMutex(String name, String holder, Duration leaseDuration) {
    return write(
        "try_acquire_mutex", () -> delegate().tryAcquireMutex(name, holder, leaseDuration));
  }

  @Override
  public void releaseMutex(String name, String holder) {
    writeVoid("release_mutex", () -> delegate().releaseMutex(name, holder));
  }

  @Override
  public boolean replaceJob(JobId id, long expectedVersion, JobReplacement replacement) {
    return write("replace_job", () -> delegate().replaceJob(id, expectedVersion, replacement));
  }

  @Override
  public void upsertCronTask(CronTask task) {
    writeVoid("upsert_cron_task", () -> delegate().upsertCronTask(task));
  }

  @Override
  public void deleteCronTask(String name) {
    writeVoid("delete_cron_task", () -> delegate().deleteCronTask(name));
  }

  @Override
  public void recordCronTaskOwnership(String namespace, String taskName) {
    writeVoid(
        "record_cron_task_ownership",
        () -> delegate().recordCronTaskOwnership(namespace, taskName));
  }

  @Override
  public void upsertCronTaskState(CronTaskScheduleState state) {
    writeVoid("upsert_cron_task_state", () -> delegate().upsertCronTaskState(state));
  }

  @Override
  public NudgeOutcome requestCronNudge(String taskName, Instant requestedAt) {
    return write("request_cron_nudge", () -> delegate().requestCronNudge(taskName, requestedAt));
  }

  @Override
  public void clearCronNudge(String taskName, long observedRevision) {
    writeVoid("clear_cron_nudge", () -> delegate().clearCronNudge(taskName, observedRevision));
  }

  private <T> T write(String operation, Supplier<T> action) {
    return write(operation, false, action);
  }

  private <T> T write(String operation, boolean duplicateIdIsContractual, Supplier<T> action) {
    try {
      return action.get();
    } catch (RuntimeException rejected) {
      if (!isContractualRejection(duplicateIdIsContractual, rejected)) {
        metrics.recordRejectedWrite(operation);
      }
      throw rejected;
    }
  }

  private static boolean isContractualRejection(
      boolean duplicateIdIsContractual, RuntimeException failure) {
    // Invalid arguments are caller-side contract failures for every SPI
    // operation. Duplicate ids are contractual only on the three insertion
    // entry points, so that policy is declared at their call sites rather
    // than inferred from the exported operation tag.
    return failure instanceof StaleJobException
        || failure instanceof OversizedJobException
        || failure instanceof IllegalArgumentException
        || (duplicateIdIsContractual && failure instanceof IllegalStateException);
  }

  private void writeVoid(String operation, Runnable action) {
    writeVoid(operation, false, action);
  }

  private void writeVoid(String operation, boolean duplicateIdIsContractual, Runnable action) {
    write(operation, duplicateIdIsContractual, () -> {
      action.run();
      return null;
    });
  }
}
