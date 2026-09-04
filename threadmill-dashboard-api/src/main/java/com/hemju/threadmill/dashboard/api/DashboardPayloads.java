package com.hemju.threadmill.dashboard.api;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.hemju.threadmill.core.JobLog;
import com.hemju.threadmill.core.JobProgress;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.JobStateEntry;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.store.JobStoreCapabilities;
import com.hemju.threadmill.core.store.NodeHeartbeat;

/** JSON payload types used by the dashboard API. */
public final class DashboardPayloads {

  private DashboardPayloads() {}

  public record SessionResponse(
      String displayName, Set<DashboardPermission> permissions, Csrf csrf, String redactionMode) {
    public record Csrf(String headerName, String parameterName, String token) {}
  }

  public record OverviewResponse(
      Instant takenAt,
      Map<JobState, Long> countsByState,
      Map<String, Long> queueDepths,
      Map<String, Instant> oldestEnqueuedAt,
      Instant oldestProcessingHeartbeat,
      List<NodeHeartbeat> nodeHeartbeats,
      List<RecurringTaskView> cronTasks,
      Set<String> pausedQueues,
      JobStoreCapabilities capabilities) {}

  public record JobListResponse(List<JobSummary> jobs, int limit, int offset) {}

  /**
   * @param cronOrigin for recurring instances, what triggered this one —
   *                   {@code schedule}, {@code nudge}, or {@code manual};
   *                   {@code null} otherwise. Deliberately not subject to
   *                   detail redaction: the value set is closed (arbitrary
   *                   metadata can never leak through it) and carries no
   *                   payload data, so read-level dashboard users can
   *                   distinguish nudged from scheduled runs
   */
  public record JobSummary(
      String id,
      JobState state,
      String queue,
      int priority,
      String handlerType,
      int attempts,
      long version,
      Instant createdAt,
      Instant currentStateAt,
      Instant scheduledFor,
      String ownerNodeId,
      Instant ownerHeartbeatAt,
      boolean detailsRedacted,
      String cronOrigin) {}

  public record JobDetail(
      JobSummary summary,
      List<JobStateEntry> stateHistory,
      List<JobArgument> arguments,
      Map<String, String> metadata,
      List<JobLog.Entry> log,
      JobProgress.Snapshot progress,
      ResultView result,
      boolean sensitiveDetailsRedacted) {}

  public record ResultView(String typeTag, String serialized) {}

  public record QueueView(String queue, long depth, boolean paused, Instant oldestEnqueuedAt) {}

  public record RecurringTaskView(CronTaskView task, CronTaskScheduleState state) {}

  /**
   * Wire-stable view of a {@link CronTask}: the trigger is rendered as
   * explicit {@code triggerKind} / {@code triggerValue} strings (the same
   * shape {@code UpdateRecurringRequest} accepts), and
   * {@code payloadArgument} is {@code null} unless the caller is allowed
   * to see sensitive details — recurring payloads carry the exact data
   * class redacted on the job-detail endpoint.
   */
  public record CronTaskView(
      String name,
      String triggerKind,
      String triggerValue,
      String handlerType,
      JobArgument payloadArgument,
      String queue,
      int priority,
      boolean exclusive,
      String missedRunPolicy,
      String zone,
      boolean enabled,
      boolean payloadRedacted) {}

  public record PauseQueueRequest(String reason) {}

  public record VersionedActionRequest(long expectedVersion) {}

  public record ScheduleRetryRequest(long expectedVersion, Duration delay) {}

  /**
   * Pending-job replacement request.
   *
   * <p>Queue, priority, and schedule edits require {@link DashboardPermission#REPLACE_JOB}.
   * Supplying either {@code handlerType} or {@code arguments} replaces the executable definition
   * and requires {@link DashboardPermission#ADMIN}; omitted definition fields preserve their
   * current values.
   */
  public record ReplaceJobRequest(
      long expectedVersion,
      String queue,
      Integer priority,
      Instant scheduledFor,
      String handlerType,
      List<JobArgument> arguments) {

    public boolean replacesDefinition() {
      return handlerType != null || arguments != null;
    }

    /** Permission required by the fields present in this request. */
    public DashboardPermission requiredPermission() {
      return replacesDefinition() ? DashboardPermission.ADMIN : DashboardPermission.REPLACE_JOB;
    }
  }

  /** Recurring-task update; omitted fields preserve the current definition. */
  public record UpdateRecurringRequest(
      String triggerKind,
      String triggerValue,
      String handlerType,
      JobArgument payloadArgument,
      String queue,
      Integer priority,
      CronTask.MissedRunPolicy missedRunPolicy,
      String zone,
      Boolean enabled) {

    public boolean replacesDefinition() {
      return handlerType != null || payloadArgument != null;
    }

    /** Permission required by the fields present in this request. */
    public DashboardPermission requiredPermission() {
      return replacesDefinition()
          ? DashboardPermission.ADMIN
          : DashboardPermission.UPDATE_RECURRING;
    }
  }

  public record ActionResponse(String status, String target) {}
}
