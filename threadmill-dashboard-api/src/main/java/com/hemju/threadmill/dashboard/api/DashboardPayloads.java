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
            boolean detailsRedacted) {}

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

    public record RecurringTaskView(CronTask task, CronTaskScheduleState state) {}

    public record PauseQueueRequest(String reason) {}

    public record VersionedActionRequest(long expectedVersion) {}

    public record ScheduleRetryRequest(long expectedVersion, Duration delay) {}

    public record ReplaceJobRequest(
            long expectedVersion,
            String queue,
            Integer priority,
            Instant scheduledFor,
            String handlerType,
            List<JobArgument> arguments) {}

    public record UpdateRecurringRequest(
            String triggerKind,
            String triggerValue,
            String handlerType,
            JobArgument payloadArgument,
            String queue,
            Integer priority,
            CronTask.MissedRunPolicy missedRunPolicy,
            String zone,
            Boolean enabled) {}

    public record ActionResponse(String status, String target) {}
}
