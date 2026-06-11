package com.hemju.threadmill.dashboard.api;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobReplacement;
import com.hemju.threadmill.core.JobResult;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.JobStateEntry;
import com.hemju.threadmill.core.JobStateMachine;
import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.schedule.CronExpression;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobSearch;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.ActionResponse;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.JobDetail;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.JobListResponse;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.JobSummary;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.OverviewResponse;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.QueueView;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.RecurringTaskView;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.ReplaceJobRequest;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.ResultView;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.UpdateRecurringRequest;

/** Dashboard read and mutation service. */
public final class DashboardApiService {

    private static final int MAX_PAUSE_REASON_BYTES = 256;

    private final JobStore store;
    private final LocalWakeBus wakeBus;

    public DashboardApiService(JobStore store, LocalWakeBus wakeBus) {
        this.store = Objects.requireNonNull(store, "store");
        this.wakeBus = Objects.requireNonNull(wakeBus, "wakeBus");
    }

    public OverviewResponse overview(boolean includeSensitiveDetails) {
        var snapshot = EngineSnapshot.of(store);
        var tasks = snapshot.cronTasks().stream()
                .map(task -> new RecurringTaskView(
                        cronTaskView(task, includeSensitiveDetails),
                        store.findCronTaskState(task.name()).orElse(null)))
                .toList();
        return new OverviewResponse(
                snapshot.takenAt(),
                snapshot.countsByState(),
                snapshot.queueDepths(),
                snapshot.oldestEnqueuedAt(),
                snapshot.oldestProcessingHeartbeat(),
                snapshot.nodeHeartbeats(),
                tasks,
                snapshot.pausedQueues(),
                snapshot.capabilities());
    }

    public JobListResponse jobs(JobSearch search) {
        if (!store.capabilities().supportsRichSearch()
                && (search.state() == null || search.queue() != null || search.handlerType() != null)) {
            throw DashboardApiException.badRequest("this store supports dashboard job search only by state");
        }
        var jobs =
                store.searchJobs(search).stream().map(job -> summary(job, true)).toList();
        return new JobListResponse(jobs, search.limit(), search.offset());
    }

    public JobDetail job(JobId id, boolean includeSensitiveDetails) {
        Job job = load(id);
        var summary = summary(job, !includeSensitiveDetails);
        if (!includeSensitiveDetails) {
            return new JobDetail(summary, redactedStateHistory(job), List.of(), Map.of(), List.of(), null, null, true);
        }
        return new JobDetail(
                summary,
                job.stateHistory(),
                job.spec().arguments(),
                job.metadata().snapshot(),
                job.log().snapshot(),
                job.progress().snapshot().orElse(null),
                job.result().map(DashboardApiService::result).orElse(null),
                false);
    }

    public List<QueueView> queues() {
        var snapshot = EngineSnapshot.of(store);
        return snapshot.queueDepths().entrySet().stream()
                .map(e -> new QueueView(
                        e.getKey(),
                        e.getValue(),
                        snapshot.pausedQueues().contains(e.getKey()),
                        snapshot.oldestEnqueuedAt().get(e.getKey())))
                .toList();
    }

    public List<RecurringTaskView> recurringTasks(boolean includeSensitiveDetails) {
        return store.listCronTasks().stream()
                .map(task -> new RecurringTaskView(
                        cronTaskView(task, includeSensitiveDetails),
                        store.findCronTaskState(task.name()).orElse(null)))
                .toList();
    }

    private static DashboardPayloads.CronTaskView cronTaskView(CronTask task, boolean includeSensitiveDetails) {
        String triggerKind =
                switch (task.trigger()) {
                    case CronTask.Trigger.CronExpr ignored -> "CRON";
                    case CronTask.Trigger.Interval ignored -> "INTERVAL";
                };
        String triggerValue =
                switch (task.trigger()) {
                    case CronTask.Trigger.CronExpr cron -> cron.expression().toString();
                    case CronTask.Trigger.Interval interval ->
                        interval.interval().toString();
                };
        return new DashboardPayloads.CronTaskView(
                task.name(),
                triggerKind,
                triggerValue,
                task.handlerType(),
                includeSensitiveDetails ? task.payloadArgument() : null,
                task.queue(),
                task.priority(),
                task.missedRunPolicy().name(),
                task.zone().toString(),
                task.enabled(),
                !includeSensitiveDetails);
    }

    public ActionResponse pauseQueue(String queue, String reason) {
        if (reason != null && reason.getBytes(StandardCharsets.UTF_8).length > MAX_PAUSE_REASON_BYTES) {
            throw DashboardApiException.badRequest("pause reason must be at most 256 UTF-8 bytes");
        }
        store.pauseQueue(queue, reason);
        return new ActionResponse("paused", queue);
    }

    public ActionResponse resumeQueue(String queue) {
        store.resumeQueue(queue);
        wakeBus.wake(queue);
        return new ActionResponse("resumed", queue);
    }

    public ActionResponse deleteJob(JobId id, long expectedVersion) {
        Job job = load(id);
        requireVersion(job, expectedVersion);
        if (!JobStateMachine.isLegal(job.currentState(), JobState.DELETED)) {
            throw conflict("job cannot be deleted from " + job.currentState());
        }
        job.transitionTo(JobState.DELETED, Instant.now(), "dashboard.delete", null);
        job.clearOwner();
        store.saveAtomic(job, expectedVersion);
        return new ActionResponse("deleted", id.toString());
    }

    public ActionResponse requeue(JobId id, long expectedVersion) {
        Job job = load(id);
        requireVersion(job, expectedVersion);
        if (job.currentState() == JobState.PROCESSING || job.currentState() == JobState.QUARANTINED) {
            throw conflict("job cannot be requeued from " + job.currentState());
        }
        if (!JobStateMachine.isLegal(job.currentState(), JobState.ENQUEUED)) {
            throw conflict("job cannot be requeued from " + job.currentState());
        }
        job.transitionTo(JobState.ENQUEUED, Instant.now(), "dashboard.requeue", null);
        job.clearOwner();
        job.clearScheduledFor();
        store.saveAtomic(job, expectedVersion);
        wakeBus.wake(job.queue());
        return new ActionResponse("requeued", id.toString());
    }

    public ActionResponse scheduleRetry(JobId id, long expectedVersion, Duration delay) {
        Objects.requireNonNull(delay, "delay");
        if (delay.isNegative()) throw DashboardApiException.badRequest("delay must not be negative");
        Job job = load(id);
        requireVersion(job, expectedVersion);
        if (job.currentState() != JobState.FAILED) {
            throw conflict("only FAILED jobs can be scheduled for retry");
        }
        job.transitionTo(JobState.SCHEDULED, Instant.now(), "dashboard.schedule_retry", null);
        job.scheduleAt(Instant.now().plus(delay));
        store.saveAtomic(job, expectedVersion);
        return new ActionResponse("scheduled", id.toString());
    }

    public ActionResponse replaceJob(JobId id, ReplaceJobRequest request) {
        Objects.requireNonNull(request, "request");
        Job job = load(id);
        requireVersion(job, request.expectedVersion());
        if (!isReplaceable(job.currentState())) {
            throw conflict("job cannot be replaced from " + job.currentState());
        }
        var builder = JobReplacement.builder();
        if (request.queue() != null) builder.queue(request.queue());
        if (request.priority() != null) builder.priority(request.priority());
        if (request.scheduledFor() != null) builder.scheduledFor(request.scheduledFor());
        if (request.handlerType() != null) {
            builder.spec(
                    new JobSpec(request.handlerType(), request.arguments() == null ? List.of() : request.arguments()));
        }
        boolean replaced = store.replaceJob(id, request.expectedVersion(), builder.build());
        if (!replaced) {
            if (store.findById(id).isEmpty()) throw notFound("job not found");
            throw conflict("job cannot be replaced in its current state");
        }
        return new ActionResponse("replaced", id.toString());
    }

    public ActionResponse triggerRecurring(String name) {
        CronTask task = store.findCronTask(name).orElseThrow(() -> notFound("recurring task not found"));
        Job job = Job.builder()
                .spec(new JobSpec(task.handlerType(), List.of(task.payloadArgument())))
                .queue(task.queue())
                .priority(task.priority())
                .build();
        store.insert(job);
        var prior = store.findCronTaskState(name).orElse(CronTaskScheduleState.initial(name, null));
        store.upsertCronTaskState(new CronTaskScheduleState(
                name,
                Instant.now(),
                job.id().asUuid(),
                prior.nextRunAt(),
                job.id().asUuid()));
        wakeBus.wake(task.queue());
        return new ActionResponse("triggered", name);
    }

    public ActionResponse updateRecurring(String name, UpdateRecurringRequest request) {
        Objects.requireNonNull(request, "request");
        CronTask existing = store.findCronTask(name).orElseThrow(() -> notFound("recurring task not found"));
        CronTask task = new CronTask(
                name,
                trigger(request.triggerKind(), request.triggerValue(), existing.trigger()),
                request.handlerType() == null ? existing.handlerType() : request.handlerType(),
                request.payloadArgument() == null ? existing.payloadArgument() : request.payloadArgument(),
                request.queue() == null ? existing.queue() : request.queue(),
                request.priority() == null ? existing.priority() : request.priority(),
                request.missedRunPolicy() == null ? existing.missedRunPolicy() : request.missedRunPolicy(),
                request.zone() == null ? existing.zone() : ZoneId.of(request.zone()),
                request.enabled() == null ? existing.enabled() : request.enabled());
        store.upsertCronTask(task);
        store.upsertCronTaskState(stateAfterRecurringUpdate(task));
        return new ActionResponse("updated", name);
    }

    public ActionResponse deleteRecurring(String name) {
        store.deleteCronTask(name);
        return new ActionResponse("deleted", name);
    }

    private CronTaskScheduleState stateAfterRecurringUpdate(CronTask task) {
        var existing = store.findCronTaskState(task.name());
        Instant next = task.enabled() ? task.trigger().nextAfter(Instant.now(), task.zone()) : null;
        if (existing.isEmpty()) return CronTaskScheduleState.initial(task.name(), next);
        var state = existing.get();
        return new CronTaskScheduleState(
                task.name(), state.lastRunAt(), state.lastRunJobId(), next, state.inFlightJobId());
    }

    private static CronTask.Trigger trigger(String kind, String value, CronTask.Trigger fallback) {
        if (kind == null && value == null) return fallback;
        if (kind == null || value == null) throw DashboardApiException.badRequest("trigger incomplete");
        return switch (kind.toUpperCase()) {
            case "CRON" -> new CronTask.Trigger.CronExpr(CronExpression.parse(value));
            case "INTERVAL" -> new CronTask.Trigger.Interval(Duration.parse(value));
            default -> throw DashboardApiException.badRequest("unknown trigger kind");
        };
    }

    private Job load(JobId id) {
        return store.findById(id).orElseThrow(() -> notFound("job not found"));
    }

    private static ResultView result(JobResult result) {
        return new ResultView(result.typeTag(), result.serialized());
    }

    private static JobSummary summary(Job job, boolean redacted) {
        return new JobSummary(
                job.id().toString(),
                job.currentState(),
                job.queue(),
                job.priority(),
                job.spec().handlerType(),
                job.attempts(),
                job.version(),
                job.createdAt(),
                job.stateHistory().getLast().at(),
                job.scheduledFor().orElse(null),
                job.ownerNodeId().map(Object::toString).orElse(null),
                job.ownerHeartbeatAt().orElse(null),
                redacted);
    }

    private static List<JobStateEntry> redactedStateHistory(Job job) {
        return job.stateHistory().stream()
                .map(entry -> new JobStateEntry(entry.state(), entry.at(), entry.reason(), null))
                .toList();
    }

    private static boolean isReplaceable(JobState state) {
        return state == JobState.ENQUEUED || state == JobState.SCHEDULED || state == JobState.AWAITING;
    }

    private static void requireVersion(Job job, long expectedVersion) {
        if (job.version() != expectedVersion) throw conflict("stale job version");
    }

    private static DashboardApiException notFound(String message) {
        return DashboardApiException.notFound(message);
    }

    private static DashboardApiException conflict(String message) {
        return DashboardApiException.conflict(message);
    }
}
