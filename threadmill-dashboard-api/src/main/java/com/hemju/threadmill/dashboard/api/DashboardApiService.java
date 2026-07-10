package com.hemju.threadmill.dashboard.api;

import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobReplacement;
import com.hemju.threadmill.core.JobResult;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.JobStateEntry;
import com.hemju.threadmill.core.JobStateMachine;
import com.hemju.threadmill.core.engine.JobRunner;
import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.engine.RetryInterceptor;
import com.hemju.threadmill.core.schedule.CronExpression;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.schedule.RecurringMaterializer;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobSearch;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.core.store.NodeHeartbeat;
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
    private static final Duration CRON_MUTEX_LEASE = Duration.ofSeconds(30);

    /**
     * Deep offset pagination is an O(offset) skip in the relational
     * backends; cap it so a runaway poller cannot trigger large scans.
     * Keyset pagination is the follow-up if deeper paging is ever needed.
     */
    public static final int MAX_SEARCH_OFFSET = 100_000;

    /** Dashboard polling is the access pattern — one second of staleness is fine. */
    public static final Duration DEFAULT_SNAPSHOT_CACHE_TTL = Duration.ofSeconds(1);

    private final JobStore store;
    private final LocalWakeBus wakeBus;
    private final String cronMutexHolder = UUID.randomUUID().toString();
    private final long snapshotCacheTtlNanos;
    private volatile CachedSnapshot cachedSnapshot;

    public DashboardApiService(JobStore store, LocalWakeBus wakeBus) {
        this(store, wakeBus, DEFAULT_SNAPSHOT_CACHE_TTL);
    }

    public DashboardApiService(JobStore store, LocalWakeBus wakeBus, Duration snapshotCacheTtl) {
        this.store = Objects.requireNonNull(store, "store");
        this.wakeBus = Objects.requireNonNull(wakeBus, "wakeBus");
        Objects.requireNonNull(snapshotCacheTtl, "snapshotCacheTtl");
        if (snapshotCacheTtl.isNegative()) throw new IllegalArgumentException("snapshotCacheTtl must be >= 0");
        this.snapshotCacheTtlNanos = snapshotCacheTtl.toNanos();
    }

    /** One snapshot plus batched cron states, shared by every snapshot-backed endpoint for one TTL window. */
    private record CachedSnapshot(
            EngineSnapshot snapshot, Map<String, CronTaskScheduleState> cronStates, long expiresAtNanos) {}

    private CachedSnapshot snapshotData() {
        var cached = cachedSnapshot;
        if (cached != null && System.nanoTime() - cached.expiresAtNanos() < 0) return cached;
        var snapshot = EngineSnapshot.of(store);
        Map<String, CronTaskScheduleState> states = new LinkedHashMap<>();
        for (var task : snapshot.cronTasks()) {
            store.findCronTaskState(task.name()).ifPresent(state -> states.put(task.name(), state));
        }
        var fresh = new CachedSnapshot(
                snapshot, Collections.unmodifiableMap(states), System.nanoTime() + snapshotCacheTtlNanos);
        cachedSnapshot = fresh;
        return fresh;
    }

    /** Dashboard mutations drop the cache so the operator immediately sees their own action. */
    private void invalidateSnapshotCache() {
        cachedSnapshot = null;
    }

    public OverviewResponse overview(boolean includeSensitiveDetails) {
        var data = snapshotData();
        var snapshot = data.snapshot();
        var tasks = recurringTaskViews(data, includeSensitiveDetails);
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
        if (search.offset() > MAX_SEARCH_OFFSET) {
            throw DashboardApiException.badRequest("offset must be at most " + MAX_SEARCH_OFFSET);
        }
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
        var snapshot = snapshotData().snapshot();
        return snapshot.queueDepths().entrySet().stream()
                .map(e -> new QueueView(
                        e.getKey(),
                        e.getValue(),
                        snapshot.pausedQueues().contains(e.getKey()),
                        snapshot.oldestEnqueuedAt().get(e.getKey())))
                .toList();
    }

    /** Node heartbeats only — never builds a full snapshot for this one list. */
    public List<NodeHeartbeat> nodeHeartbeats() {
        return store.listNodeHeartbeats();
    }

    public List<RecurringTaskView> recurringTasks(boolean includeSensitiveDetails) {
        return recurringTaskViews(snapshotData(), includeSensitiveDetails);
    }

    private static List<RecurringTaskView> recurringTaskViews(CachedSnapshot data, boolean includeSensitiveDetails) {
        return data.snapshot().cronTasks().stream()
                .map(task -> new RecurringTaskView(
                        cronTaskView(task, includeSensitiveDetails),
                        data.cronStates().get(task.name())))
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
        invalidateSnapshotCache();
        return new ActionResponse("paused", queue);
    }

    public ActionResponse resumeQueue(String queue) {
        store.resumeQueue(queue);
        wakeBus.wake(queue);
        invalidateSnapshotCache();
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
        invalidateSnapshotCache();
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
        invalidateSnapshotCache();
        return new ActionResponse("requeued", id.toString());
    }

    public ActionResponse scheduleRetry(JobId id, long expectedVersion, Duration delay) {
        // A request body omitting delay reaches here as null (Jackson leaves
        // the record component unset) — operator input, not a server fault.
        if (delay == null) throw DashboardApiException.badRequest("delay is required");
        if (delay.isNegative()) throw DashboardApiException.badRequest("delay must not be negative");
        Job job = load(id);
        requireVersion(job, expectedVersion);
        if (job.currentState() != JobState.FAILED) {
            throw conflict("only FAILED jobs can be scheduled for retry");
        }
        job.transitionTo(JobState.SCHEDULED, Instant.now(), "dashboard.schedule_retry", null);
        job.scheduleAt(Instant.now().plus(delay));
        store.saveAtomic(job, expectedVersion);
        invalidateSnapshotCache();
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
        invalidateSnapshotCache();
        return new ActionResponse("replaced", id.toString());
    }

    public ActionResponse triggerRecurring(String name) {
        CronTask task = store.findCronTask(name).orElseThrow(() -> notFound("recurring task not found"));
        var builder = Job.builder()
                .spec(new JobSpec(task.handlerType(), List.of(task.payloadArgument())))
                .queue(task.queue())
                .priority(task.priority())
                .cronTaskName(task.name());
        if (task.timeout() != null) {
            builder.metadata(
                    JobRunner.META_TIMEOUT_SECONDS, Long.toString(task.timeout().toSeconds()));
        }
        if (task.maxAttempts() != null) {
            builder.metadata(RetryInterceptor.META_MAX_ATTEMPTS, Integer.toString(task.maxAttempts()));
        }
        Job job = builder.build();
        withTaskMutex(name, () -> {
            store.insert(job);
            var prior = store.findCronTaskState(name).orElse(CronTaskScheduleState.initial(name, null));
            // The materializer's pile-up guard tracks inFlightJobId. While a
            // scheduled instance is still running, the manual job runs but
            // must NOT take over the guard — otherwise the guard is released
            // as soon as the manual job terminates and the next scheduled
            // instance overlaps the still-running one.
            UUID inFlight = prior.inFlightJobId();
            boolean priorStillRunning = inFlight != null
                    && store.findById(JobId.of(inFlight))
                            .map(running ->
                                    !running.currentState().isTerminal() && running.currentState() != JobState.FAILED)
                            .orElse(false);
            store.upsertCronTaskState(new CronTaskScheduleState(
                    name,
                    Instant.now(),
                    job.id().asUuid(),
                    prior.nextRunAt(),
                    priorStillRunning ? inFlight : job.id().asUuid()));
        });
        wakeBus.wake(task.queue());
        invalidateSnapshotCache();
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
                existing.timeout(),
                existing.maxAttempts(),
                request.missedRunPolicy() == null ? existing.missedRunPolicy() : request.missedRunPolicy(),
                request.zone() == null ? existing.zone() : parseZone(request.zone()),
                request.enabled() == null ? existing.enabled() : request.enabled());
        withTaskMutex(name, () -> {
            store.upsertCronTask(task);
            store.upsertCronTaskState(stateAfterRecurringUpdate(task));
        });
        invalidateSnapshotCache();
        return new ActionResponse("updated", name);
    }

    public ActionResponse deleteRecurring(String name) {
        withTaskMutex(name, () -> store.deleteCronTask(name));
        invalidateSnapshotCache();
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

    /**
     * Guard a {@code CronTaskScheduleState} read-modify-write with the same
     * per-task store mutex the {@code RecurringMaterializer} and
     * {@code Scheduler.upsertCron} take, so dashboard mutations cannot
     * clobber a concurrently-set {@code inFlightJobId} on the maintenance
     * master. If the mutex never frees, reject the mutation with a conflict so
     * correctness is never traded for an unguarded operator action.
     */
    private void withTaskMutex(String name, Runnable action) {
        String mutex = RecurringMaterializer.taskMutexName(name);
        boolean locked = false;
        for (int attempt = 0; attempt < 100 && !locked; attempt++) {
            locked = store.tryAcquireMutex(mutex, cronMutexHolder, CRON_MUTEX_LEASE);
            if (!locked) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        if (!locked) {
            throw conflict("recurring task is busy; retry after the current mutation completes");
        }
        try {
            action.run();
        } finally {
            if (locked) {
                try {
                    store.releaseMutex(mutex, cronMutexHolder);
                } catch (RuntimeException ignored) {
                    // the lease expires on its own
                }
            }
        }
    }

    private static CronTask.Trigger trigger(String kind, String value, CronTask.Trigger fallback) {
        if (kind == null && value == null) return fallback;
        if (kind == null || value == null) throw DashboardApiException.badRequest("trigger incomplete");
        return switch (kind.toUpperCase()) {
            case "CRON" -> new CronTask.Trigger.CronExpr(CronExpression.parse(value));
            case "INTERVAL" -> new CronTask.Trigger.Interval(parseInterval(value));
            default -> throw DashboardApiException.badRequest("unknown trigger kind");
        };
    }

    private static ZoneId parseZone(String zone) {
        // ZoneRulesException is a DateTimeException, not an
        // IllegalArgumentException — unwrapped it surfaces as a 500.
        try {
            return ZoneId.of(zone);
        } catch (DateTimeException e) {
            throw DashboardApiException.badRequest("invalid zone: " + zone);
        }
    }

    private static Duration parseInterval(String value) {
        try {
            return Duration.parse(value);
        } catch (DateTimeParseException e) {
            throw DashboardApiException.badRequest(
                    "invalid INTERVAL value: " + value + " (expected ISO-8601 duration)");
        }
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
