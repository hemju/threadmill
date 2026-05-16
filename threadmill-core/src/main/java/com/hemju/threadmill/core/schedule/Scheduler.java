package com.hemju.threadmill.core.schedule;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobReplacement;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStore;

/**
 * The small, public scheduling API: {@code enqueue}, {@code scheduleAt},
 * {@code defineCronTask}. Intentionally minimal — overload sprawl is a
 * known maintenance trap and a public-API name change is expensive later.
 *
 * <p>A {@code Scheduler} only needs a {@link JobStore} and a
 * {@link JobSerializer}; it does not need a running engine. Submitting
 * applications can wire one against a remote store without ever starting
 * a {@code ProcessingNode}.
 */
public final class Scheduler {

    public static final String SYSTEM_QUEUE = "system";
    public static final Duration DEFAULT_MAX_DEDUP_TTL = Duration.ofDays(30);

    private final JobStore store;
    private final JobSerializer serializer;

    public Scheduler(JobStore store, JobSerializer serializer) {
        this.store = Objects.requireNonNull(store, "store");
        this.serializer = Objects.requireNonNull(serializer, "serializer");
    }

    // ---------------------------------------------------------------- fire-and-forget

    public <P extends JobPayload> JobId enqueue(P payload, Class<? extends JobHandler<P>> handler) {
        return enqueue(payload, handler, "default", 0);
    }

    public <P extends JobPayload> JobId enqueue(
            P payload, Class<? extends JobHandler<P>> handler, String queue, int priority) {
        return enqueue(payload, handler, queue, priority, null, null);
    }

    public <P extends JobPayload> JobId enqueue(
            P payload,
            Class<? extends JobHandler<P>> handler,
            String queue,
            int priority,
            String concurrencyKey,
            ConcurrencyMode concurrencyMode) {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(handler, "handler");
        Objects.requireNonNull(queue, "queue");
        JobArgument arg = serializer.serializePayload(payload);
        Job job = Job.builder()
                .spec(new JobSpec(handler.getName(), List.of(arg)))
                .queue(queue)
                .priority(priority)
                .concurrencyKey(concurrencyKey)
                .concurrencyMode(concurrencyMode)
                .build();
        store.insert(job);
        return job.id();
    }

    public <P extends JobPayload> EnqueueResult enqueueIfAbsent(
            P payload, Class<? extends JobHandler<P>> handler, String dedupKey, Duration ttl) {
        return enqueueIfAbsent(payload, handler, "default", 0, dedupKey, ttl, DEFAULT_MAX_DEDUP_TTL);
    }

    public <P extends JobPayload> EnqueueResult enqueueIfAbsent(
            P payload,
            Class<? extends JobHandler<P>> handler,
            String queue,
            int priority,
            String dedupKey,
            Duration ttl,
            Duration maxTtl) {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(handler, "handler");
        Objects.requireNonNull(ttl, "ttl");
        Objects.requireNonNull(maxTtl, "maxTtl");
        if (ttl.compareTo(maxTtl) > 0) {
            throw new IllegalArgumentException("dedup ttl must not exceed " + maxTtl);
        }
        JobArgument arg = serializer.serializePayload(payload);
        JobSpec spec = new JobSpec(handler.getName(), List.of(arg), dedupKey, ttl);
        Job job = Job.builder().spec(spec).queue(queue).priority(priority).build();
        return store.enqueueIfAbsent(job, dedupKey, ttl, Instant.now());
    }

    // ---------------------------------------------------------------- scheduled

    public <P extends JobPayload> JobId scheduleAt(Instant when, P payload, Class<? extends JobHandler<P>> handler) {
        return scheduleAt(when, payload, handler, "default", 0);
    }

    public <P extends JobPayload> JobId scheduleAt(
            Instant when, P payload, Class<? extends JobHandler<P>> handler, String queue, int priority) {
        Objects.requireNonNull(when, "when");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(handler, "handler");
        Objects.requireNonNull(queue, "queue");
        JobArgument arg = serializer.serializePayload(payload);
        Job job = Job.builder()
                .spec(new JobSpec(handler.getName(), List.of(arg)))
                .queue(queue)
                .priority(priority)
                .initialState(JobState.SCHEDULED)
                .scheduledFor(when)
                .build();
        store.insert(job);
        return job.id();
    }

    public JobId scheduleIn(
            Duration delay, JobPayload payload, Class<? extends JobHandler<? extends JobPayload>> handler) {
        Objects.requireNonNull(delay, "delay");
        @SuppressWarnings({"unchecked", "rawtypes"})
        Class<? extends JobHandler<JobPayload>> cast = (Class) handler;
        return scheduleAt(Instant.now().plus(delay), payload, cast);
    }

    // ---------------------------------------------------------------- recurring

    /**
     * Register a recurring task driven by a cron expression. The first run
     * is scheduled for the next time the expression fires after "now".
     */
    public <P extends JobPayload> void defineCronTask(
            String name, String cronExpression, P payload, Class<? extends JobHandler<P>> handler) {
        defineCronTask(
                name,
                cronExpression,
                payload,
                handler,
                "default",
                0,
                CronTask.MissedRunPolicy.DROP,
                ZoneId.systemDefault());
    }

    public <P extends JobPayload> void defineCronTask(
            String name,
            String cronExpression,
            P payload,
            Class<? extends JobHandler<P>> handler,
            String queue,
            int priority,
            CronTask.MissedRunPolicy policy,
            ZoneId zone) {
        var cron = CronExpression.parse(cronExpression);
        upsertCron(new CronTask(
                name,
                new CronTask.Trigger.CronExpr(cron),
                handler.getName(),
                serializer.serializePayload(payload),
                queue,
                priority,
                policy,
                zone,
                true));
    }

    /**
     * Register a recurring task that fires every {@code interval}. The
     * first run is scheduled for now + interval.
     */
    public <P extends JobPayload> void defineIntervalTask(
            String name, Duration interval, P payload, Class<? extends JobHandler<P>> handler) {
        defineIntervalTask(name, interval, payload, handler, "default", 0, CronTask.MissedRunPolicy.DROP);
    }

    public <P extends JobPayload> void defineIntervalTask(
            String name,
            Duration interval,
            P payload,
            Class<? extends JobHandler<P>> handler,
            String queue,
            int priority,
            CronTask.MissedRunPolicy policy) {
        upsertCron(new CronTask(
                name,
                new CronTask.Trigger.Interval(interval),
                handler.getName(),
                serializer.serializePayload(payload),
                queue,
                priority,
                policy,
                ZoneId.systemDefault(),
                true));
    }

    public void deleteCronTask(String name) {
        store.deleteCronTask(name);
    }

    // ---------------------------------------------------------------- replace

    /**
     * Atomically replace a non-running job's definition.
     *
     * <p>Only succeeds if the job is currently in {@code ENQUEUED},
     * {@code SCHEDULED}, or {@code AWAITING} and its persisted version
     * matches {@code expectedVersion}. Use {@link com.hemju.threadmill.core.Job#version()}
     * on a freshly-loaded job to obtain the expected version.
     *
     * @return {@code true} if the replacement was applied, {@code false}
     *         if the job vanished or is in a non-replaceable state
     * @throws com.hemju.threadmill.core.StaleJobException on version mismatch
     */
    public boolean replace(JobId id, long expectedVersion, JobReplacement replacement) {
        return store.replaceJob(id, expectedVersion, replacement);
    }

    /**
     * Convenience: replace just the {@link JobSpec}.
     * The new spec must specify a handler whose payload type matches the
     * argument it carries.
     */
    public boolean replaceSpec(JobId id, long expectedVersion, JobSpec newSpec) {
        return replace(id, expectedVersion, JobReplacement.ofSpec(newSpec));
    }

    /**
     * Enqueue a batch of payloads as a single logical operation. Either all
     * payloads are persisted or none — see
     * {@link com.hemju.threadmill.core.store.JobStore#insertAll(java.util.List)}.
     *
     * <p>This is materially cheaper than calling {@link #enqueue(JobPayload, Class)}
     * in a loop: PostgreSQL collapses the writes into one batched INSERT,
     * Redis into one Lua script, and the in-memory store into one mutex
     * acquisition.
     */
    public <P extends JobPayload> List<JobId> enqueueAll(List<P> payloads, Class<? extends JobHandler<P>> handler) {
        return enqueueAll(payloads, handler, "default", 0);
    }

    public <P extends JobPayload> List<JobId> enqueueAll(
            List<P> payloads, Class<? extends JobHandler<P>> handler, String queue, int priority) {
        Objects.requireNonNull(payloads, "payloads");
        Objects.requireNonNull(handler, "handler");
        Objects.requireNonNull(queue, "queue");
        if (payloads.isEmpty()) return List.of();
        var jobs = new ArrayList<Job>(payloads.size());
        for (P p : payloads) {
            Objects.requireNonNull(p, "payload");
            JobArgument arg = serializer.serializePayload(p);
            jobs.add(Job.builder()
                    .spec(new JobSpec(handler.getName(), List.of(arg)))
                    .queue(queue)
                    .priority(priority)
                    .build());
        }
        return store.insertAll(jobs);
    }

    // ---------------------------------------------------------------- queue pauses

    /**
     * Pause claiming from {@code queue}. Pending jobs stay {@code ENQUEUED};
     * in-flight jobs run to completion. Idempotent.
     *
     * @param queue  the queue to pause
     * @param reason a short label for ops audit trails (nullable)
     */
    public void pauseQueue(String queue, String reason) {
        store.pauseQueue(queue, reason);
    }

    /** Resume claiming from {@code queue}. Idempotent. */
    public void resumeQueue(String queue) {
        store.resumeQueue(queue);
    }

    /** Snapshot of queues currently paused. */
    public Set<String> pausedQueues() {
        return store.listPausedQueues();
    }

    private void upsertCron(CronTask task) {
        // Identity vs schedule-state: re-registering a task replaces only the
        // definition. The state is initialised on first registration; on
        // re-registration we preserve in-flight job tracking but recompute
        // the next-run from the new trigger relative to now (so a freshly
        // edited cron does not fire stale times).
        var existingState = store.findCronTaskState(task.name());
        var now = Instant.now();
        Instant next = task.trigger().nextAfter(now, task.zone());
        store.upsertCronTask(task);
        if (existingState.isEmpty()) {
            store.upsertCronTaskState(CronTaskScheduleState.initial(task.name(), next));
        } else {
            var s = existingState.get();
            store.upsertCronTaskState(
                    new CronTaskScheduleState(task.name(), s.lastRunAt(), s.lastRunJobId(), next, s.inFlightJobId()));
        }
    }
}
