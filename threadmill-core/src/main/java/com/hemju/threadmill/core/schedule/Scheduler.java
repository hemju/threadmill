package com.hemju.threadmill.core.schedule;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobReplacement;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.engine.LocalWakeBus;
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

    private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
    private static final Duration CRON_MUTEX_LEASE = Duration.ofSeconds(30);

    private final JobStore store;
    private final JobSerializer serializer;
    private final LocalWakeBus wakeBus;
    private final String cronMutexHolder = UUID.randomUUID().toString();

    public Scheduler(JobStore store, JobSerializer serializer) {
        this(store, serializer, new LocalWakeBus());
    }

    public Scheduler(JobStore store, JobSerializer serializer, LocalWakeBus wakeBus) {
        this.store = Objects.requireNonNull(store, "store");
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.wakeBus = Objects.requireNonNull(wakeBus, "wakeBus");
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
        wakeBus.wake(queue);
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
        EnqueueResult result = store.enqueueIfAbsent(job, dedupKey, ttl, Instant.now());
        if (result instanceof EnqueueResult.Created) {
            wakeBus.wake(queue);
        }
        return result;
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

    public <P extends JobPayload> JobId scheduleIn(Duration delay, P payload, Class<? extends JobHandler<P>> handler) {
        Objects.requireNonNull(delay, "delay");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(handler, "handler");
        return scheduleAt(Instant.now().plus(delay), payload, handler);
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
        defineCronTask(name, cronExpression, payload, handler, queue, priority, null, null, policy, zone);
    }

    /**
     * Full cron-task registration including the per-instance overrides: an
     * execution {@code timeout} ({@code null} keeps the engine's global job
     * timeout) and a retry budget {@code maxAttempts} ({@code null} keeps the
     * {@code RetryInterceptor} defaults).
     */
    public <P extends JobPayload> void defineCronTask(
            String name,
            String cronExpression,
            P payload,
            Class<? extends JobHandler<P>> handler,
            String queue,
            int priority,
            Duration timeout,
            Integer maxAttempts,
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
                timeout,
                maxAttempts,
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
        defineIntervalTask(name, interval, payload, handler, queue, priority, null, null, policy);
    }

    /**
     * Full interval-task registration including the per-instance overrides: an
     * execution {@code timeout} ({@code null} keeps the engine's global job
     * timeout) and a retry budget {@code maxAttempts} ({@code null} keeps the
     * {@code RetryInterceptor} defaults).
     */
    public <P extends JobPayload> void defineIntervalTask(
            String name,
            Duration interval,
            P payload,
            Class<? extends JobHandler<P>> handler,
            String queue,
            int priority,
            Duration timeout,
            Integer maxAttempts,
            CronTask.MissedRunPolicy policy) {
        upsertCron(new CronTask(
                name,
                new CronTask.Trigger.Interval(interval),
                handler.getName(),
                serializer.serializePayload(payload),
                queue,
                priority,
                timeout,
                maxAttempts,
                policy,
                ZoneId.systemDefault(),
                true));
    }

    public void deleteCronTask(String name) {
        store.deleteCronTask(name);
    }

    /**
     * Reconcile a namespace-owned recurring-task set with the currently desired
     * definitions. Tasks previously recorded as owned by {@code namespace} but
     * missing from {@code desiredTasks} are deleted with their schedule state.
     */
    public void reconcileRecurring(String namespace, Collection<CronTask> desiredTasks) {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(desiredTasks, "desiredTasks");
        if (namespace.isBlank()) {
            throw new IllegalArgumentException("namespace must not be blank");
        }
        Set<String> desiredNames = new HashSet<>();
        for (CronTask task : desiredTasks) {
            desiredNames.add(task.name());
            upsertCron(task);
            store.recordCronTaskOwnership(namespace, task.name());
        }
        for (String owned : store.listCronTaskNamesOwnedBy(namespace)) {
            if (!desiredNames.contains(owned)) {
                store.deleteCronTask(owned);
            }
        }
    }

    /**
     * Define a recurring task with an already-parsed trigger.
     *
     * <p>Use this when the caller has parsed the cron expression or constructed
     * the {@link CronTask.Trigger.Interval} itself — for example a framework
     * adapter that validates annotation-level recurring specs at startup and
     * does not want the {@code Scheduler} to re-parse a string. Restart
     * semantics match {@link #defineCronTask}: existing schedule state
     * (last-run, in-flight) is preserved; only the next-run timestamp is
     * recomputed from {@code now} against the new trigger.
     */
    public void defineRecurring(
            String name,
            CronTask.Trigger trigger,
            JobPayload payload,
            String handlerClassName,
            String queue,
            int priority,
            CronTask.MissedRunPolicy missedRunPolicy) {
        defineRecurring(name, trigger, payload, handlerClassName, queue, priority, null, null, missedRunPolicy);
    }

    /**
     * Full recurring registration including the per-instance overrides: an
     * execution {@code timeout} ({@code null} keeps the engine's global job
     * timeout) and a retry budget {@code maxAttempts} ({@code null} keeps the
     * {@code RetryInterceptor} defaults).
     */
    public void defineRecurring(
            String name,
            CronTask.Trigger trigger,
            JobPayload payload,
            String handlerClassName,
            String queue,
            int priority,
            Duration timeout,
            Integer maxAttempts,
            CronTask.MissedRunPolicy missedRunPolicy) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(trigger, "trigger");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(handlerClassName, "handlerClassName");
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(missedRunPolicy, "missedRunPolicy");
        upsertCron(new CronTask(
                name,
                trigger,
                handlerClassName,
                serializer.serializePayload(payload),
                queue,
                priority,
                timeout,
                maxAttempts,
                missedRunPolicy,
                ZoneId.systemDefault(),
                true));
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
        List<JobId> ids = store.insertAll(jobs);
        wakeBus.wake(queue);
        return ids;
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
        //
        // The read-modify-write of the schedule state is guarded by the same
        // store mutex the materializer takes per task: without it, a
        // concurrent materializer tick on the maintenance master (a
        // different JVM during a rolling deploy) could set inFlightJobId
        // between our read and write, and the re-registration would clobber
        // it with the stale value — defeating the pile-up guard.
        String mutex = RecurringMaterializer.taskMutexName(task.name());
        boolean locked = acquireCronMutex(mutex);
        try {
            var existingState = store.findCronTaskState(task.name());
            var now = Instant.now();
            Instant next = task.trigger().nextAfter(now, task.zone());
            store.upsertCronTask(task);
            if (existingState.isEmpty()) {
                store.upsertCronTaskState(CronTaskScheduleState.initial(task.name(), next));
            } else {
                var s = existingState.get();
                store.upsertCronTaskState(new CronTaskScheduleState(
                        task.name(), s.lastRunAt(), s.lastRunJobId(), next, s.inFlightJobId()));
            }
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

    private boolean acquireCronMutex(String mutex) {
        for (int i = 0; i < 100; i++) {
            if (store.tryAcquireMutex(mutex, cronMutexHolder, CRON_MUTEX_LEASE)) {
                return true;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        // Registration must not fail application startup; the poll-interval
        // materializer remains the correctness fallback.
        LOG.warn("Could not acquire recurring-state mutex {} — proceeding unguarded", mutex);
        return false;
    }
}
