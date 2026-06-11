package com.hemju.threadmill.spring;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.engine.JobRunner;
import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.engine.RetryInterceptor;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.schedule.CronExpression;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskId;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStore;

/**
 * Spring-friendly scheduling API.
 *
 * <p>Every method takes the target handler class as the first argument and the
 * payload as the second. The generic constraint
 * {@code Class<? extends JobHandler<P>>} paired with {@code P payload} makes a
 * mismatch a {@code javac} error — the only way to enqueue a payload whose
 * handler is unregistered or whose type is wrong is to bypass the type system
 * (raw types, reflection). For no-payload handlers, pair the handler class
 * with {@code NoPayload.INSTANCE}; declare those handlers as
 * {@link com.hemju.threadmill.core.handler.JobAction} so the payload-type
 * parameter does not appear in the user-facing signature.
 *
 * <p>A defensive runtime check still cross-references the supplied handler
 * against the {@code @Job} registration at enqueue time: callers that lie via
 * raw types or reflection trip {@link IllegalStateException} before a job is
 * written to the store.
 *
 * <p>For background on transactional behaviour see
 * {@link TransactionAwareJobScheduler}, which is the default {@code JobScheduler}
 * bean under Spring's auto-configuration.
 */
public class JobScheduler {

    protected final JobStore store;
    protected final JobSerializer serializer;
    protected final ThreadmillJobRegistry registry;
    protected final ProcessingNodeConfig config;
    protected final LocalWakeBus wakeBus;

    public JobScheduler(
            JobStore store, JobSerializer serializer, ThreadmillJobRegistry registry, ProcessingNodeConfig config) {
        this(store, serializer, registry, config, new LocalWakeBus());
    }

    public JobScheduler(
            JobStore store,
            JobSerializer serializer,
            ThreadmillJobRegistry registry,
            ProcessingNodeConfig config,
            LocalWakeBus wakeBus) {
        this.store = Objects.requireNonNull(store, "store");
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.config = Objects.requireNonNull(config, "config");
        this.wakeBus = Objects.requireNonNull(wakeBus, "wakeBus");
    }

    public <P extends JobPayload> JobId enqueue(Class<? extends JobHandler<P>> handler, P payload) {
        var registration = registrationFor(handler, payload);
        Job job = jobFor(payload, null, registration.priority(), null, null, registration);
        store.insert(job);
        wakeBus.wake(registration.queue());
        return job.id();
    }

    public <P extends JobPayload> JobId enqueueIn(Class<? extends JobHandler<P>> handler, P payload, Duration delay) {
        Objects.requireNonNull(delay, "delay");
        return enqueueAt(handler, payload, Instant.now().plus(delay));
    }

    public <P extends JobPayload> JobId enqueueAt(Class<? extends JobHandler<P>> handler, P payload, Instant when) {
        Objects.requireNonNull(when, "when");
        var registration = registrationFor(handler, payload);
        Job job = jobFor(payload, when, registration.priority(), null, null, registration);
        // SCHEDULED jobs aren't claimable yet; maintenance picks them up at the due time
        // and the wake will fire from there. Producer-side wake here would be a no-op.
        store.insert(job);
        return job.id();
    }

    public <P extends JobPayload> JobId enqueueWithPriority(
            Class<? extends JobHandler<P>> handler, P payload, int priority) {
        var registration = registrationFor(handler, payload);
        Job job = jobFor(payload, null, priority, null, null, registration);
        store.insert(job);
        wakeBus.wake(registration.queue());
        return job.id();
    }

    /**
     * Enqueue a batch of payloads in one logical operation. Either all are
     * persisted or none — backed by {@code JobStore.insertAll}.
     *
     * <p>All payloads must share the same handler type, which is the price of
     * compile-time safety. For mixed-handler batches, call {@link #enqueue}
     * once per payload.
     */
    public <P extends JobPayload> List<JobId> enqueueAll(
            Class<? extends JobHandler<P>> handler, List<? extends P> payloads) {
        Objects.requireNonNull(handler, "handler");
        Objects.requireNonNull(payloads, "payloads");
        if (payloads.isEmpty()) return List.of();
        ThreadmillJobRegistry.Registration registration = null;
        var jobs = new ArrayList<Job>(payloads.size());
        for (P p : payloads) {
            registration = registrationFor(handler, p);
            jobs.add(jobFor(p, null, registration.priority(), null, null, registration));
        }
        List<JobId> ids = store.insertAll(jobs);
        wakeBus.wake(registration.queue());
        return ids;
    }

    public <P extends JobPayload> EnqueueResult enqueueIfAbsent(
            Class<? extends JobHandler<P>> handler, P payload, String dedupKey, Duration ttl) {
        Objects.requireNonNull(dedupKey, "dedupKey");
        Objects.requireNonNull(ttl, "ttl");
        if (ttl.compareTo(config.maxDedupTtl()) > 0) {
            throw new IllegalArgumentException("dedup ttl must not exceed " + config.maxDedupTtl());
        }
        var registration = registrationFor(handler, payload);
        Job job = jobFor(payload, null, registration.priority(), dedupKey, ttl, registration);
        EnqueueResult result = store.enqueueIfAbsent(job, dedupKey, ttl, Instant.now());
        if (result instanceof EnqueueResult.Created) {
            wakeBus.wake(registration.queue());
        }
        return result;
    }

    // ---------------------------------------------------------------- queue pauses

    /** Pause claiming from {@code queue}; pending jobs stay {@code ENQUEUED}. */
    public void pauseQueue(String queue, String reason) {
        store.pauseQueue(queue, reason);
    }

    /** Resume claiming from {@code queue}. */
    public void resumeQueue(String queue) {
        store.resumeQueue(queue);
    }

    /** Snapshot of queues currently paused. */
    public Set<String> pausedQueues() {
        return store.listPausedQueues();
    }

    public <P extends JobPayload> CronTaskId enqueueRecurring(
            Class<? extends JobHandler<P>> handler, P payload, String cron) {
        Objects.requireNonNull(cron, "cron");
        var registration = registrationFor(handler, payload);
        String name = registration.payloadType().getSimpleName() + "-" + UUID.randomUUID();
        var expression = CronExpression.parse(cron);
        ZoneId zone = ZoneId.systemDefault();
        JobArgument arg = serializer.serializePayload(payload);
        store.upsertCronTask(new CronTask(
                name,
                new CronTask.Trigger.CronExpr(expression),
                registration.handlerType().getName(),
                arg,
                registration.queue(),
                registration.priority(),
                CronTask.MissedRunPolicy.DROP,
                zone,
                true));
        store.upsertCronTaskState(CronTaskScheduleState.initial(name, expression.nextAfter(Instant.now(), zone)));
        return new CronTaskId(name);
    }

    /**
     * Resolve the registration by handler class and verify the supplied
     * {@code payload} matches the handler's declared payload type. Generics
     * make a mismatch impossible at compile time, but runtime callers
     * (reflection, mocks, tests) can still lie; this guard keeps
     * "wrong handler" out of the consumer side.
     *
     * <p>Routing by handler class (rather than payload type) is what lets
     * multiple handlers share the same payload type — most notably any number
     * of {@link com.hemju.threadmill.core.handler.JobAction} beans, which all
     * declare {@code NoPayload}.
     */
    protected <P extends JobPayload> ThreadmillJobRegistry.Registration registrationFor(
            Class<? extends JobHandler<P>> handler, P payload) {
        Objects.requireNonNull(handler, "handler");
        Objects.requireNonNull(payload, "payload");
        var registration = registry.registrationFor(handler);
        if (!registration.payloadType().isInstance(payload)) {
            throw new IllegalStateException("Payload " + payload.getClass().getName()
                    + " is not a " + registration.payloadType().getName()
                    + " (handler " + handler.getName() + " declared that payload type)");
        }
        if (payload.getClass() != registration.payloadType()) {
            // isInstance admits subtypes — but a subtype with its own
            // registered handler is genuinely ambiguous dispatch, and
            // routing it to the supertype's handler is almost always a bug.
            // The deliberate NoPayload multi-handler path is unaffected:
            // every JobAction payload IS exactly NoPayload.
            for (var own : registry.registrationsForExactPayloadType(payload.getClass())) {
                if (!own.handlerType().equals(registration.handlerType())) {
                    throw new IllegalStateException(
                            "Payload " + payload.getClass().getName()
                                    + " has its own registered handler "
                                    + own.handlerType().getName()
                                    + "; refusing to route it to " + handler.getName()
                                    + " (declared for supertype "
                                    + registration.payloadType().getName() + ")");
                }
            }
        }
        return registration;
    }

    protected Job jobFor(
            JobPayload payload,
            Instant scheduledFor,
            int priority,
            String dedupKey,
            Duration dedupTtl,
            ThreadmillJobRegistry.Registration registration) {
        JobArgument arg = serializer.serializePayload(payload);
        var builder = Job.builder()
                .spec(new JobSpec(registration.handlerType().getName(), List.of(arg), dedupKey, dedupTtl))
                .queue(registration.queue())
                .priority(priority);
        if (scheduledFor != null) {
            builder.initialState(JobState.SCHEDULED).scheduledFor(scheduledFor);
        }
        Job job = builder.build();
        job.metadata().put(RetryInterceptor.META_MAX_ATTEMPTS, Integer.toString(registration.maxRetries()));
        job.metadata()
                .put(
                        JobRunner.META_TIMEOUT_SECONDS,
                        Long.toString(registration.timeout().toSeconds()));
        return job;
    }
}
