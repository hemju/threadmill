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
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.engine.RetryInterceptor;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.schedule.CronExpression;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskId;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStore;

/** Spring-friendly enqueue API that routes jobs by payload type. */
public class JobEnqueuer {

    protected final JobStore store;
    protected final JobSerializer serializer;
    protected final ThreadmillJobRegistry registry;
    protected final ProcessingNodeConfig config;

    public JobEnqueuer(
            JobStore store, JobSerializer serializer, ThreadmillJobRegistry registry, ProcessingNodeConfig config) {
        this.store = Objects.requireNonNull(store, "store");
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.config = Objects.requireNonNull(config, "config");
    }

    public JobId enqueue(JobPayload payload) {
        Job job = jobFor(payload, null, registry.registrationFor(payload).priority());
        store.insert(job);
        return job.id();
    }

    public JobId enqueueIn(JobPayload payload, Duration delay) {
        Objects.requireNonNull(delay, "delay");
        return enqueueAt(payload, Instant.now().plus(delay));
    }

    public JobId enqueueAt(JobPayload payload, Instant when) {
        Objects.requireNonNull(when, "when");
        Job job = jobFor(payload, when, registry.registrationFor(payload).priority());
        store.insert(job);
        return job.id();
    }

    public JobId enqueueWithPriority(JobPayload payload, int priority) {
        Job job = jobFor(payload, null, priority);
        store.insert(job);
        return job.id();
    }

    /**
     * Enqueue a batch of payloads in one logical operation. Either all are
     * persisted or none — backed by {@code JobStore.insertAll}.
     *
     * <p>Each payload is routed by its runtime type, so a batch may mix
     * payload types as long as every type has a registered handler.
     */
    public List<JobId> enqueueAll(List<? extends JobPayload> payloads) {
        Objects.requireNonNull(payloads, "payloads");
        if (payloads.isEmpty()) return List.of();
        var jobs = new ArrayList<Job>(payloads.size());
        for (JobPayload p : payloads) {
            jobs.add(jobFor(p, null, registry.registrationFor(p).priority()));
        }
        return store.insertAll(jobs);
    }

    public EnqueueResult enqueueIfAbsent(JobPayload payload, String dedupKey, Duration ttl) {
        Objects.requireNonNull(ttl, "ttl");
        if (ttl.compareTo(config.maxDedupTtl()) > 0) {
            throw new IllegalArgumentException("dedup ttl must not exceed " + config.maxDedupTtl());
        }
        Job job = jobFor(payload, null, registry.registrationFor(payload).priority(), dedupKey, ttl);
        return store.enqueueIfAbsent(job, dedupKey, ttl, Instant.now());
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

    public CronTaskId enqueueRecurring(JobPayload payload, String cron) {
        Objects.requireNonNull(cron, "cron");
        var registration = registry.registrationFor(payload);
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

    protected Job jobFor(JobPayload payload, Instant scheduledFor, int priority) {
        return jobFor(payload, scheduledFor, priority, null, null);
    }

    protected Job jobFor(JobPayload payload, Instant scheduledFor, int priority, String dedupKey, Duration dedupTtl) {
        Objects.requireNonNull(payload, "payload");
        var registration = registry.registrationFor(payload);
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
