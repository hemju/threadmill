package com.hemju.threadmill.spring;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.store.JobStore;

/**
 * Scheduler variant for stores that can join the caller's Spring transaction.
 *
 * <p>The job row is written synchronously. When a Spring transaction is active,
 * the store participates in that transaction and local wake signals are delayed
 * until commit so workers never race an uncommitted row.
 */
public final class TransactionJoinedJobScheduler extends JobScheduler {

    public TransactionJoinedJobScheduler(
            JobStore store,
            JobSerializer serializer,
            ThreadmillJobRegistry registry,
            ProcessingNodeConfig config,
            LocalWakeBus wakeBus) {
        super(store, serializer, registry, config, wakeBus);
    }

    @Override
    public <P extends JobPayload> JobId enqueue(Class<? extends JobHandler<P>> handler, P payload) {
        var registration = registrationFor(handler, payload);
        Job job = jobFor(payload, null, registration.priority(), null, null, registration);
        store.insert(job);
        wakeAfterCommitOrNow(registration.queue());
        return job.id();
    }

    @Override
    public <P extends JobPayload> JobId enqueueIn(Class<? extends JobHandler<P>> handler, P payload, Duration delay) {
        Objects.requireNonNull(delay, "delay");
        return enqueueAt(handler, payload, Instant.now().plus(delay));
    }

    @Override
    public <P extends JobPayload> JobId enqueueAt(Class<? extends JobHandler<P>> handler, P payload, Instant when) {
        Objects.requireNonNull(when, "when");
        var registration = registrationFor(handler, payload);
        Job job = jobFor(payload, when, registration.priority(), null, null, registration);
        store.insert(job);
        return job.id();
    }

    @Override
    public <P extends JobPayload> JobId enqueueWithPriority(
            Class<? extends JobHandler<P>> handler, P payload, int priority) {
        var registration = registrationFor(handler, payload);
        Job job = jobFor(payload, null, priority, null, null, registration);
        store.insert(job);
        wakeAfterCommitOrNow(registration.queue());
        return job.id();
    }

    @Override
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
        wakeAfterCommitOrNow(registration.queue());
        return ids;
    }

    @Override
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
            wakeAfterCommitOrNow(registration.queue());
        }
        return result;
    }

    private void wakeAfterCommitOrNow(String queue) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            wakeBus.wake(queue);
            return;
        }
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            throw new IllegalStateException(
                    "Spring transaction is active but transaction synchronization is not active; cannot defer Threadmill wake signal");
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                wakeBus.wake(queue);
            }
        });
    }
}
