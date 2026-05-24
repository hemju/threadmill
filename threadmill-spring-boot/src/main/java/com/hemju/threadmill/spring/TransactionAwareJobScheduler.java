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
 * {@link JobScheduler} wrapper that defers store writes until {@code afterCommit}
 * when called inside an active Spring transaction synchronisation.
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li>If no synchronisation is active, this scheduler behaves identically to
 *       its superclass — writes happen synchronously and any producer-side wake
 *       on {@link LocalWakeBus} fires before {@code enqueue()} returns.</li>
 *   <li>If a synchronisation is active, the {@link Job} (including its
 *       {@link JobId}) is built synchronously, but the
 *       {@link JobStore#insert(Job)} call AND the wake-bus signal are registered
 *       via {@link TransactionSynchronization#afterCommit()}. A rollback leaves
 *       nothing in the store and nothing is signaled. (Signalling before commit
 *       would be wrong — the dispatcher would race a row that does not yet
 *       exist.)</li>
 *   <li>The returned {@code JobId} is the reserved id, available before the
 *       row exists. Callers depending on {@code findById(id)} succeeding
 *       immediately after {@code enqueue()} returns should use
 *       {@code threadmill.spring.enqueue-mode=immediate}.</li>
 * </ul>
 *
 * <p>Recurring tasks defined through {@link #enqueueRecurring(Class, JobPayload, String)}
 * are <em>not</em> after-commit deferred — cron-task definitions are
 * configuration, not work, and registering them on rollback would be
 * surprising. Deferral applies only to actual job enqueue paths.
 */
public final class TransactionAwareJobScheduler extends JobScheduler {

    public TransactionAwareJobScheduler(
            JobStore store, JobSerializer serializer, ThreadmillJobRegistry registry, ProcessingNodeConfig config) {
        super(store, serializer, registry, config);
    }

    public TransactionAwareJobScheduler(
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
        return deferredOrImmediate(
                jobFor(payload, null, registration.priority(), null, null, registration), registration.queue());
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
        // SCHEDULED state — no wake (the maintenance loop will materialize it later).
        return deferredOrImmediate(jobFor(payload, when, registration.priority(), null, null, registration), null);
    }

    @Override
    public <P extends JobPayload> JobId enqueueWithPriority(
            Class<? extends JobHandler<P>> handler, P payload, int priority) {
        var registration = registrationFor(handler, payload);
        return deferredOrImmediate(jobFor(payload, null, priority, null, null, registration), registration.queue());
    }

    @Override
    public <P extends JobPayload> List<JobId> enqueueAll(
            Class<? extends JobHandler<P>> handler, List<? extends P> payloads) {
        Objects.requireNonNull(payloads, "payloads");
        if (payloads.isEmpty()) return List.of();
        ThreadmillJobRegistry.Registration registration = null;
        var jobs = new ArrayList<Job>(payloads.size());
        for (P p : payloads) {
            registration = registrationFor(handler, p);
            jobs.add(jobFor(p, null, registration.priority(), null, null, registration));
        }
        String queueToWake = registration.queue();
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    store.insertAll(jobs);
                    wakeBus.wake(queueToWake);
                }
            });
        } else {
            store.insertAll(jobs);
            wakeBus.wake(queueToWake);
        }
        var ids = new ArrayList<JobId>(jobs.size());
        for (Job j : jobs) ids.add(j.id());
        return List.copyOf(ids);
    }

    @Override
    public <P extends JobPayload> EnqueueResult enqueueIfAbsent(
            Class<? extends JobHandler<P>> handler, P payload, String dedupKey, Duration ttl) {
        // Dedup must return a meaningful EnqueueResult (Created vs Coalesced)
        // synchronously — we cannot defer it without changing the API. So
        // dedup writes are always immediate. Use a plain enqueue path if
        // after-commit semantics matter more than dedup.
        return super.enqueueIfAbsent(handler, payload, dedupKey, ttl);
    }

    /**
     * @param queueToWake the queue to wake after the insert lands, or {@code null}
     *                    for SCHEDULED-state inserts where no wake is appropriate
     */
    private JobId deferredOrImmediate(Job job, String queueToWake) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    store.insert(job);
                    if (queueToWake != null) wakeBus.wake(queueToWake);
                }
            });
            return job.id();
        }
        store.insert(job);
        if (queueToWake != null) wakeBus.wake(queueToWake);
        return job.id();
    }
}
