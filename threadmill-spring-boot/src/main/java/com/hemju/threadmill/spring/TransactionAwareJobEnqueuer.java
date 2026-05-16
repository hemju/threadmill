package com.hemju.threadmill.spring;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.store.JobStore;

/**
 * {@link JobEnqueuer} wrapper that defers store writes until {@code afterCommit}
 * when called inside an active Spring transaction synchronisation.
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li>If no synchronisation is active, this enqueuer behaves identically to
 *       its superclass — writes happen synchronously.</li>
 *   <li>If a synchronisation is active, the {@link Job} (including its
 *       {@link JobId}) is built synchronously, but the
 *       {@link JobStore#insert(Job)} call is registered via
 *       {@link TransactionSynchronization#afterCommit()}. A rollback leaves
 *       nothing in the store.</li>
 *   <li>The returned {@code JobId} is the reserved id, available before the
 *       row exists. Callers depending on {@code findById(id)} succeeding
 *       immediately after {@code enqueue()} returns should disable this
 *       feature via {@code threadmill.spring.enqueue-after-commit=false}.</li>
 * </ul>
 *
 * <p>Recurring tasks defined through {@link #enqueueRecurring(JobPayload, String)}
 * are <em>not</em> after-commit deferred — cron-task definitions are
 * configuration, not work, and registering them on rollback would be
 * surprising. Deferral applies only to actual job enqueue paths.
 */
public final class TransactionAwareJobEnqueuer extends JobEnqueuer {

    public TransactionAwareJobEnqueuer(
            JobStore store, JobSerializer serializer, ThreadmillJobRegistry registry, ProcessingNodeConfig config) {
        super(store, serializer, registry, config);
    }

    @Override
    public JobId enqueue(JobPayload payload) {
        return deferredOrImmediate(
                jobFor(payload, null, registry.registrationFor(payload).priority()));
    }

    @Override
    public JobId enqueueIn(JobPayload payload, Duration delay) {
        return enqueueAt(payload, Instant.now().plus(delay));
    }

    @Override
    public JobId enqueueAt(JobPayload payload, Instant when) {
        return deferredOrImmediate(
                jobFor(payload, when, registry.registrationFor(payload).priority()));
    }

    @Override
    public JobId enqueueWithPriority(JobPayload payload, int priority) {
        return deferredOrImmediate(jobFor(payload, null, priority));
    }

    @Override
    public List<JobId> enqueueAll(List<? extends JobPayload> payloads) {
        if (payloads.isEmpty()) return List.of();
        var jobs = new ArrayList<Job>(payloads.size());
        for (JobPayload p : payloads) {
            jobs.add(jobFor(p, null, registry.registrationFor(p).priority()));
        }
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    store.insertAll(jobs);
                }
            });
        } else {
            store.insertAll(jobs);
        }
        var ids = new ArrayList<JobId>(jobs.size());
        for (Job j : jobs) ids.add(j.id());
        return List.copyOf(ids);
    }

    @Override
    public EnqueueResult enqueueIfAbsent(JobPayload payload, String dedupKey, Duration ttl) {
        // Dedup must return a meaningful EnqueueResult (Created vs Coalesced)
        // synchronously — we cannot defer it without changing the API. So
        // dedup writes are always immediate. Use a plain enqueue path if
        // after-commit semantics matter more than dedup.
        return super.enqueueIfAbsent(payload, dedupKey, ttl);
    }

    private JobId deferredOrImmediate(Job job) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    store.insert(job);
                }
            });
            return job.id();
        }
        store.insert(job);
        return job.id();
    }
}
