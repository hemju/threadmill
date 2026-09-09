package com.hemju.threadmill.spring;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.internal.FatalErrors;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.store.BulkInsertBudget;
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
 *   <li>{@link #enqueueIfAbsent(Class, JobPayload, String, Duration)} is the
 *       exception: it is <strong>always immediate</strong>, even inside an
 *       active transaction. Its synchronous {@code EnqueueResult} (Created
 *       vs Coalesced) cannot be deferred, so the dedup record and the job
 *       row are written when the method is called and <em>survive a
 *       rollback</em> of the surrounding business transaction. If the
 *       enqueue must roll back with the caller, use
 *       {@code threadmill.spring.enqueue-mode=join_transaction} (Postgres)
 *       or a plain {@code enqueue()} without dedup.</li>
 * </ul>
 *
 * <p>Deferred submissions are validated before returning, with at most the store
 * bulk-insert job/byte budget per scheduler and transaction. Unconfirmed writes
 * increment {@link #deferredEnqueueFailureCount()} and notify the configured observer.
 * The business commit cannot be rolled back by this observation.
 *
 * <p>Recurring tasks defined through {@link #enqueueRecurring(Class, JobPayload, String)}
 * are <em>not</em> after-commit deferred — cron-task definitions are
 * configuration, not work, and registering them on rollback would be
 * surprising. Deferral applies only to actual job enqueue paths.
 */
public final class TransactionAwareJobScheduler extends JobScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionAwareJobScheduler.class);

  private final Consumer<AfterCommitEnqueueFailure> failureListener;
  private final LongAdder deferredEnqueueFailures = new LongAdder();

  public TransactionAwareJobScheduler(
      JobStore store,
      JobSerializer serializer,
      ThreadmillJobRegistry registry,
      ProcessingNodeConfig config) {
    this(store, serializer, registry, config, new LocalWakeBus());
  }

  public TransactionAwareJobScheduler(
      JobStore store,
      JobSerializer serializer,
      ThreadmillJobRegistry registry,
      ProcessingNodeConfig config,
      LocalWakeBus wakeBus) {
    this(store, serializer, registry, config, wakeBus, failure -> {});
  }

  /** Create a scheduler with an observer for unconfirmed after-commit inserts. */
  public TransactionAwareJobScheduler(
      JobStore store,
      JobSerializer serializer,
      ThreadmillJobRegistry registry,
      ProcessingNodeConfig config,
      LocalWakeBus wakeBus,
      Consumer<AfterCommitEnqueueFailure> failureListener) {
    super(store, serializer, registry, config, wakeBus);
    this.failureListener = Objects.requireNonNull(failureListener, "failureListener");
  }

  @Override
  public <P extends JobPayload> JobId enqueue(Class<? extends JobHandler<P>> handler, P payload) {
    var registration = registrationFor(handler, payload);
    return deferredOrImmediate(
        jobFor(payload, null, registration.priority(), null, null, registration),
        registration.queue());
  }

  @Override
  public <P extends JobPayload> JobId enqueueIn(
      Class<? extends JobHandler<P>> handler, P payload, Duration delay) {
    Objects.requireNonNull(delay, "delay");
    return enqueueAt(handler, payload, Instant.now().plus(delay));
  }

  @Override
  public <P extends JobPayload> JobId enqueueAt(
      Class<? extends JobHandler<P>> handler, P payload, Instant when) {
    Objects.requireNonNull(when, "when");
    var registration = registrationFor(handler, payload);
    // SCHEDULED state — no wake (the maintenance loop will materialize it later).
    return deferredOrImmediate(
        jobFor(payload, when, registration.priority(), null, null, registration), null);
  }

  @Override
  public <P extends JobPayload> JobId enqueueWithPriority(
      Class<? extends JobHandler<P>> handler, P payload, int priority) {
    var registration = registrationFor(handler, payload);
    return deferredOrImmediate(
        jobFor(payload, null, priority, null, null, registration), registration.queue());
  }

  @Override
  public <P extends JobPayload> List<JobId> enqueueAll(
      Class<? extends JobHandler<P>> handler, List<? extends P> payloads) {
    Objects.requireNonNull(payloads, "payloads");
    if (payloads.isEmpty()) return List.of();
    new BulkInsertBudget(payloads.size(), store.capabilities());
    ThreadmillJobRegistry.Registration registration = null;
    var jobs = new ArrayList<Job>(payloads.size());
    for (P p : payloads) {
      registration = registrationFor(handler, p);
      jobs.add(jobFor(p, null, registration.priority(), null, null, registration));
    }
    String queueToWake = registration.queue();
    if (TransactionSynchronizationManager.isSynchronizationActive()) {
      defer(jobs, () -> store.insertAll(jobs), queueToWake);
    } else {
      store.insertAll(jobs);
      wakeBus.wake(queueToWake);
    }
    var ids = new ArrayList<JobId>(jobs.size());
    for (Job j : jobs) ids.add(j.id());
    return List.copyOf(ids);
  }

  /**
   * Nudge with after-commit semantics: validation (unknown / disabled task)
   * fails fast on the calling thread, but the nudge write itself is deferred
   * to {@code afterCommit} — a rollback discards it, so producers can nudge
   * in the same transaction that writes the work row. The residual crash
   * window between the commit and the deferred write is covered by the
   * task's backstop schedule (worst case one schedule period of latency,
   * never a lost run).
   */
  @Override
  public void nudgeRecurring(String taskName) {
    DeferredNudge.onCommit(taskName, store, name -> super.nudgeRecurring(name), LOG);
  }

  @Override
  public <P extends JobPayload> EnqueueResult enqueueIfAbsent(
      Class<? extends JobHandler<P>> handler, P payload, String dedupKey, Duration ttl) {
    // Dedup must return a meaningful EnqueueResult (Created vs Coalesced)
    // synchronously — we cannot defer it without changing the API. So
    // dedup writes are always immediate. Use a plain enqueue path if
    // after-commit semantics matter more than dedup.
    if (TransactionSynchronizationManager.isSynchronizationActive()) {
      LOG.debug(
          "enqueueIfAbsent(dedupKey={}) called inside an active transaction: the dedup write is "
              + "immediate and will survive a rollback of the surrounding transaction",
          dedupKey);
    }
    return super.enqueueIfAbsent(handler, payload, dedupKey, ttl);
  }

  /**
   * @param queueToWake the queue to wake after the insert lands, or {@code null}
   *                    for SCHEDULED-state inserts where no wake is appropriate
   */
  private JobId deferredOrImmediate(Job job, String queueToWake) {
    if (TransactionSynchronizationManager.isSynchronizationActive()) {
      defer(List.of(job), () -> store.insert(job), queueToWake);
      return job.id();
    }
    store.insert(job);
    if (queueToWake != null) wakeBus.wake(queueToWake);
    return job.id();
  }

  /** Number of reserved job ids whose after-commit persistence was unconfirmed. */
  public long deferredEnqueueFailureCount() {
    return deferredEnqueueFailures.sum();
  }

  private void defer(List<Job> jobs, Runnable insert, String queueToWake) {
    long bytes = 0;
    for (var job : jobs) {
      bytes += serializer
          .serializeJob(job.snapshot(), store.capabilities())
          .getBytes(StandardCharsets.UTF_8)
          .length;
    }
    DeferredBudget budget = null;
    for (var synchronization : TransactionSynchronizationManager.getSynchronizations()) {
      if (synchronization instanceof DeferredBudget candidate && candidate.owner == this) {
        budget = candidate;
        break;
      }
    }
    if (budget == null) {
      budget = new DeferredBudget(this);
      // Reserve before registering, so a rejected submission has no callback.
      budget.reserve(jobs.size(), bytes);
      TransactionSynchronizationManager.registerSynchronization(budget);
    } else {
      budget.reserve(jobs.size(), bytes);
    }
    var ids = jobs.stream().map(Job::id).toList();
    TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
      @Override
      public void afterCommit() {
        try {
          insert.run();
        } catch (RuntimeException failure) {
          deferredEnqueueFailures.add(ids.size());
          LOG.error(
              "Threadmill after-commit enqueue failed; persistence is unconfirmed for jobs {}",
              ids,
              failure);
          try {
            failureListener.accept(new AfterCommitEnqueueFailure(ids, failure));
          } catch (Throwable observerFailure) {
            FatalErrors.rethrowIfFatal(observerFailure);
            LOG.error("Threadmill after-commit failure observer failed", observerFailure);
          }
          return;
        }
        if (queueToWake != null) wakeBus.wake(queueToWake);
      }
    });
  }

  // Synchronization-scoped, so REQUIRES_NEW suspension cannot borrow an outer budget.
  private static final class DeferredBudget implements TransactionSynchronization {
    private final TransactionAwareJobScheduler owner;
    private int count;
    private long bytes;

    DeferredBudget(TransactionAwareJobScheduler owner) {
      this.owner = owner;
    }

    void reserve(int additionalCount, long additionalBytes) {
      var capabilities = owner.store.capabilities();
      if ((long) count + additionalCount > capabilities.maxBulkInsertJobs()
          || bytes + additionalBytes > capabilities.maxBulkInsertBytes()) {
        throw new IllegalArgumentException("Deferred enqueue transaction exceeds "
            + capabilities.maxBulkInsertJobs() + " jobs or " + capabilities.maxBulkInsertBytes()
            + " serialized bytes; use smaller transactions");
      }
      count += additionalCount;
      bytes += additionalBytes;
    }
  }
}
