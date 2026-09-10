package com.hemju.threadmill.soak.harness;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;

import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.store.ForwardingJobStore;
import com.hemju.threadmill.core.store.JobStore;

/**
 * Harness-only producer recovery for bounded datastore outages. Reuses the same
 * job IDs and reconciles uncertain acknowledgements before retrying. Worker
 * operations use the original store, so their recovery remains under test.
 */
final class RecoveringProducerStore extends ForwardingJobStore {
  private final SoakTraceWriter trace;
  private final BooleanSupplier aborted;
  private final Duration recoveryBudget;

  RecoveringProducerStore(JobStore store, SoakTraceWriter trace, BooleanSupplier aborted) {
    this(store, trace, aborted, Duration.ofMinutes(2));
  }

  RecoveringProducerStore(
      JobStore store, SoakTraceWriter trace, BooleanSupplier aborted, Duration recoveryBudget) {
    super(store);
    this.trace = trace;
    this.aborted = aborted;
    this.recoveryBudget = recoveryBudget;
  }

  @Override
  public void insert(Job job) {
    recover(
        "insert",
        () -> {
          delegate().insert(job);
          return job.id();
        },
        () -> confirmed(job) ? Optional.of(job.id()) : Optional.empty());
  }

  @Override
  public List<JobId> insertAll(List<Job> jobs) {
    return recover("insertAll", () -> delegate().insertAll(jobs), () -> {
      int found = 0;
      for (var job : jobs) {
        if (confirmed(job)) found++;
      }
      if (found == jobs.size()) return Optional.of(jobs.stream().map(Job::id).toList());
      if (found != 0) {
        throw new IllegalStateException("Cannot reconcile a partially visible producer batch");
      }
      return Optional.empty();
    });
  }

  @Override
  public EnqueueResult enqueueIfAbsent(Job job, String key, Duration ttl, Instant now) {
    boolean alreadyInserted = job.version() > 0;
    return recover(
        "enqueueIfAbsent",
        () -> delegate().enqueueIfAbsent(job, key, ttl, now),
        () -> confirmed(job)
            ? Optional.of(
                alreadyInserted
                    ? new EnqueueResult.Coalesced(job.id())
                    : new EnqueueResult.Created(job.id()))
            : Optional.empty());
  }

  private boolean confirmed(Job job) {
    var persisted = delegate().findById(job.id());
    if (persisted.isEmpty()) return false;
    var stored = persisted.get();
    if (!stored.spec().equals(job.spec()) || !stored.createdAt().equals(job.createdAt())) {
      throw new IllegalStateException("Producer recovery found a conflicting job ID: " + job.id());
    }
    // Only confirm the original insert; don't copy later worker state into the producer object.
    if (job.version() == 0) job.adoptVersion(1);
    return true;
  }

  private <T> T recover(String operation, Supplier<T> write, Supplier<Optional<T>> reconcile) {
    long started = System.nanoTime();
    RuntimeException firstFailure = null;
    while (true) {
      try {
        T result;
        if (firstFailure == null) {
          result = write.get();
        } else {
          var known = reconcile.get();
          result = known.isPresent() ? known.get() : write.get();
        }
        if (firstFailure != null) {
          trace.emit(
              "producer_recovered",
              Map.of(
                  "operation", operation, "elapsedMs", (System.nanoTime() - started) / 1_000_000));
        }
        return result;
      } catch (RuntimeException failure) {
        if (!isOutage(failure)) throw failure;
        if (firstFailure == null) {
          firstFailure = failure;
          trace.emit(
              "producer_outage",
              Map.of("operation", operation, "failureType", failure.getClass().getSimpleName()));
        }
        if (aborted.getAsBoolean() || System.nanoTime() - started >= recoveryBudget.toNanos()) {
          throw firstFailure;
        }
        try {
          Thread.sleep(200);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw firstFailure;
        }
      }
    }
  }

  private static boolean isOutage(Throwable failure) {
    for (var cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof RedisCommandTimeoutException
          || cause instanceof RedisConnectionException) {
        return true;
      }
      if (cause instanceof SQLException sql
          && sql.getSQLState() != null
          && (sql.getSQLState().startsWith("08")
              || sql.getSQLState().equals("57P01")
              || sql.getSQLState().equals("57P02")
              || sql.getSQLState().equals("57P03"))) {
        // Existing sessions and new connections report different states during a restart.
        return true;
      }
    }
    return false;
  }
}
