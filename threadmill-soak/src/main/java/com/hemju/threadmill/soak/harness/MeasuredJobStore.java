package com.hemju.threadmill.soak.harness;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.store.ForwardingJobStore;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.core.store.RetentionPage;

/** Harness-only operation timing with fixed-size recent sample windows and cumulative counters. */
final class MeasuredJobStore extends ForwardingJobStore {
  private final Map<String, Measurement> measurements = new ConcurrentHashMap<>();
  private final LongAdder jobsDeleted = new LongAdder();
  private final LongAdder groupsDeleted = new LongAdder();

  MeasuredJobStore(JobStore delegate) {
    super(delegate);
  }

  @Override
  public void insert(Job job) {
    measure("enqueue", () -> {
      super.insert(job);
      return null;
    });
  }

  @Override
  public List<JobId> insertAll(List<Job> jobs) {
    return measure("bulkEnqueue", () -> super.insertAll(jobs));
  }

  @Override
  public EnqueueResult enqueueIfAbsent(Job job, String key, Duration ttl, Instant now) {
    return measure("dedupEnqueue", () -> super.enqueueIfAbsent(job, key, ttl, now));
  }

  @Override
  public List<Job> claimReady(NodeId node, String queue, int max, Instant now) {
    return measure("claim", () -> super.claimReady(node, queue, max, now));
  }

  @Override
  public void saveAtomic(Job job, long version) {
    String operation = job.currentState().isTerminal() || job.currentState() == JobState.FAILED
        ? "terminalSave"
        : "stateSave";
    measure(operation, () -> {
      super.saveAtomic(job, version);
      return null;
    });
  }

  @Override
  public RetentionPage deleteFinishedPage(Instant cutoff, JobState state, int max, JobId after) {
    var page = measure("retentionPage", () -> super.deleteFinishedPage(cutoff, state, max, after));
    jobsDeleted.add(page.deleted());
    return page;
  }

  @Override
  public long deleteIdleConcurrencyGroups(int max) {
    long deleted = measure("concurrencyCleanup", () -> super.deleteIdleConcurrencyGroups(max));
    groupsDeleted.add(deleted);
    return deleted;
  }

  long jobsDeleted() {
    return jobsDeleted.sum();
  }

  long groupsDeleted() {
    return groupsDeleted.sum();
  }

  Map<String, Object> snapshot() {
    var result = new LinkedHashMap<String, Object>();
    measurements.forEach((name, value) -> result.put(
        name,
        Map.of(
            "calls",
            value.calls.sum(),
            "failures",
            value.failures.sum(),
            "p50Micros",
            value.window.snapshotPercentile(0.50),
            "p95Micros",
            value.window.snapshotPercentile(0.95),
            "p99Micros",
            value.window.snapshotPercentile(0.99))));
    return result;
  }

  private <T> T measure(String operation, Supplier<T> action) {
    var measurement = measurements.computeIfAbsent(operation, ignored -> new Measurement());
    long started = System.nanoTime();
    try {
      return action.get();
    } catch (RuntimeException | Error failure) {
      measurement.failures.increment();
      throw failure;
    } finally {
      measurement.calls.increment();
      measurement.window.add((System.nanoTime() - started) / 1000);
    }
  }

  private static final class Measurement {
    final LongAdder calls = new LongAdder();
    final LongAdder failures = new LongAdder();
    final LatencyTracker.RecentWindow window = new LatencyTracker.RecentWindow(4096);
  }
}
