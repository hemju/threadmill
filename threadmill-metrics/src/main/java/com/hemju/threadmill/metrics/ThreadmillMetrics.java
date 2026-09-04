package com.hemju.threadmill.metrics;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.engine.JobInterceptor;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.store.JobStore;

/**
 * Micrometer integration for Threadmill.
 *
 * <p>Store-derived gauges use a pull-through cache: the first gauge read after
 * the configured refresh interval reloads one atomic snapshot from the store.
 * This keeps gauges current while processing is stalled or queues are paused,
 * without a background thread or a store read per meter. A failed refresh
 * retains the last successful snapshot and marks it stale.
 *
 * <p>Register the {@link #asInterceptor()} return value with the
 * {@code ProcessingNode.Builder} for job lifecycle meters. Use the
 * {@link #meteredStore()} return value everywhere the backing store would have
 * been used — by processing nodes and producers — so claim and write-boundary
 * meters cover the actual operations.
 */
public final class ThreadmillMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(ThreadmillMetrics.class);

  /** Default minimum interval between store-derived gauge refresh attempts. */
  public static final Duration DEFAULT_REFRESH_INTERVAL = Duration.ofSeconds(1);

  /** Default maximum number of distinct queue values exported as metric tags. */
  public static final int DEFAULT_MAX_QUEUE_TAGS = 100;

  private final MeterRegistry registry;
  private final JobStore store;
  private final JobStore meteredStore;
  private final long refreshIntervalNanos;
  private final int maxQueueTags;
  private final AtomicReference<StoreSnapshot> snapshot =
      new AtomicReference<>(StoreSnapshot.empty());
  private final AtomicBoolean snapshotStale = new AtomicBoolean(true);
  private final ReentrantLock refreshLock = new ReentrantLock();
  private final AtomicInteger consecutiveRefreshFailures = new AtomicInteger();
  private final AtomicInteger consecutiveMeterFailures = new AtomicInteger();
  private final Map<String, QueueMeters> queueMeters = new ConcurrentHashMap<>();
  private final Counter processedCounter;
  private final Counter refreshErrors;
  private final Counter meterReconciliationErrors;
  private final Counter claimFailures;
  private final Counter orphanReclaims;
  private final Map<String, Counter> failedCounters = new ConcurrentHashMap<>();
  private final Map<String, Counter> recurringRunCounters = new ConcurrentHashMap<>();
  private final Map<String, Counter> rejectedWriteCounters = new ConcurrentHashMap<>();
  private final Timer processingTime;
  private final Timer claimLatency;
  private final ConcurrentHashMap<String, Instant> inFlightStart = new ConcurrentHashMap<>();

  private volatile long lastRefreshNanos = Long.MIN_VALUE;

  /** Create metrics with a one-second refresh interval and at most 100 queue tags. */
  public ThreadmillMetrics(MeterRegistry registry, JobStore store) {
    this(registry, store, DEFAULT_REFRESH_INTERVAL, DEFAULT_MAX_QUEUE_TAGS);
  }

  /**
   * Create metrics with explicit pull-refresh and queue-cardinality bounds.
   *
   * @param registry registry receiving the meters
   * @param store backing store used for gauge snapshots
   * @param refreshInterval minimum interval between refresh attempts; must be positive
   * @param maxQueueTags maximum active queue names exported as tags; zero disables per-queue meters
   */
  public ThreadmillMetrics(
      MeterRegistry registry, JobStore store, Duration refreshInterval, int maxQueueTags) {
    this.registry = Objects.requireNonNull(registry, "registry");
    this.store = Objects.requireNonNull(store, "store");
    Objects.requireNonNull(refreshInterval, "refreshInterval");
    if (refreshInterval.isZero() || refreshInterval.isNegative()) {
      throw new IllegalArgumentException("refreshInterval must be positive");
    }
    if (maxQueueTags < 0) {
      throw new IllegalArgumentException("maxQueueTags must not be negative");
    }
    this.refreshIntervalNanos = refreshInterval.toNanos();
    this.maxQueueTags = maxQueueTags;
    this.meteredStore = new MeteredJobStore(store, this);

    this.processedCounter = Counter.builder("threadmill.jobs.processed")
        .description("Total successfully-processed Threadmill jobs since startup")
        .register(registry);
    this.refreshErrors = Counter.builder("threadmill.metrics.refresh.errors")
        .description("Errors while refreshing Threadmill metrics from the store")
        .register(registry);
    this.meterReconciliationErrors = Counter.builder("threadmill.metrics.queue.meter.errors")
        .description("Errors while reconciling bounded per-queue meters")
        .register(registry);
    this.claimFailures = Counter.builder("threadmill.claim.failures")
        .description("JobStore claimReady calls that failed")
        .register(registry);
    this.orphanReclaims = Counter.builder("threadmill.jobs.orphan.reclaimed")
        .description("Orphaned attempts reclaimed through the engine failure path")
        .register(registry);
    this.processingTime = Timer.builder("threadmill.jobs.processing.time")
        .description("Wall-clock time from claim to terminal transition")
        .register(registry);
    this.claimLatency = Timer.builder("threadmill.claim.latency")
        .description("Wall-clock time spent in JobStore claimReady calls")
        .register(registry);

    // Register gauges only after every field their callbacks can reach has
    // been assigned. Registries may read a gauge from an onMeterAdded hook.
    for (var state : JobState.values()) {
      Gauge.builder("threadmill.jobs.count", this, metrics -> metrics.stateCount(state))
          .tag("state", state.name())
          .description("Number of Threadmill jobs in this state")
          .strongReference(true)
          .register(registry);
    }
    Gauge.builder(
            "threadmill.processing.oldest.heartbeat.age",
            this,
            ThreadmillMetrics::oldestProcessingHeartbeatAgeMillis)
        .description("Age in milliseconds of the oldest processing heartbeat")
        .strongReference(true)
        .register(registry);
    Gauge.builder("threadmill.metrics.snapshot.stale", this, ThreadmillMetrics::snapshotStaleGauge)
        .description("One when the latest store-derived gauge refresh failed, otherwise zero")
        .strongReference(true)
        .register(registry);
    Gauge.builder("threadmill.metrics.snapshot.age", this, ThreadmillMetrics::snapshotAgeMillis)
        .description(
            "Milliseconds since the last successful store-derived gauge refresh; -1 before any success")
        .strongReference(true)
        .register(registry);
    Gauge.builder(
            "threadmill.metrics.queue.tags.omitted", this, ThreadmillMetrics::omittedQueueTags)
        .description("Active queues omitted because the configured queue-tag cap was reached")
        .strongReference(true)
        .register(registry);
    refresh();
  }

  /**
   * Return the store decorator that records claim and rejected-write meters.
   *
   * <p>The same returned instance should be passed to every processing node
   * and producer that uses this metrics object. Gauge reads continue to use
   * the backing store directly and are not counted as application operations.
   */
  public JobStore meteredStore() {
    return meteredStore;
  }

  /**
   * Refresh all store-derived gauges immediately.
   *
   * <p>On failure the previous successful snapshot remains visible,
   * {@code threadmill.metrics.snapshot.stale} becomes one, and
   * {@code threadmill.metrics.refresh.errors} increments. Age gauges continue
   * advancing from their last known timestamps rather than dropping to zero.
   */
  public void refresh() {
    refreshLock.lock();
    try {
      refreshAt();
    } finally {
      lastRefreshNanos = System.nanoTime();
      refreshLock.unlock();
    }
  }

  private void refreshAt() {
    Set<String> selectedQueues;
    try {
      var counts = new EnumMap<JobState, Long>(JobState.class);
      counts.putAll(store.countsByState());
      var depths = Map.copyOf(store.queueDepths());
      selectedQueues = selectQueues(depths.keySet());
      var queues = new HashMap<String, QueueSnapshot>();
      for (var queue : selectedQueues) {
        queues.put(queue, new QueueSnapshot(depths.get(queue), store.oldestEnqueuedAt(queue)));
      }
      var refreshed = new StoreSnapshot(
          Map.copyOf(counts),
          Map.copyOf(queues),
          store.oldestProcessingHeartbeat(),
          Instant.now(),
          maxQueueTags == 0 ? 0 : Math.max(0, depths.size() - selectedQueues.size()));
      snapshot.set(refreshed);
      snapshotStale.set(false);
      var failures = consecutiveRefreshFailures.getAndSet(0);
      if (failures > 0) {
        LOG.info("Threadmill metrics store refresh recovered after {} failed attempts", failures);
      }
    } catch (RuntimeException e) {
      snapshotStale.set(true);
      refreshErrors.increment();
      var failures = consecutiveRefreshFailures.incrementAndGet();
      if (failures == 1 || failures % 60 == 0) {
        LOG.warn(
            "Threadmill metrics store refresh has failed {} consecutive times; gauges are stale",
            failures,
            e);
      }
      return;
    }

    try {
      reconcileQueueMeters(selectedQueues);
      consecutiveMeterFailures.set(0);
    } catch (RuntimeException e) {
      meterReconciliationErrors.increment();
      var failures = consecutiveMeterFailures.incrementAndGet();
      if (failures == 1 || failures % 60 == 0) {
        LOG.warn(
            "Threadmill queue-meter reconciliation has failed {} consecutive times; the store snapshot is current",
            failures,
            e);
      }
    }
  }

  private Set<String> selectQueues(Set<String> activeQueues) {
    var selected = new LinkedHashSet<String>();
    queueMeters.keySet().stream()
        .filter(activeQueues::contains)
        .sorted()
        .limit(maxQueueTags)
        .forEach(selected::add);
    if (selected.size() < maxQueueTags) {
      activeQueues.stream()
          .filter(queue -> !selected.contains(queue))
          .sorted()
          .limit(maxQueueTags - selected.size())
          .forEach(selected::add);
    }
    return Set.copyOf(selected);
  }

  private void reconcileQueueMeters(Set<String> selectedQueues) {
    for (var entry : queueMeters.entrySet()) {
      if (!selectedQueues.contains(entry.getKey())
          && queueMeters.remove(entry.getKey(), entry.getValue())) {
        registry.remove(entry.getValue().depth());
        registry.remove(entry.getValue().oldestAge());
      }
    }
    for (var queue : selectedQueues) {
      queueMeters.computeIfAbsent(queue, this::registerQueueMeters);
    }
  }

  private QueueMeters registerQueueMeters(String queue) {
    var depth = Gauge.builder("threadmill.queue.depth", this, metrics -> metrics.queueDepth(queue))
        .tag("queue", queue)
        .description("Number of ENQUEUED Threadmill jobs in this queue")
        .strongReference(true)
        .register(registry);
    var oldestAge = Gauge.builder(
            "threadmill.queue.oldest.enqueued.age", this, metrics -> metrics.queueAgeMillis(queue))
        .tag("queue", queue)
        .description("Age in milliseconds of the oldest ENQUEUED job in this queue")
        .strongReference(true)
        .register(registry);
    return new QueueMeters(depth, oldestAge);
  }

  private void refreshThrottled() {
    if (refreshLock.isHeldByCurrentThread()) {
      // A registry callback can read a gauge while reconcileQueueMeters is
      // still registering it. tryLock() would succeed for the holder, and the
      // re-entered refresh would recurse into the ConcurrentHashMap mapping
      // function computing that queue's meters. The snapshot such a call
      // would rebuild is the one this thread is already producing, so there
      // is nothing to gain by refreshing again.
      return;
    }
    var now = System.nanoTime();
    if (!refreshDue(now, lastRefreshNanos)) {
      return;
    }
    if (!refreshLock.tryLock()) {
      return;
    }
    try {
      if (refreshDue(System.nanoTime(), lastRefreshNanos)) {
        try {
          refreshAt();
        } finally {
          // Start the cooldown when an attempt actually finishes. Slow or
          // failed reads must not provoke an immediate retry pile-up.
          lastRefreshNanos = System.nanoTime();
        }
      }
    } finally {
      refreshLock.unlock();
    }
  }

  private boolean refreshDue(long now, long last) {
    return last == Long.MIN_VALUE || now - last >= refreshIntervalNanos;
  }

  private double stateCount(JobState state) {
    refreshThrottled();
    return snapshot.get().counts().getOrDefault(state, 0L);
  }

  private double queueDepth(String queue) {
    refreshThrottled();
    var current = snapshot.get().queues().get(queue);
    return current == null ? 0d : current.depth();
  }

  private double queueAgeMillis(String queue) {
    refreshThrottled();
    var current = snapshot.get().queues().get(queue);
    return current == null ? 0d : ageMillis(current.oldestEnqueuedAt());
  }

  private double oldestProcessingHeartbeatAgeMillis() {
    refreshThrottled();
    return ageMillis(snapshot.get().oldestProcessingHeartbeat());
  }

  private double snapshotStaleGauge() {
    refreshThrottled();
    return snapshotStale.get() ? 1d : 0d;
  }

  private double snapshotAgeMillis() {
    refreshThrottled();
    var refreshedAt = snapshot.get().refreshedAt();
    return refreshedAt == null ? -1d : ageMillis(Optional.of(refreshedAt));
  }

  private double omittedQueueTags() {
    refreshThrottled();
    return snapshot.get().omittedQueueTags();
  }

  private static double ageMillis(Optional<Instant> timestamp) {
    return timestamp
        .map(at -> (double) Math.max(0L, Duration.between(at, Instant.now()).toMillis()))
        .orElse(0d);
  }

  /**
   * Record an externally-observed claim latency.
   *
   * @deprecated Pass {@link #meteredStore()} to the processing node instead;
   *     calling both paths double-counts claims.
   */
  @Deprecated(since = "0.2.2", forRemoval = false)
  public void recordClaimLatency(Duration duration) {
    recordClaimReadyLatency(duration);
  }

  void recordClaimReadyLatency(Duration duration) {
    claimLatency.record(duration);
  }

  void recordClaimFailure() {
    claimFailures.increment();
  }

  void recordRejectedWrite(String operation) {
    rejectedWriteCounters
        .computeIfAbsent(
            operation,
            name -> Counter.builder("threadmill.store.writes.rejected")
                .tag("operation", name)
                .description("Store write operations rejected with an exception")
                .register(registry))
        .increment();
  }

  private void recordRecurringRun(String origin) {
    var tag =
        switch (origin) {
          case JobExecutionContext.CRON_ORIGIN_SCHEDULE,
              JobExecutionContext.CRON_ORIGIN_NUDGE,
              JobExecutionContext.CRON_ORIGIN_MANUAL -> origin;
          default -> "other";
        };
    recurringRunCounters
        .computeIfAbsent(
            tag,
            value -> Counter.builder("threadmill.jobs.recurring.runs")
                .tag("origin", value)
                .description("Recurring-task instances started, by trigger origin")
                .register(registry))
        .increment();
  }

  /** A JobInterceptor that drives the lifecycle counters and processing timer. */
  public JobInterceptor asInterceptor() {
    return new JobInterceptor() {
      @Override
      public void onProcessingStarting(Job job, JobExecutionContext ctx) {
        inFlightStart.put(job.id().toString(), Instant.now());
        // This hook fires once per attempt, but the meter counts recurring
        // instances so retries cannot distort the trigger-origin ratio.
        if (job.attempts() <= 1) {
          job.metadata()
              .get(JobExecutionContext.CRON_ORIGIN_META)
              .ifPresent(ThreadmillMetrics.this::recordRecurringRun);
        }
      }

      @Override
      public void onProcessingSucceeded(Job job, JobExecutionContext ctx) {
        processedCounter.increment();
        recordElapsed(job);
      }

      @Override
      public void onProcessingFailed(
          Job job, JobExecutionContext ctx, Throwable cause, FailureCause kind) {
        failedCounters
            .computeIfAbsent(
                kind.name(),
                name ->
                    Counter.builder("threadmill.jobs.failed").tag("cause", name).register(registry))
            .increment();
        if (kind == FailureCause.ORPHAN_RECLAIM) {
          orphanReclaims.increment();
        }
        recordElapsed(job);
      }
    };
  }

  private void recordElapsed(Job job) {
    var started = inFlightStart.remove(job.id().toString());
    if (started != null) {
      processingTime.record(Duration.between(started, Instant.now()));
    }
  }

  private record QueueMeters(Gauge depth, Gauge oldestAge) {}

  private record QueueSnapshot(long depth, Optional<Instant> oldestEnqueuedAt) {}

  private record StoreSnapshot(
      Map<JobState, Long> counts,
      Map<String, QueueSnapshot> queues,
      Optional<Instant> oldestProcessingHeartbeat,
      Instant refreshedAt,
      int omittedQueueTags) {

    private static StoreSnapshot empty() {
      return new StoreSnapshot(Map.of(), Map.of(), Optional.empty(), null, 0);
    }
  }
}
