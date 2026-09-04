package com.hemju.threadmill.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobEngineFatalException;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.schedule.Scheduler;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.memory.InMemoryJobStore;
import com.hemju.threadmill.test.ForwardingJobStore;

class ThreadmillMetricsTest {

  private static final Duration TEST_REFRESH_INTERVAL = Duration.ofMillis(5);

  public static final class P implements JobPayload {
    public String s;

    public P() {}

    public P(String s) {
      this.s = s;
    }
  }

  public static final class OkHandler implements JobHandler<P> {
    public static final AtomicInteger COUNT = new AtomicInteger();

    @Override
    public void run(P p, JobExecutionContext c) {
      COUNT.incrementAndGet();
    }
  }

  public static final class FailHandler implements JobHandler<P> {
    @Override
    public void run(P p, JobExecutionContext c) {
      throw new IllegalStateException("boom");
    }
  }

  @Test
  void recordsCountsTimersAndFailureCountersThroughTheMeteredStore() {
    OkHandler.COUNT.set(0);
    var backingStore = new InMemoryJobStore();
    var registry = new SimpleMeterRegistry();
    var metrics = new ThreadmillMetrics(registry, backingStore);
    var store = metrics.meteredStore();
    var scheduler = new Scheduler(store, new JsonJobSerializer());

    var node = ProcessingNode.builder(store)
        .config(ProcessingNodeConfig.builder()
            .workerCount(2)
            .pollInterval(Duration.ofMillis(30))
            .claimHeartbeat(Duration.ofMillis(60))
            .heartbeatTimeout(Duration.ofSeconds(2))
            .jobTimeout(Duration.ofSeconds(2))
            .defaultMaxAttempts(1)
            .retryInitialBackoff(Duration.ofMillis(50))
            .storeOutagePollInterval(Duration.ofMillis(100))
            .build())
        .interceptor(metrics.asInterceptor())
        .build();
    try {
      node.start();
      scheduler.enqueue(new P("ok"), OkHandler.class);
      scheduler.enqueue(new P("oops"), FailHandler.class);

      await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
        metrics.refresh();
        assertThat(registry.counter("threadmill.jobs.processed").count()).isEqualTo(1.0);
        assertThat(
                registry.counter("threadmill.jobs.failed", "cause", "EXCEPTION").count())
            .isGreaterThanOrEqualTo(1.0);
        assertThat(registry.timer("threadmill.jobs.processing.time").count())
            .isGreaterThanOrEqualTo(2L);
        assertThat(registry.timer("threadmill.claim.latency").count()).isPositive();
        assertThat(stateGauge(registry, JobState.SUCCEEDED)).isGreaterThanOrEqualTo(1.0);
        assertThat(stateGauge(registry, JobState.FAILED)).isGreaterThanOrEqualTo(1.0);
      });
    } finally {
      node.close();
    }
  }

  @Test
  void pullRefreshTracksStalledProcessingWithoutACompletion() {
    var store = new InMemoryJobStore();
    var registry = new SimpleMeterRegistry();
    new ThreadmillMetrics(registry, store, TEST_REFRESH_INTERVAL, 10);
    var job = newJob("stalled");
    store.insert(job);
    store.claimReady(NodeId.newId(), "stalled", 1, Instant.now().minusSeconds(2));

    await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
      assertThat(stateGauge(registry, JobState.PROCESSING)).isEqualTo(1d);
      assertThat(
              registry.get("threadmill.processing.oldest.heartbeat.age").gauge().value())
          .isGreaterThanOrEqualTo(1_000d);
    });
  }

  @Test
  void pullRefreshReportsPausedQueueDepthWithoutACompletion() {
    var store = new InMemoryJobStore();
    var registry = new SimpleMeterRegistry();
    new ThreadmillMetrics(registry, store, TEST_REFRESH_INTERVAL, 10);
    store.pauseQueue("paused", "maintenance");
    store.insert(newJob("paused"));

    await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
      assertThat(stateGauge(registry, JobState.ENQUEUED)).isEqualTo(1d);
      assertThat(registry
              .get("threadmill.queue.depth")
              .tag("queue", "paused")
              .gauge()
              .value())
          .isEqualTo(1d);
    });
  }

  @Test
  void pullRefreshRegistersANewlyAppearingQueue() {
    var store = new InMemoryJobStore();
    var registry = new SimpleMeterRegistry();
    new ThreadmillMetrics(registry, store, TEST_REFRESH_INTERVAL, 10);
    store.insert(newJob("new-queue"));

    await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
      assertThat(stateGauge(registry, JobState.ENQUEUED)).isEqualTo(1d);
      assertThat(
              registry.find("threadmill.queue.depth").tag("queue", "new-queue").gauge())
          .isNotNull();
    });
  }

  @Test
  void failedPullRefreshRetainsTheLastSnapshotAndMarksItStaleUntilRecovery() {
    var backingStore = new InMemoryJobStore();
    backingStore.insert(newJob("q"));
    var failingStore = new SnapshotFailureStore(backingStore);
    var registry = new SimpleMeterRegistry();
    new ThreadmillMetrics(registry, failingStore, TEST_REFRESH_INTERVAL, 10);
    backingStore.insert(newJob("q"));
    failingStore.failRefresh.set(true);

    await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
      assertThat(stateGauge(registry, JobState.ENQUEUED))
          .as("last successful snapshot stays visible")
          .isEqualTo(1d);
      assertThat(registry.get("threadmill.metrics.snapshot.stale").gauge().value())
          .isEqualTo(1d);
      assertThat(registry.counter("threadmill.metrics.refresh.errors").count()).isPositive();
      assertThat(registry.get("threadmill.metrics.snapshot.age").gauge().value())
          .isNotNegative();
    });

    failingStore.failRefresh.set(false);
    await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
      assertThat(stateGauge(registry, JobState.ENQUEUED)).isEqualTo(2d);
      assertThat(registry.get("threadmill.metrics.snapshot.stale").gauge().value())
          .isZero();
    });
  }

  @Test
  void failedInitialRefreshReportsUnknownSnapshotAge() {
    var failingStore = new SnapshotFailureStore(new InMemoryJobStore());
    failingStore.failRefresh.set(true);
    var registry = new SimpleMeterRegistry();

    new ThreadmillMetrics(registry, failingStore, TEST_REFRESH_INTERVAL, 10);

    assertThat(registry.get("threadmill.metrics.snapshot.age").gauge().value()).isEqualTo(-1d);
    assertThat(registry.get("threadmill.metrics.snapshot.stale").gauge().value())
        .isEqualTo(1d);
  }

  @Test
  void gaugeCallbacksCanRunDuringRegistrationAfterTheirDependenciesAreInitialized() {
    var failingStore = new SnapshotFailureStore(new InMemoryJobStore());
    failingStore.failRefresh.set(true);
    var registry = new SimpleMeterRegistry();
    registry.config().onMeterAdded(meter -> {
      if (meter instanceof Gauge gauge && meter.getId().getName().equals("threadmill.jobs.count")) {
        gauge.value();
      }
    });

    new ThreadmillMetrics(registry, failingStore, TEST_REFRESH_INTERVAL, 10);

    assertThat(stateGauge(registry, JobState.ENQUEUED)).isZero();
    assertThat(registry.counter("threadmill.metrics.refresh.errors").count()).isPositive();
  }

  @Test
  void queueGaugeCallbackDuringRegistrationDoesNotReEnterTheRefresh() {
    var store = new InMemoryJobStore();
    store.insert(newJob("q1"));
    var registry = new SimpleMeterRegistry();
    // A registry that reads a meter from onMeterAdded observes the queue gauge
    // while reconcileQueueMeters is still registering it. Re-entering the
    // refresh from there would recurse into the ConcurrentHashMap mapping
    // function that is computing that very queue's meters.
    registry.config().onMeterAdded(meter -> {
      if (meter instanceof Gauge gauge
          && meter.getId().getName().equals("threadmill.queue.depth")) {
        gauge.value();
      }
    });

    new ThreadmillMetrics(registry, store, Duration.ofNanos(1), 10);

    assertThat(registry.get("threadmill.queue.depth").tag("queue", "q1").gauge().value())
        .isEqualTo(1d);
    assertThat(registry.counter("threadmill.metrics.queue.meter.errors").count())
        .as("a healthy store and registry must not report a meter-reconciliation failure")
        .isZero();
    assertThat(registry.counter("threadmill.metrics.refresh.errors").count()).isZero();
    assertThat(registry.get("threadmill.metrics.snapshot.stale").gauge().value())
        .isZero();
  }

  @Test
  void constructorRejectsInvalidRefreshAndQueueBounds() {
    var registry = new SimpleMeterRegistry();
    var store = new InMemoryJobStore();

    assertThatThrownBy(() -> new ThreadmillMetrics(registry, store, Duration.ZERO, 10))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("refreshInterval must be positive");
    assertThatThrownBy(() -> new ThreadmillMetrics(registry, store, Duration.ofSeconds(1), -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("maxQueueTags must not be negative");
  }

  @Test
  void queueTagCardinalityIsCappedAndAReleasedSlotAdmitsANewQueue() {
    var store = new InMemoryJobStore();
    var registry = new SimpleMeterRegistry();
    var metrics = new ThreadmillMetrics(registry, store, Duration.ofHours(1), 2);
    store.insert(newJob("q1"));
    store.insert(newJob("q2"));
    store.insert(newJob("q3"));

    metrics.refresh();

    assertThat(registry.find("threadmill.queue.depth").gauges()).hasSize(2);
    assertThat(registry.find("threadmill.queue.depth").tag("queue", "q1").gauge())
        .isNotNull();
    assertThat(registry.find("threadmill.queue.depth").tag("queue", "q2").gauge())
        .isNotNull();
    assertThat(registry.find("threadmill.queue.depth").tag("queue", "q3").gauge())
        .isNull();
    assertThat(registry.get("threadmill.metrics.queue.tags.omitted").gauge().value())
        .isEqualTo(1d);

    finishQueue(store, "q1");
    metrics.refresh();

    assertThat(registry.find("threadmill.queue.depth").gauges()).hasSize(2);
    assertThat(registry.find("threadmill.queue.depth").tag("queue", "q1").gauge())
        .isNull();
    assertThat(registry.find("threadmill.queue.depth").tag("queue", "q3").gauge())
        .isNotNull();
    assertThat(registry.get("threadmill.metrics.queue.tags.omitted").gauge().value())
        .isZero();
  }

  @Test
  void zeroQueueTagCapDisablesQueueMetersWithoutReportingOverflow() {
    var store = new InMemoryJobStore();
    var registry = new SimpleMeterRegistry();
    var metrics = new ThreadmillMetrics(registry, store, Duration.ofHours(1), 0);
    store.insert(newJob("q1"));
    store.insert(newJob("q2"));

    metrics.refresh();

    assertThat(registry.find("threadmill.queue.depth").gauges()).isEmpty();
    assertThat(registry.get("threadmill.metrics.queue.tags.omitted").gauge().value())
        .isZero();
  }

  @Test
  void concurrentGaugeReadsUseTheCachedSnapshotWhileARefreshIsInFlight() throws Exception {
    var store = new BlockingSnapshotStore(new InMemoryJobStore());
    var registry = new SimpleMeterRegistry();
    new ThreadmillMetrics(registry, store, Duration.ofNanos(1), 10);
    store.blockRefresh.set(true);

    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var refreshingRead = executor.submit(() -> stateGauge(registry, JobState.ENQUEUED));
      assertThat(store.refreshEntered.await(1, TimeUnit.SECONDS)).isTrue();

      var cachedRead = executor.submit(() -> stateGauge(registry, JobState.ENQUEUED));
      assertThat(cachedRead.get(1, TimeUnit.SECONDS)).isZero();

      store.releaseRefresh.countDown();
      assertThat(refreshingRead.get(1, TimeUnit.SECONDS)).isZero();
    } finally {
      store.releaseRefresh.countDown();
    }
    assertThat(store.countsCalls.get()).isEqualTo(2);
  }

  @Test
  void forcedRefreshRunsAfterAnInFlightPullAndObservesLaterWrites() throws Exception {
    var backingStore = new InMemoryJobStore();
    var store = new BlockingSnapshotStore(backingStore);
    var registry = new SimpleMeterRegistry();
    var metrics = new ThreadmillMetrics(registry, store, Duration.ofNanos(1), 10);
    store.blockRefresh.set(true);

    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var pullRefresh = executor.submit(() -> stateGauge(registry, JobState.ENQUEUED));
      assertThat(store.refreshEntered.await(1, TimeUnit.SECONDS)).isTrue();
      backingStore.insert(newJob("q"));

      var forcedRefresh = executor.submit(metrics::refresh);
      assertThat(forcedRefresh.isDone()).isFalse();
      store.releaseRefresh.countDown();

      assertThat(pullRefresh.get(1, TimeUnit.SECONDS)).isZero();
      forcedRefresh.get(1, TimeUnit.SECONDS);
      assertThat(store.countsCalls.get()).isEqualTo(3);
      assertThat(stateGauge(registry, JobState.ENQUEUED)).isEqualTo(1d);
    } finally {
      store.releaseRefresh.countDown();
    }
  }

  @Test
  void queueMeterReconciliationFailureDoesNotMarkTheStoreSnapshotStale() {
    var store = new InMemoryJobStore();
    store.insert(newJob("q"));
    var registry = new SimpleMeterRegistry();
    registry.counter("threadmill.queue.depth", "queue", "q");
    var metrics = new ThreadmillMetrics(registry, store, Duration.ofHours(1), 10);

    metrics.refresh();

    assertThat(stateGauge(registry, JobState.ENQUEUED)).isEqualTo(1d);
    assertThat(registry.get("threadmill.metrics.snapshot.stale").gauge().value())
        .isZero();
    assertThat(registry.counter("threadmill.metrics.refresh.errors").count()).isZero();
    assertThat(registry.counter("threadmill.metrics.queue.meter.errors").count())
        .isPositive();
  }

  @Test
  void meteredStoreRecordsClaimFailuresAndRejectedWritesAtTheirBoundaries() {
    var backingStore = new InMemoryJobStore();
    var failingStore = new BoundaryFailureStore(backingStore);
    var registry = new SimpleMeterRegistry();
    var metrics = new ThreadmillMetrics(registry, failingStore);
    var store = metrics.meteredStore();

    store.claimReady(NodeId.newId(), "q", 1, Instant.now());
    failingStore.failClaim.set(true);
    assertThatThrownBy(() -> store.claimReady(NodeId.newId(), "q", 1, Instant.now()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("claim unavailable");
    failingStore.failInsert.set(true);
    assertThatThrownBy(() -> store.insert(newJob("q")))
        .isInstanceOf(JobEngineFatalException.class)
        .hasMessage("write rejected");

    assertThat(registry.timer("threadmill.claim.latency").count()).isEqualTo(2L);
    assertThat(registry.timer("threadmill.claim.latency").getId().getTags()).isEmpty();
    assertThat(registry.counter("threadmill.claim.failures").count()).isEqualTo(1d);
    assertThat(registry.counter("threadmill.claim.failures").getId().getTags()).isEmpty();
    assertThat(registry
            .counter("threadmill.store.writes.rejected", "operation", "insert")
            .count())
        .isEqualTo(1d);
  }

  @Test
  void contractualWriteRejectionsDoNotReportAStoreHealthFailure() {
    var registry = new SimpleMeterRegistry();
    var metrics = new ThreadmillMetrics(registry, new InMemoryJobStore());
    var store = metrics.meteredStore();
    var job = newJob("q");
    store.insert(job);

    assertThatThrownBy(() -> store.insert(job)).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> store.insertAll(List.of(job)))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(
            () -> store.enqueueIfAbsent(job, "dedup", Duration.ofMinutes(1), Instant.now()))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> store.saveAtomic(job, job.version() + 1))
        .isInstanceOf(StaleJobException.class);

    assertThat(registry.find("threadmill.store.writes.rejected").counters()).isEmpty();
  }

  @Test
  void orphanReclaimHasADedicatedCounterAtThePersistedFailureBoundary() {
    var backingStore = new InMemoryJobStore();
    var registry = new SimpleMeterRegistry();
    var metrics = new ThreadmillMetrics(registry, backingStore);
    var orphan = newJob("orphan");
    backingStore.insert(orphan);
    backingStore.claimReady(
        NodeId.newId(), "orphan", 1, Instant.now().minus(Duration.ofSeconds(2)));
    var node = ProcessingNode.builder(metrics.meteredStore())
        .config(ProcessingNodeConfig.builder()
            .workerCount(1)
            .pollInterval(Duration.ofMillis(20))
            .maintenancePollInterval(Duration.ofMillis(10))
            .claimHeartbeat(Duration.ofMillis(40))
            .heartbeatTimeout(Duration.ofMillis(100))
            .defaultMaxAttempts(1)
            .build())
        .interceptor(metrics.asInterceptor())
        .build();

    try {
      node.start();
      await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
        assertThat(registry.counter("threadmill.jobs.orphan.reclaimed").count()).isEqualTo(1d);
        assertThat(registry
                .counter("threadmill.jobs.failed", "cause", "ORPHAN_RECLAIM")
                .count())
            .isEqualTo(1d);
        assertThat(backingStore.findById(orphan.id()).orElseThrow().currentState())
            .isEqualTo(JobState.FAILED);
      });
    } finally {
      node.close();
    }
  }

  @Test
  void deadQueueMetersAreEvicted() {
    var store = new InMemoryJobStore();
    var registry = new SimpleMeterRegistry();
    var metrics = new ThreadmillMetrics(registry, store);

    store.insert(newJob("q1"));
    metrics.refresh();
    assertThat(registry.find("threadmill.queue.depth").tag("queue", "q1").gauge())
        .isNotNull();

    finishQueue(store, "q1");
    metrics.refresh();

    assertThat(registry.find("threadmill.queue.depth").tag("queue", "q1").gauge())
        .isNull();
    assertThat(registry
            .find("threadmill.queue.oldest.enqueued.age")
            .tag("queue", "q1")
            .gauge())
        .isNull();
  }

  @Test
  void jobCompletionsDoNotPerformStoreGaugeRefreshes() {
    var countingStore = new CountingStore(new InMemoryJobStore());
    var metrics = new ThreadmillMetrics(new SimpleMeterRegistry(), countingStore);
    var interceptor = metrics.asInterceptor();
    var job = newJob("default");

    for (int i = 0; i < 5; i++) {
      interceptor.onProcessingSucceeded(job, null);
    }

    assertThat(countingStore.countsCalls.get()).isEqualTo(1);
  }

  @Test
  void recurringRunsCounterTagsTheTriggerOriginWithBoundedCardinality() {
    var registry = new SimpleMeterRegistry();
    var metrics = new ThreadmillMetrics(registry, new InMemoryJobStore());
    var interceptor = metrics.asInterceptor();

    interceptor.onProcessingStarting(jobWithOrigin(JobExecutionContext.CRON_ORIGIN_SCHEDULE), null);
    interceptor.onProcessingStarting(jobWithOrigin(JobExecutionContext.CRON_ORIGIN_NUDGE), null);
    interceptor.onProcessingStarting(jobWithOrigin(JobExecutionContext.CRON_ORIGIN_NUDGE), null);
    interceptor.onProcessingStarting(jobWithOrigin(JobExecutionContext.CRON_ORIGIN_MANUAL), null);
    interceptor.onProcessingStarting(jobWithOrigin("attacker-controlled"), null);
    interceptor.onProcessingStarting(newJob("default"), null);

    assertThat(recurringRuns(registry, JobExecutionContext.CRON_ORIGIN_NUDGE)).isEqualTo(2.0);
    assertThat(recurringRuns(registry, JobExecutionContext.CRON_ORIGIN_SCHEDULE))
        .isEqualTo(1.0);
    assertThat(recurringRuns(registry, JobExecutionContext.CRON_ORIGIN_MANUAL)).isEqualTo(1.0);
    assertThat(recurringRuns(registry, "other")).isEqualTo(1.0);
    assertThat(registry.find("threadmill.jobs.recurring.runs").counters()).hasSize(4);
  }

  @Test
  void recurringRunsCounterCountsInstancesNotRetryAttempts() {
    var registry = new SimpleMeterRegistry();
    var interceptor = new ThreadmillMetrics(registry, new InMemoryJobStore()).asInterceptor();

    interceptor.onProcessingStarting(nudgedJobOnAttempt(1), null);
    for (int attempt = 2; attempt <= 5; attempt++) {
      interceptor.onProcessingStarting(nudgedJobOnAttempt(attempt), null);
    }

    assertThat(recurringRuns(registry, JobExecutionContext.CRON_ORIGIN_NUDGE))
        .as("one instance that was retried four times is still one instance")
        .isEqualTo(1.0);
  }

  private static Job newJob(String queue) {
    return Job.builder()
        .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
        .queue(queue)
        .build();
  }

  private static void finishQueue(InMemoryJobStore store, String queue) {
    var claimed = store.claimReady(NodeId.newId(), queue, 1, Instant.now()).getFirst();
    claimed.transitionTo(JobState.SUCCEEDED, Instant.now());
    claimed.clearOwner();
    store.saveAtomic(claimed, claimed.version());
  }

  private static double stateGauge(SimpleMeterRegistry registry, JobState state) {
    return registry
        .get("threadmill.jobs.count")
        .tag("state", state.name())
        .gauge()
        .value();
  }

  private static Job nudgedJobOnAttempt(int attempt) {
    return Job.builder()
        .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
        .metadata(JobExecutionContext.CRON_ORIGIN_META, JobExecutionContext.CRON_ORIGIN_NUDGE)
        .attempts(attempt)
        .build();
  }

  private static Job jobWithOrigin(String origin) {
    return Job.builder()
        .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
        .metadata(JobExecutionContext.CRON_ORIGIN_META, origin)
        .build();
  }

  private static double recurringRuns(SimpleMeterRegistry registry, String origin) {
    return registry
        .get("threadmill.jobs.recurring.runs")
        .tag("origin", origin)
        .counter()
        .count();
  }

  private static final class CountingStore extends ForwardingJobStore {
    private final AtomicInteger countsCalls = new AtomicInteger();

    private CountingStore(JobStore delegate) {
      super(delegate);
    }

    @Override
    public Map<JobState, Long> countsByState() {
      countsCalls.incrementAndGet();
      return super.countsByState();
    }
  }

  private static final class SnapshotFailureStore extends ForwardingJobStore {
    private final AtomicBoolean failRefresh = new AtomicBoolean();

    private SnapshotFailureStore(JobStore delegate) {
      super(delegate);
    }

    @Override
    public Map<JobState, Long> countsByState() {
      if (failRefresh.get()) {
        throw new IllegalStateException("store unavailable");
      }
      return super.countsByState();
    }
  }

  private static final class BoundaryFailureStore extends ForwardingJobStore {
    private final AtomicBoolean failClaim = new AtomicBoolean();
    private final AtomicBoolean failInsert = new AtomicBoolean();

    private BoundaryFailureStore(JobStore delegate) {
      super(delegate);
    }

    @Override
    public List<Job> claimReady(NodeId nodeId, String queue, int max, Instant heartbeatAt) {
      if (failClaim.get()) {
        throw new IllegalStateException("claim unavailable");
      }
      return super.claimReady(nodeId, queue, max, heartbeatAt);
    }

    @Override
    public void insert(Job job) {
      if (failInsert.get()) {
        throw new JobEngineFatalException("write rejected");
      }
      super.insert(job);
    }
  }

  private static final class BlockingSnapshotStore extends ForwardingJobStore {
    private final AtomicBoolean blockRefresh = new AtomicBoolean();
    private final AtomicInteger countsCalls = new AtomicInteger();
    private final CountDownLatch refreshEntered = new CountDownLatch(1);
    private final CountDownLatch releaseRefresh = new CountDownLatch(1);

    private BlockingSnapshotStore(JobStore delegate) {
      super(delegate);
    }

    @Override
    public Map<JobState, Long> countsByState() {
      countsCalls.incrementAndGet();
      var counts = super.countsByState();
      if (blockRefresh.get()) {
        refreshEntered.countDown();
        try {
          if (!releaseRefresh.await(1, TimeUnit.SECONDS)) {
            throw new IllegalStateException("timed out waiting to release metrics refresh");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(
              "interrupted while waiting to release metrics refresh", e);
        }
      }
      return counts;
    }
  }
}
