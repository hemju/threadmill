package com.hemju.threadmill.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.schedule.Scheduler;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

class ThreadmillMetricsTest {

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
  void recordsCountsTimerAndFailureCounter() {
    OkHandler.COUNT.set(0);
    var store = new InMemoryJobStore();
    var registry = new SimpleMeterRegistry();
    var metrics = new ThreadmillMetrics(registry, store);
    var scheduler = new Scheduler(store, new JsonJobSerializer());

    ProcessingNode node = ProcessingNode.builder(store)
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
        // The per-completion refresh is coalesced (1s TTL); force a fresh
        // read so the gauges reflect the final state within the window.
        metrics.refresh();
        assertThat(registry.counter("threadmill.jobs.processed").count()).isEqualTo(1.0);
        assertThat(
                registry.counter("threadmill.jobs.failed", "cause", "EXCEPTION").count())
            .isGreaterThanOrEqualTo(1.0);
        assertThat(registry.timer("threadmill.jobs.processing.time").count())
            .isGreaterThanOrEqualTo(2L);
        // State gauges: SUCCEEDED >= 1, FAILED >= 1.
        Double succeededCount = registry
            .find("threadmill.jobs.count")
            .tag("state", JobState.SUCCEEDED.name())
            .gauge()
            .value();
        Double failedCount = registry
            .find("threadmill.jobs.count")
            .tag("state", JobState.FAILED.name())
            .gauge()
            .value();
        assertThat(succeededCount).isGreaterThanOrEqualTo(1.0);
        assertThat(failedCount).isGreaterThanOrEqualTo(1.0);
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

    var job = Job.builder()
        .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
        .queue("q1")
        .build();
    store.insert(job);
    metrics.refresh();
    assertThat(registry.find("threadmill.queue.depth").tag("queue", "q1").gauge())
        .isNotNull();

    // Drain q1 so it has no ENQUEUED work and drops out of queueDepths().
    var node = NodeId.newId();
    Job claimed = store.claimReady(node, "q1", 1, Instant.now()).get(0);
    claimed.transitionTo(JobState.SUCCEEDED, Instant.now());
    claimed.clearOwner();
    store.saveAtomic(claimed, claimed.version());

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
  void refreshIsCoalescedNotRunOnEveryCompletion() {
    var real = new InMemoryJobStore();
    var countsCalls = new AtomicInteger();
    var counting =
        (com.hemju.threadmill.core.store.JobStore) java.lang.reflect.Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {com.hemju.threadmill.core.store.JobStore.class},
            (proxy, method, args) -> {
              if (method.getName().equals("countsByState")) {
                countsCalls.incrementAndGet();
              }
              try {
                return method.invoke(real, args);
              } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
              }
            });
    var metrics =
        new ThreadmillMetrics(new SimpleMeterRegistry(), counting); // constructor refresh -> 1
    var interceptor = metrics.asInterceptor();
    var job = Job.builder()
        .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
        .build();

    for (int i = 0; i < 5; i++) {
      interceptor.onProcessingSucceeded(job, null);
    }

    // The 1s TTL coalesces: constructor (1) + at most one throttled refresh.
    assertThat(countsCalls.get()).isLessThanOrEqualTo(2);
  }

  @Test
  void recurringRunsCounterTagsTheTriggerOriginWithBoundedCardinality() {
    // Issue #108 observability: schedule-fired, nudged, and operator
    // force-triggered instances must be distinguishable in metrics.
    var registry = new SimpleMeterRegistry();
    var metrics = new ThreadmillMetrics(registry, new InMemoryJobStore());
    var interceptor = metrics.asInterceptor();

    interceptor.onProcessingStarting(jobWithOrigin(JobExecutionContext.CRON_ORIGIN_SCHEDULE), null);
    interceptor.onProcessingStarting(jobWithOrigin(JobExecutionContext.CRON_ORIGIN_NUDGE), null);
    interceptor.onProcessingStarting(jobWithOrigin(JobExecutionContext.CRON_ORIGIN_NUDGE), null);
    interceptor.onProcessingStarting(jobWithOrigin(JobExecutionContext.CRON_ORIGIN_MANUAL), null);
    // Arbitrary metadata values are clamped so user-controlled strings
    // can never explode the tag cardinality.
    interceptor.onProcessingStarting(jobWithOrigin("attacker-controlled"), null);
    // Non-recurring jobs record nothing.
    interceptor.onProcessingStarting(
        Job.builder()
            .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
            .build(),
        null);

    assertThat(recurringRuns(registry, JobExecutionContext.CRON_ORIGIN_NUDGE)).isEqualTo(2.0);
    assertThat(recurringRuns(registry, JobExecutionContext.CRON_ORIGIN_SCHEDULE))
        .isEqualTo(1.0);
    assertThat(recurringRuns(registry, JobExecutionContext.CRON_ORIGIN_MANUAL)).isEqualTo(1.0);
    assertThat(recurringRuns(registry, "other")).isEqualTo(1.0);
    assertThat(registry.find("threadmill.jobs.recurring.runs").counters()).hasSize(4);
  }

  @Test
  void recurringRunsCounterCountsInstancesNotRetryAttempts() {
    // The hook fires per attempt, but operators read the
    // nudge-versus-schedule ratio off this meter — a retry-storming task
    // would inflate both origins and make that reading meaningless.
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
}
