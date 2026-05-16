package com.hemju.threadmill.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.schedule.Scheduler;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
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
                assertThat(registry.counter("threadmill.jobs.processed").count())
                        .isEqualTo(1.0);
                assertThat(registry.counter("threadmill.jobs.failed", "cause", "EXCEPTION")
                                .count())
                        .isGreaterThanOrEqualTo(1.0);
                assertThat(registry.timer("threadmill.jobs.processing.time").count())
                        .isGreaterThanOrEqualTo(2L);
                // State gauges: SUCCEEDED >= 1, FAILED >= 1.
                Double succeededCount = registry.find("threadmill.jobs.count")
                        .tag("state", JobState.SUCCEEDED.name())
                        .gauge()
                        .value();
                Double failedCount = registry.find("threadmill.jobs.count")
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
}
