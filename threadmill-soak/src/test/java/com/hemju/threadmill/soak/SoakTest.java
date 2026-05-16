package com.hemju.threadmill.soak;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.engine.QueueWeights;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.Scheduler;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

/**
 * In-memory soak runs. Tagged "soak" so the regular {@code check} task
 * excludes them; run explicitly with
 * {@code ./gradlew :threadmill-soak:soakRegression}.
 */
@Tag("soak")
class SoakTest {

    private InMemoryJobStore store;
    private Scheduler scheduler;
    private ProcessingNode node;
    private final JsonJobSerializer serializer = new JsonJobSerializer();

    @BeforeEach
    void setUp() {
        store = new InMemoryJobStore();
        scheduler = new Scheduler(store, serializer);
        SoakWork.CountingHandler.COUNT.set(0);
    }

    @AfterEach
    void tearDown() {
        if (node != null) node.close();
    }

    @Test
    void sustainedThroughputProcessesTenThousandJobs() {
        int total = 10_000;
        for (int i = 0; i < total; i++) {
            scheduler.enqueue(new SoakWork.P(i), SoakWork.CountingHandler.class);
        }
        node = ProcessingNode.builder(store).config(fastConfig()).build();
        long started = System.nanoTime();
        node.start();
        await().atMost(Duration.ofSeconds(60)).until(() -> SoakWork.CountingHandler.COUNT.get() >= total);
        long elapsedMs = (System.nanoTime() - started) / 1_000_000L;
        System.out.printf(
                "Soak in-memory: %d jobs in %d ms (%.1f jobs/sec)%n",
                total, elapsedMs, total * 1000.0 / Math.max(1, elapsedMs));
    }

    @Test
    void sustainedContentionOnOneConcurrencyKeyCompletes() {
        int total = 100;
        for (int i = 0; i < total; i++) {
            var mode = i % 10 == 0 ? ConcurrencyMode.EXCLUSIVE : ConcurrencyMode.SHARED;
            scheduler.enqueue(
                    new SoakWork.P(i), SoakWork.CountingHandler.class, "default", 0, "project:contention", mode);
        }
        node = ProcessingNode.builder(store).config(fastConfig()).build();
        long started = System.nanoTime();
        node.start();
        await().atMost(Duration.ofSeconds(60)).until(() -> SoakWork.CountingHandler.COUNT.get() >= total);
        long elapsedMs = (System.nanoTime() - started) / 1_000_000L;
        System.out.printf(
                "Soak in-memory contention: %d jobs in %d ms (%.1f jobs/sec)%n",
                total, elapsedMs, total * 1000.0 / Math.max(1, elapsedMs));
    }

    @Test
    void queueFamilyLaneProcessesBurstyProjectQueues() {
        int total = 1_000;
        for (int i = 0; i < total; i++) {
            scheduler.enqueue(new SoakWork.P(i), SoakWork.CountingHandler.class, "project:" + (i % 10), 0);
        }
        node = ProcessingNode.builder(store)
                .config(fastConfig().toBuilder()
                        .queueFamilyDiscoveryInterval(Duration.ofMillis(50))
                        .queueFamilyRetentionAfterEmpty(Duration.ofSeconds(1))
                        .build())
                .lane("project:*", 16, QueueWeights.uniform())
                .build();
        long started = System.nanoTime();
        node.start();
        await().atMost(Duration.ofSeconds(60)).until(() -> SoakWork.CountingHandler.COUNT.get() >= total);
        long elapsedMs = (System.nanoTime() - started) / 1_000_000L;
        System.out.printf(
                "Soak in-memory queue-family: %d jobs in %d ms (%.1f jobs/sec)%n",
                total, elapsedMs, total * 1000.0 / Math.max(1, elapsedMs));
    }

    @Test
    void recurringEverySecondHasNoSkipsOverASimulatedLongPeriod() {
        scheduler.defineIntervalTask(
                "ping",
                Duration.ofMillis(100),
                new SoakWork.P(0),
                SoakWork.CountingHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.DROP);
        node = ProcessingNode.builder(store).config(fastConfig()).build();

        var start = Instant.now();
        node.start();
        Duration runFor = Duration.ofSeconds(5);
        await().pollDelay(runFor).atMost(runFor.plus(Duration.ofSeconds(2))).until(() -> true);
        long elapsedMs = Duration.between(start, Instant.now()).toMillis();
        long expectedMin = (long) ((elapsedMs - 200) / 100.0 * 0.6);
        int observed = SoakWork.CountingHandler.COUNT.get();
        System.out.printf(
                "Soak in-memory recurring: %d runs in %d ms (expected >= %d)%n", observed, elapsedMs, expectedMin);
        assertThat(observed).isGreaterThanOrEqualTo((int) expectedMin);
    }

    private static ProcessingNodeConfig fastConfig() {
        return ProcessingNodeConfig.builder()
                .workerCount(16)
                .pollInterval(Duration.ofMillis(10))
                .claimHeartbeat(Duration.ofMillis(100))
                .heartbeatTimeout(Duration.ofSeconds(5))
                .jobTimeout(Duration.ofSeconds(10))
                .claimBatchSize(64)
                .defaultMaxAttempts(1)
                .retryInitialBackoff(Duration.ofMillis(50))
                .storeOutagePollInterval(Duration.ofMillis(100))
                .build();
    }
}
