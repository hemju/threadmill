package com.hemju.threadmill.soak;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.engine.QueueWeights;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.Scheduler;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.store.redis.RedisJobStore;

/**
 * Soak runs against a real Redis 7 via Testcontainers — throughput,
 * recurring no-skip, and an induced container-pause outage that the
 * dispatcher's circuit-breaker must recover from.
 */
@Tag("soak")
class RedisSoakTest {

    @SuppressWarnings("resource")
    private static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379)
            .withCommand("redis-server", "--appendonly", "yes")
            .waitingFor(Wait.forListeningPort());

    private static RedisURI uri;
    private static RedisClient adminClient;
    private static StatefulRedisConnection<String, String> adminConnection;

    private final JsonJobSerializer serializer = new JsonJobSerializer();
    private RedisJobStore store;
    private Scheduler scheduler;
    private ProcessingNode node;

    @BeforeAll
    static void startContainer() {
        REDIS.start();
        uri = RedisURI.create("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
        adminClient = RedisClient.create(uri);
        adminConnection = adminClient.connect();
    }

    @AfterAll
    static void stopContainer() {
        if (adminConnection != null) adminConnection.close();
        if (adminClient != null) adminClient.shutdown();
        if (REDIS.isRunning()) REDIS.stop();
    }

    @BeforeEach
    void freshDb() {
        adminConnection.sync().flushdb();
        store = new RedisJobStore(uri);
        scheduler = new Scheduler(store, serializer);
        SoakWork.CountingHandler.COUNT.set(0);
    }

    @AfterEach
    void tearDown() {
        if (node != null) node.close();
        if (store != null) store.close();
    }

    @Test
    void sustainedThroughputProcessesFiveThousandJobsThroughRealRedis() {
        int total = 5_000;
        for (int i = 0; i < total; i++) {
            scheduler.enqueue(new SoakWork.P(i), SoakWork.CountingHandler.class);
        }
        node = ProcessingNode.builder(store).config(fastConfig()).build();
        long started = System.nanoTime();
        node.start();
        await().atMost(Duration.ofMinutes(5)).until(() -> SoakWork.CountingHandler.COUNT.get() >= total);
        long elapsedMs = (System.nanoTime() - started) / 1_000_000L;
        System.out.printf(
                "Soak Redis throughput: %d jobs in %d ms (%.1f jobs/sec)%n",
                total, elapsedMs, total * 1000.0 / Math.max(1, elapsedMs));
    }

    @Test
    void sustainedContentionOnOneConcurrencyKeyCompletesAgainstRedis() {
        int total = 100;
        for (int i = 0; i < total; i++) {
            var mode = i % 10 == 0 ? ConcurrencyMode.EXCLUSIVE : ConcurrencyMode.SHARED;
            scheduler.enqueue(
                    new SoakWork.P(i), SoakWork.CountingHandler.class, "default", 0, "project:contention", mode);
        }
        node = ProcessingNode.builder(store).config(fastConfig()).build();
        long started = System.nanoTime();
        node.start();
        await().atMost(Duration.ofMinutes(3)).until(() -> SoakWork.CountingHandler.COUNT.get() >= total);
        long elapsedMs = (System.nanoTime() - started) / 1_000_000L;
        System.out.printf(
                "Soak Redis contention: %d jobs in %d ms (%.1f jobs/sec)%n",
                total, elapsedMs, total * 1000.0 / Math.max(1, elapsedMs));
    }

    @Test
    void queueFamilyLaneProcessesBurstyProjectQueuesAgainstRedis() {
        int total = 100;
        for (int i = 0; i < total; i++) {
            scheduler.enqueue(new SoakWork.P(i), SoakWork.CountingHandler.class, "project:" + (i % 10), 0);
        }
        node = ProcessingNode.builder(store)
                .config(fastConfig().toBuilder()
                        .queueFamilyDiscoveryInterval(Duration.ofMillis(50))
                        .queueFamilyRetentionAfterEmpty(Duration.ofSeconds(1))
                        .build())
                .lane("project:*", 8, QueueWeights.uniform())
                .build();
        long started = System.nanoTime();
        node.start();
        await().atMost(Duration.ofMinutes(3)).until(() -> SoakWork.CountingHandler.COUNT.get() >= total);
        long elapsedMs = (System.nanoTime() - started) / 1_000_000L;
        System.out.printf(
                "Soak Redis queue-family: %d jobs in %d ms (%.1f jobs/sec)%n",
                total, elapsedMs, total * 1000.0 / Math.max(1, elapsedMs));
    }

    @Test
    void recurringEveryHundredMillisHasNoSkipsAgainstRedis() {
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
        await().pollDelay(runFor).atMost(runFor.plus(Duration.ofSeconds(3))).until(() -> true);
        long elapsedMs = Duration.between(start, Instant.now()).toMillis();
        long expectedMin = Math.max(10L, (elapsedMs - 500) / 100L / 4);
        int observed = SoakWork.CountingHandler.COUNT.get();
        System.out.printf(
                "Soak Redis recurring: %d runs in %d ms (expected >= %d)%n", observed, elapsedMs, expectedMin);
        assertThat(observed).isGreaterThanOrEqualTo((int) expectedMin);
    }

    @Test
    void clusterPausesAndResumesAcrossAnInducedRedisOutage() throws Exception {
        int total = 500;
        for (int i = 0; i < total; i++) {
            scheduler.enqueue(new SoakWork.P(i), SoakWork.CountingHandler.class);
        }
        node = ProcessingNode.builder(store)
                .config(fastConfig().toBuilder()
                        .maxConsecutiveDispatcherFailures(2)
                        .storeOutagePollInterval(Duration.ofMillis(200))
                        .build())
                .build();
        node.start();
        await().atMost(Duration.ofSeconds(20)).until(() -> SoakWork.CountingHandler.COUNT.get() >= 50);

        String containerId = REDIS.getContainerId();
        var docker = DockerClientFactory.instance().client();
        docker.pauseContainerCmd(containerId).exec();
        int countAtPause = SoakWork.CountingHandler.COUNT.get();
        System.out.printf("Soak Redis outage: paused container at %d jobs done%n", countAtPause);
        try {
            Thread.sleep(2_000);
        } finally {
            docker.unpauseContainerCmd(containerId).exec();
        }
        int countAfterPause = SoakWork.CountingHandler.COUNT.get();
        assertThat(countAfterPause - countAtPause)
                .as("no large progress while the store was paused")
                .isLessThan(64);

        await().atMost(Duration.ofMinutes(2)).until(() -> SoakWork.CountingHandler.COUNT.get() >= total);
        System.out.printf("Soak Redis outage: resumed; %d total processed%n", SoakWork.CountingHandler.COUNT.get());
    }

    private static ProcessingNodeConfig fastConfig() {
        return ProcessingNodeConfig.builder()
                .workerCount(8)
                .pollInterval(Duration.ofMillis(20))
                .claimHeartbeat(Duration.ofMillis(200))
                .heartbeatTimeout(Duration.ofSeconds(30))
                .jobTimeout(Duration.ofSeconds(15))
                .claimBatchSize(32)
                .defaultMaxAttempts(1)
                .retryInitialBackoff(Duration.ofMillis(50))
                .storeOutagePollInterval(Duration.ofMillis(200))
                .build();
    }
}
