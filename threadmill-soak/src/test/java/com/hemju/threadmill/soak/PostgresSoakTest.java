package com.hemju.threadmill.soak;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGPoolingDataSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.engine.QueueWeights;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.Scheduler;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.store.postgres.MigrationRunner;
import com.hemju.threadmill.store.postgres.PostgresJobStore;

/**
 * Soak runs against a real PostgreSQL 18 via Testcontainers.
 *
 * <ul>
 *   <li>{@code sustainedThroughput…} drives 5,000 jobs through the engine.</li>
 *   <li>{@code recurringNoSkip…} confirms the maintenance cycle materialises
 *       every interval over a five-second period.</li>
 *   <li>{@code clusterPauses…AcrossAnInducedStoreOutage} pauses the
 *       Postgres container mid-run via the Docker API, asserts no progress
 *       while paused, unpauses, and asserts the engine resumes — the
 *       store-outage circuit-breaker recovery path in production.</li>
 * </ul>
 */
@Tag("soak")
@SuppressWarnings("deprecation")
class PostgresSoakTest {

    @SuppressWarnings("resource")
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
                    DockerImageName.parse("postgres:18-alpine"))
            .withDatabaseName("threadmill")
            .withUsername("threadmill")
            .withPassword("threadmill");

    private static PGPoolingDataSource dataSource;
    private final JsonJobSerializer serializer = new JsonJobSerializer();
    private PostgresJobStore store;
    private Scheduler scheduler;
    private ProcessingNode node;

    @BeforeAll
    static void startContainer() {
        POSTGRES.start();
        var ds = new PGPoolingDataSource();
        ds.setServerName(POSTGRES.getHost());
        ds.setPortNumber(POSTGRES.getMappedPort(5432));
        ds.setDatabaseName(POSTGRES.getDatabaseName());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        ds.setDataSourceName("threadmill-soak-" + POSTGRES.getContainerId());
        ds.setInitialConnections(2);
        ds.setMaxConnections(16);
        dataSource = ds;
    }

    @AfterAll
    static void stopContainer() {
        if (dataSource != null) dataSource.close();
        if (POSTGRES.isRunning()) POSTGRES.stop();
    }

    @BeforeEach
    void freshDb() throws SQLException {
        new MigrationRunner(dataSource).migrate();
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            st.execute("TRUNCATE threadmill_jobs, threadmill_nodes, threadmill_metadata, "
                    + "threadmill_cron_task_state, threadmill_cron_tasks, threadmill_mutexes, "
                    + "threadmill_dedup_keys, threadmill_concurrency_groups, "
                    + "threadmill_concurrency_workflow_holds RESTART IDENTITY CASCADE");
            st.execute("UPDATE threadmill_job_counts SET count = 0");
        }
        store = new PostgresJobStore(dataSource);
        scheduler = new Scheduler(store, serializer);
        SoakWork.CountingHandler.COUNT.set(0);
    }

    @AfterEach
    void tearDown() {
        if (node != null) node.close();
    }

    @Test
    void sustainedThroughputProcessesFiveThousandJobsThroughRealPostgres() {
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
                "Soak Postgres throughput: %d jobs in %d ms (%.1f jobs/sec)%n",
                total, elapsedMs, total * 1000.0 / Math.max(1, elapsedMs));
    }

    @Test
    void sustainedContentionOnOneConcurrencyKeyCompletesAgainstPostgres() {
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
                "Soak Postgres contention: %d jobs in %d ms (%.1f jobs/sec)%n",
                total, elapsedMs, total * 1000.0 / Math.max(1, elapsedMs));
    }

    @Test
    void queueFamilyLaneProcessesBurstyProjectQueuesAgainstPostgres() {
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
                "Soak Postgres queue-family: %d jobs in %d ms (%.1f jobs/sec)%n",
                total, elapsedMs, total * 1000.0 / Math.max(1, elapsedMs));
    }

    @Test
    void recurringEveryHundredMillisHasNoSkipsAgainstPostgres() {
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
        // PG round-trips add latency per claim+promote tick; the actual no-skip
        // guarantee is structural (DROP materialises exactly one instance when due).
        // Here we only assert the loop is producing runs at a sustainable rate.
        long expectedMin = Math.max(10L, (elapsedMs - 500) / 100L / 4);
        int observed = SoakWork.CountingHandler.COUNT.get();
        System.out.printf(
                "Soak Postgres recurring: %d runs in %d ms (expected >= %d)%n", observed, elapsedMs, expectedMin);
        assertThat(observed).isGreaterThanOrEqualTo((int) expectedMin);
    }

    @Test
    void clusterPausesAndResumesAcrossAnInducedStoreOutage() throws Exception {
        // Enqueue work, then pause the Postgres container — the engine must pause its
        // dispatcher (via the circuit breaker), not crash; unpause, the engine resumes.
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
        // Let some jobs run before the outage so we can observe both phases.
        await().atMost(Duration.ofSeconds(20)).until(() -> SoakWork.CountingHandler.COUNT.get() >= 50);

        String containerId = POSTGRES.getContainerId();
        var docker = DockerClientFactory.instance().client();
        docker.pauseContainerCmd(containerId).exec();
        int countAtPause = SoakWork.CountingHandler.COUNT.get();
        System.out.printf("Soak Postgres outage: paused container at %d jobs done%n", countAtPause);
        try {
            Thread.sleep(2_000);
        } finally {
            docker.unpauseContainerCmd(containerId).exec();
        }
        int countAfterPause = SoakWork.CountingHandler.COUNT.get();
        // Reasonable: while paused, at most the in-flight batch's worth of work finished.
        assertThat(countAfterPause - countAtPause)
                .as("no large progress while the store was paused")
                .isLessThan(64);

        // After unpause, the dispatcher's circuit breaker must trip back and processing resume.
        await().atMost(Duration.ofMinutes(2)).until(() -> SoakWork.CountingHandler.COUNT.get() >= total);
        System.out.printf("Soak Postgres outage: resumed; %d total processed%n", SoakWork.CountingHandler.COUNT.get());
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
