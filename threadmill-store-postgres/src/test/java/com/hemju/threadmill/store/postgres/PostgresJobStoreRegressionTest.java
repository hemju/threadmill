package com.hemju.threadmill.store.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStore;

/**
 * PostgreSQL-specific regression tests.
 *
 * <p>The contract suite ({@link PostgresJobStoreContractTest}) already
 * covers the SPI semantics on real PostgreSQL. These tests pin down
 * Postgres-only invariants: encoding correctness on the body and
 * metadata columns, atomic claim across many concurrent virtual-thread
 * workers, the cheapness of per-state counts as the jobs table grows,
 * deadlock-state recognition, and migration-emit / cron-task /
 * mutex-lease semantics.
 */
@EnabledIf("com.hemju.threadmill.store.postgres.DockerAvailable#check")
class PostgresJobStoreRegressionTest {

    @SuppressWarnings("resource")
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
                    DockerImageName.parse("postgres:18-alpine"))
            .withDatabaseName("threadmill")
            .withUsername("threadmill")
            .withPassword("threadmill");

    private static DataSource dataSource;

    @BeforeAll
    static void start() {
        POSTGRES.start();
        var ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        dataSource = ds;
    }

    @AfterAll
    static void stop() {
        if (POSTGRES.isRunning()) POSTGRES.stop();
    }

    @BeforeEach
    void migrate() throws SQLException {
        new MigrationRunner(dataSource).migrate();
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            st.execute("TRUNCATE threadmill_jobs, threadmill_nodes, threadmill_metadata, "
                    + "threadmill_cron_tasks, threadmill_mutexes, threadmill_leases, "
                    + "threadmill_dedup_keys, threadmill_concurrency_groups, "
                    + "threadmill_concurrency_workflow_holds RESTART IDENTITY CASCADE");
            st.execute("UPDATE threadmill_job_counts SET count = 0");
        }
    }

    private JobStore store() {
        return new PostgresJobStore(dataSource);
    }

    @Test
    void fourByteUnicodeRoundTripsThroughJsonBodyAndMetadata() {
        String exotic = "shipping ✈ 🚀 𐀀 𐀁 𐀂";
        JobStore store = store();
        Job j = Job.builder()
                .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
                .metadata("note", exotic)
                .build();
        j.log().info(exotic);
        store.insert(j);

        Job loaded = store.findById(j.id()).orElseThrow();
        assertThat(loaded.metadata().get("note")).contains(exotic);
        assertThat(loaded.log().snapshot().get(0).message()).isEqualTo(exotic);
    }

    @Test
    void claimReadyIsAtomicAcrossManyConcurrentVirtualThreads() throws Exception {
        JobStore store = store();
        int total = 200;
        for (int i = 0; i < total; i++) {
            store.insert(Job.builder()
                    .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
                    .build());
        }

        int workers = 12;
        var start = new CountDownLatch(1);
        Set<java.util.UUID> seen = ConcurrentHashMap.newKeySet();
        var collisions = new ConcurrentHashMap<java.util.UUID, Integer>();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<java.util.concurrent.Future<List<Job>>> futures = new ArrayList<>();
            for (int w = 0; w < workers; w++) {
                NodeId node = NodeId.newId();
                futures.add(executor.submit(() -> {
                    start.await();
                    List<Job> mine = new ArrayList<>();
                    while (true) {
                        List<Job> got = store.claimReady(node, "default", 7, Instant.now());
                        if (got.isEmpty()) break;
                        mine.addAll(got);
                    }
                    return mine;
                }));
            }
            start.countDown();
            for (var f : futures) {
                for (Job j : f.get(60, TimeUnit.SECONDS)) {
                    if (!seen.add(j.id().asUuid())) {
                        collisions.merge(j.id().asUuid(), 1, Integer::sum);
                    }
                }
            }
            executor.shutdown();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(collisions)
                .as("no double-claim across concurrent virtual-thread workers")
                .isEmpty();
        assertThat(seen).hasSize(total);
    }

    @Test
    void perStateCountsReadFromCounterTableNotFromJobsTable() throws SQLException {
        JobStore store = store();
        for (int i = 0; i < 100; i++) {
            store.insert(Job.builder()
                    .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
                    .build());
        }
        // After insert, the trigger should have ENQUEUED at 100.
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps =
                        conn.prepareStatement("EXPLAIN (FORMAT TEXT) SELECT state, count FROM threadmill_job_counts")) {
            try (ResultSet rs = ps.executeQuery()) {
                var plan = new StringBuilder();
                while (rs.next()) plan.append(rs.getString(1)).append('\n');
                // Reading from the counter table must not touch threadmill_jobs.
                assertThat(plan.toString()).doesNotContainIgnoringCase("threadmill_jobs");
            }
        }

        var counts = store.countsByState();
        assertThat(counts.get(JobState.ENQUEUED)).isEqualTo(100L);

        // After claim, ENQUEUED should drop and PROCESSING should rise — proves the trigger fires on UPDATE too.
        store.claimReady(NodeId.newId(), "default", 30, Instant.now());
        var afterClaim = store.countsByState();
        assertThat(afterClaim.get(JobState.ENQUEUED)).isEqualTo(70L);
        assertThat(afterClaim.get(JobState.PROCESSING)).isEqualTo(30L);
    }

    @Test
    void deadlockRetryRecognisesDeadlockSqlState() {
        var deadlock = new SQLException("simulated deadlock", "40P01");
        var unrelated = new SQLException("other", "23505");
        assertThat(DeadlockRetry.isRetryable(deadlock)).isTrue();
        assertThat(DeadlockRetry.isRetryable(unrelated)).isFalse();
    }

    @Test
    void migrationsAreIdempotent() {
        // Already applied by @BeforeEach; running again must be a no-op.
        new MigrationRunner(dataSource).migrate();
        new MigrationRunner(dataSource).migrate();
    }

    @Test
    void emitPendingSqlAfterMigrateIsEmpty() {
        String sql = new MigrationRunner(dataSource).emitPendingSql();
        assertThat(sql).isBlank();
    }

    @Test
    void emittedMigrationSqlAppliesToACleanSchema() throws SQLException {
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS threadmill_mutexes CASCADE");
            st.execute("DROP TABLE IF EXISTS threadmill_concurrency_workflow_holds CASCADE");
            st.execute("DROP TABLE IF EXISTS threadmill_concurrency_groups CASCADE");
            st.execute("DROP TABLE IF EXISTS threadmill_dedup_keys CASCADE");
            st.execute("DROP TABLE IF EXISTS threadmill_cron_task_state CASCADE");
            st.execute("DROP TABLE IF EXISTS threadmill_cron_tasks CASCADE");
            st.execute("DROP TABLE IF EXISTS threadmill_jobs CASCADE");
            st.execute("DROP TABLE IF EXISTS threadmill_nodes CASCADE");
            st.execute("DROP TABLE IF EXISTS threadmill_leases CASCADE");
            st.execute("DROP TABLE IF EXISTS threadmill_metadata CASCADE");
            st.execute("DROP TABLE IF EXISTS threadmill_job_counts CASCADE");
            st.execute("DROP TABLE IF EXISTS threadmill_queue_pauses CASCADE");
            st.execute("DROP TABLE IF EXISTS threadmill_schema_history CASCADE");
        }

        String sql = new MigrationRunner(dataSource).emitPendingSql();
        assertThat(sql).contains("V1__baseline.sql");

        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            st.execute(sql);
            try (ResultSet rs = st.executeQuery("SELECT count(*) FROM threadmill_schema_history")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isEqualTo(1);
            }
            try (ResultSet rs = st.executeQuery("SELECT count(*) FROM threadmill_job_counts")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isEqualTo(JobState.values().length);
            }
        }
    }

    @Test
    void concurrencyPendingCheckUsesPartialIndex() throws SQLException {
        JobStore store = store();
        var base = Instant.now().minusSeconds(5);
        for (int i = 0; i < 100; i++) {
            store.insert(Job.builder()
                    .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
                    .concurrencyKey("project:hot")
                    .concurrencyMode(i % 10 == 0 ? ConcurrencyMode.EXCLUSIVE : ConcurrencyMode.SHARED)
                    .createdAt(base.plusMillis(i))
                    .build());
        }

        JobId candidateId = JobId.newId();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("EXPLAIN (FORMAT TEXT) SELECT EXISTS ("
                        + "SELECT 1 FROM threadmill_jobs "
                        + "WHERE concurrency_key = ? "
                        + "AND concurrency_mode = 'EXCLUSIVE' "
                        + "AND state IN ('ENQUEUED','SCHEDULED','AWAITING') "
                        + "AND current_state_at < ? "
                        + "AND id <> ?)")) {
            ps.setString(1, "project:hot");
            ps.setTimestamp(2, java.sql.Timestamp.from(base.plusSeconds(1)));
            ps.setObject(3, candidateId.asUuid());
            try (ResultSet rs = ps.executeQuery()) {
                var plan = new StringBuilder();
                while (rs.next()) plan.append(rs.getString(1)).append('\n');
                assertThat(plan.toString()).contains("threadmill_jobs_concurrency_pending_idx");
            }
        }
    }

    @Test
    void cronTaskDefinitionAndScheduleStateRoundTrip() {
        JobStore store = store();
        CronTask task = sampleCronTask("nightly-cleanup");
        var next = Instant.parse("2026-05-16T09:00:00Z");
        var last = Instant.parse("2026-05-15T09:00:00Z");

        store.upsertCronTask(task);
        store.upsertCronTaskState(new CronTaskScheduleState(
                task.name(), last, java.util.UUID.randomUUID(), next, java.util.UUID.randomUUID()));

        assertThat(store.findCronTask(task.name())).contains(task);
        assertThat(store.listCronTasks()).containsExactly(task);
        assertThat(store.findCronTaskState(task.name())).hasValueSatisfying(state -> {
            assertThat(state.lastRunAt()).isEqualTo(last);
            assertThat(state.nextRunAt()).isEqualTo(next);
            assertThat(state.inFlightJobId()).isNotNull();
        });

        store.deleteCronTask(task.name());
        assertThat(store.findCronTask(task.name())).isEmpty();
        assertThat(store.findCronTaskState(task.name())).isEmpty();
    }

    @Test
    void mutexLeaseIsExclusiveReentrantAndExpires() throws InterruptedException {
        JobStore store = store();
        assertThat(store.tryAcquireMutex("billing-close", "node-a", Duration.ofMillis(75)))
                .isTrue();
        assertThat(store.tryAcquireMutex("billing-close", "node-b", Duration.ofSeconds(5)))
                .isFalse();
        assertThat(store.tryAcquireMutex("billing-close", "node-a", Duration.ofSeconds(5)))
                .isTrue();
        store.releaseMutex("billing-close", "wrong-holder");
        assertThat(store.tryAcquireMutex("billing-close", "node-b", Duration.ofMillis(75)))
                .isFalse();
        store.releaseMutex("billing-close", "node-a");
        assertThat(store.tryAcquireMutex("billing-close", "node-b", Duration.ofMillis(75)))
                .isTrue();
        Thread.sleep(220);
        assertThat(store.tryAcquireMutex("billing-close", "node-c", Duration.ofSeconds(5)))
                .isTrue();
    }

    private static CronTask sampleCronTask(String name) {
        return new CronTask(
                name,
                new CronTask.Trigger.Interval(Duration.ofMinutes(5)),
                "com.example.Cleanup",
                new JobArgument("java.lang.String", "\"payload\""),
                "system",
                7,
                CronTask.MissedRunPolicy.CATCH_UP,
                ZoneId.of("UTC"),
                true);
    }
}
