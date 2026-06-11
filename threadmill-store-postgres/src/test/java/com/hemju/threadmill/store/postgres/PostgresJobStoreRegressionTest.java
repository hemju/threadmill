package com.hemju.threadmill.store.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
import com.hemju.threadmill.core.JobRelationship;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.test.Jobs;

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
    void unreadableBodyAtTheQueueHeadDoesNotStallClaimsForGoodJobs() throws SQLException {
        JobStore store = store();
        // Built in order, so the UUIDv7 ids are creation-ordered and `head` sits
        // at the queue head (ORDER BY priority DESC, id).
        Job head = sampleOnDefault();
        Job good1 = sampleOnDefault();
        Job good2 = sampleOnDefault();
        store.insert(head);
        store.insert(good1);
        store.insert(good2);

        // Corrupt the head job's persisted body so deserialize fails.
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps =
                        conn.prepareStatement("UPDATE threadmill_jobs SET body = '{not valid json' WHERE id = ?")) {
            ps.setObject(1, head.id().asUuid());
            ps.executeUpdate();
        }

        List<Job> claimed = store.claimReady(NodeId.newId(), "default", 3, Instant.now());
        assertThat(claimed).extracting(Job::id).containsExactlyInAnyOrder(good1.id(), good2.id());

        // The poison job is quarantined, not left ENQUEUED to wedge the queue.
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT state FROM threadmill_jobs WHERE id = ?")) {
            ps.setObject(1, head.id().asUuid());
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(1)).isEqualTo("QUARANTINED");
            }
        }
    }

    private static Job sampleOnDefault() {
        return Job.builder()
                .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
                .queue("default")
                .build();
    }

    @Test
    void verifyWritableProbesTheDatabaseAndFailsWhileUnreachable() {
        // A trippable DataSource that throws on getConnection when "down".
        var down = new java.util.concurrent.atomic.AtomicBoolean(false);
        DataSource flaky = (DataSource) java.lang.reflect.Proxy.newProxyInstance(
                DataSource.class.getClassLoader(), new Class<?>[] {DataSource.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getConnection") && down.get()) {
                        throw new SQLException("simulated outage");
                    }
                    try {
                        return method.invoke(dataSource, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        var store = new PostgresJobStore(flaky);

        store.verifyWritable(); // healthy: a real SELECT 1 round trip succeeds

        down.set(true);
        assertThatThrownBy(store::verifyWritable).isInstanceOf(PostgresJobStore.JdbcException.class);

        down.set(false);
        store.verifyWritable(); // recovered: the probe passes again
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
    void insertAllWithReversedKeyOrdersDoesNotManufactureDeadlocks() throws Exception {
        // Concurrent batches whose keyed jobs arrive in opposite key orders:
        // per-snapshot group-row locking in batch order deadlocked (recovered
        // by retry in owning mode, fatal in join_transaction mode). With the
        // sorted single locking pass, no round may fail.
        JobStore store = store();
        int rounds = 25;
        var keys = new ArrayList<String>();
        for (int i = 0; i < 8; i++) keys.add("lock-order:" + i);

        for (int round = 0; round < rounds; round++) {
            var ascending = new ArrayList<Job>();
            var descending = new ArrayList<Job>();
            for (int i = 0; i < keys.size(); i++) {
                ascending.add(keyedJob(keys.get(i)));
                descending.add(keyedJob(keys.get(keys.size() - 1 - i)));
            }
            var start = new CountDownLatch(1);
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                Future<?> a = executor.submit(() -> {
                    start.await();
                    return store.insertAll(ascending);
                });
                Future<?> b = executor.submit(() -> {
                    start.await();
                    return store.insertAll(descending);
                });
                start.countDown();
                a.get(30, TimeUnit.SECONDS);
                b.get(30, TimeUnit.SECONDS);
            }
        }
        assertThat(store.countsByState().get(JobState.ENQUEUED)).isEqualTo(rounds * keys.size() * 2L);
    }

    private static Job keyedJob(String key) {
        return Job.builder()
                .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
                .concurrencyKey(key)
                .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
                .build();
    }

    @Test
    void unkeyedClaimLocksOnlyANarrowPageSoConcurrentClaimersAreNotStarved() throws Exception {
        // With no keyed jobs, each claim must lock only a narrow page. The
        // historical unconditional 64x page pinned the entire backlog here
        // (640 > 300), so overlapping claimers' SKIP LOCKED scans returned
        // empty while claimable work existed.
        JobStore store = store();
        int total = 300;
        for (int i = 0; i < total; i++) {
            store.insert(Job.builder()
                    .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
                    .build());
        }

        int claimers = 8;
        int perClaim = 10;
        var start = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<List<Job>>> futures = new ArrayList<>();
            for (int i = 0; i < claimers; i++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    return store.claimReady(NodeId.newId(), "default", perClaim, Instant.now());
                }));
            }
            start.countDown();
            Set<UUID> seen = new HashSet<>();
            for (Future<List<Job>> f : futures) {
                List<Job> claimed = f.get(60, TimeUnit.SECONDS);
                // 300 jobs comfortably cover 8 claimers x (2x10)-row pages:
                // every overlapping claimer must find its full batch.
                assertThat(claimed).hasSize(perClaim);
                for (Job j : claimed) {
                    assertThat(seen.add(j.id().asUuid())).isTrue();
                }
            }
        }
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
        Set<UUID> seen = ConcurrentHashMap.newKeySet();
        var collisions = new ConcurrentHashMap<UUID, Integer>();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<List<Job>>> futures = new ArrayList<>();
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
    void emittedSqlWrapsEachMigrationAndItsHistoryInsertInOneTransaction() throws SQLException {
        dropSchemaObjects();
        String sql = new MigrationRunner(dataSource).emitPendingSql();

        // Every shipped migration's DDL and its history INSERT are bracketed by
        // BEGIN/COMMIT so an external psql apply cannot half-apply a migration.
        long begins = sql.lines().filter(l -> l.strip().equals("BEGIN;")).count();
        long commits = sql.lines().filter(l -> l.strip().equals("COMMIT;")).count();
        assertThat(begins).isEqualTo(commits).isGreaterThanOrEqualTo(1L);
        assertThat(sql).contains("BEGIN;").contains("COMMIT;");
        // The history INSERT lives inside a transaction block.
        int firstBegin = sql.indexOf("BEGIN;");
        int firstInsert = sql.indexOf("INSERT INTO threadmill_schema_history");
        int firstCommitAfter = sql.indexOf("COMMIT;", firstInsert);
        assertThat(firstBegin).isLessThan(firstInsert);
        assertThat(firstInsert).isLessThan(firstCommitAfter);
    }

    @Test
    void emitPendingSqlOnAFreshDatabaseIsReadOnlyAndPrependsHistoryDdl() throws SQLException {
        dropSchemaObjects();

        String sql = new MigrationRunner(dataSource).emitPendingSql();

        // The inspect-only API must not execute DDL: no history table (or any
        // other Threadmill table) may exist afterwards...
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT to_regclass('threadmill_schema_history')")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isNull();
        }
        // ...and the emitted SQL must carry the history-table DDL itself plus
        // every shipped migration, so an external apply works on a clean DB.
        assertThat(sql).contains("threadmill_schema_history").contains("V1__baseline.sql");
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            st.execute(sql);
            try (ResultSet rs = st.executeQuery("SELECT count(*) FROM threadmill_schema_history")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isEqualTo(1);
            }
        }
        new MigrationRunner(dataSource).validate();
    }

    @Test
    void emittedMigrationSqlAppliesToACleanSchema() throws SQLException {
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS threadmill_mutexes CASCADE");
            st.execute("DROP TABLE IF EXISTS threadmill_concurrency_workflow_holds CASCADE");
            st.execute("DROP TABLE IF EXISTS threadmill_concurrency_groups CASCADE");
            st.execute("DROP TABLE IF EXISTS threadmill_dedup_keys CASCADE");
            st.execute("DROP TABLE IF EXISTS threadmill_cron_task_ownership CASCADE");
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

        String sql = new MigrationRunner(dataSource).emitCleanInstallSql();
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
    void validationPassesAfterMigrate() {
        new MigrationRunner(dataSource).validate();
    }

    @Test
    void migrateFailsFastWhenHistoryContainsAVersionThisBinaryDoesNotShip() throws SQLException {
        // Simulate a binary downgrade: a newer binary applied a future version.
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            st.execute("INSERT INTO threadmill_schema_history (version, description, checksum) "
                    + "VALUES (999, 'future', 'deadbeef')");
        }
        try {
            assertThatThrownBy(() -> new MigrationRunner(dataSource).migrate())
                    .isInstanceOf(MigrationRunner.MigrationException.class)
                    .hasMessageContaining("999")
                    .hasMessageContaining("newer");
        } finally {
            try (Connection conn = dataSource.getConnection();
                    Statement st = conn.createStatement()) {
                st.execute("DELETE FROM threadmill_schema_history WHERE version = 999");
            }
        }
    }

    @Test
    void validateFailsWhenAnAppliedMigrationFileWasEditedInPlace() throws SQLException {
        // Tamper with the stored checksum to mimic an edited migration file.
        String original;
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT checksum FROM threadmill_schema_history WHERE version = 1")) {
            rs.next();
            original = rs.getString(1);
            assertThat(original).isNotBlank();
        }
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            st.execute("UPDATE threadmill_schema_history SET checksum = 'tampered' WHERE version = 1");
        }
        try {
            assertThatThrownBy(() -> new MigrationRunner(dataSource).validate())
                    .isInstanceOf(MigrationRunner.MigrationException.class)
                    .hasMessageContaining("edited after it was applied");
        } finally {
            try (Connection conn = dataSource.getConnection();
                    PreparedStatement ps = conn.prepareStatement(
                            "UPDATE threadmill_schema_history SET checksum = ? WHERE version = 1")) {
                ps.setString(1, original);
                ps.executeUpdate();
            }
        }
    }

    @Test
    void validationFailsWhenHistoryTableIsMissing() throws SQLException {
        dropSchemaObjects();

        assertThatThrownBy(() -> new MigrationRunner(dataSource).validate())
                .isInstanceOf(MigrationRunner.MigrationException.class)
                .hasMessageContaining("threadmill_schema_history");
    }

    @Test
    void validationFailsWhenHistoryIsInconsistent() throws SQLException {
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            st.execute("UPDATE threadmill_schema_history SET description = 'old baseline' WHERE version = 1");
        }

        try {
            assertThatThrownBy(() -> new MigrationRunner(dataSource).validate())
                    .isInstanceOf(MigrationRunner.MigrationException.class)
                    .hasMessageContaining("expected 'baseline'");
        } finally {
            try (Connection conn = dataSource.getConnection();
                    Statement st = conn.createStatement()) {
                st.execute("UPDATE threadmill_schema_history SET description = 'baseline' WHERE version = 1");
            }
        }
    }

    @Test
    void dropThreadmillObjectsAllowsCleanReinitialize() throws SQLException {
        new MigrationRunner(dataSource).dropThreadmillObjects();

        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT to_regclass('threadmill_jobs')")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isNull();
        }

        new MigrationRunner(dataSource).migrate();
        new MigrationRunner(dataSource).validate();
    }

    @Test
    void concurrentCleanSchemaMigrationsAreSerialized() throws Exception {
        dropSchemaObjects();

        var start = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<?> first = executor.submit(() -> {
                start.await();
                new MigrationRunner(dataSource).migrate();
                return null;
            });
            Future<?> second = executor.submit(() -> {
                start.await();
                new MigrationRunner(dataSource).migrate();
                return null;
            });
            start.countDown();
            first.get(60, TimeUnit.SECONDS);
            second.get(60, TimeUnit.SECONDS);
            executor.shutdown();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }

        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT count(*) FROM threadmill_schema_history")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(1);
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
                        + "AND (current_state_at < ? OR (current_state_at = ? AND id < ?)))")) {
            ps.setString(1, "project:hot");
            ps.setTimestamp(2, Timestamp.from(base.plusSeconds(1)));
            ps.setTimestamp(3, Timestamp.from(base.plusSeconds(1)));
            ps.setObject(4, candidateId.asUuid());
            try (ResultSet rs = ps.executeQuery()) {
                var plan = new StringBuilder();
                while (rs.next()) plan.append(rs.getString(1)).append('\n');
                assertThat(plan.toString()).contains("threadmill_jobs_concurrency_pending_idx");
            }
        }
    }

    @Test
    void batchedConcurrencyPendingLookupUsesPartialIndex() throws SQLException {
        JobStore store = store();
        var base = Instant.now().minusSeconds(5);
        for (int i = 0; i < 100; i++) {
            store.insert(Job.builder()
                    .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
                    .concurrencyKey(i % 2 == 0 ? "project:hot" : "project:warm")
                    .concurrencyMode(i % 10 == 0 ? ConcurrencyMode.EXCLUSIVE : ConcurrencyMode.SHARED)
                    .createdAt(base.plusMillis(i))
                    .build());
        }

        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            st.execute("SET enable_seqscan = off");
            var keyArray = conn.createArrayOf("text", new String[] {"project:hot", "project:warm"});
            try (PreparedStatement ps = conn.prepareStatement("EXPLAIN (FORMAT TEXT) "
                    + "SELECT DISTINCT ON (concurrency_key) concurrency_key, current_state_at, id "
                    + "FROM threadmill_jobs "
                    + "WHERE concurrency_key = ANY (?) "
                    + "AND state IN ('ENQUEUED','SCHEDULED','AWAITING') "
                    + "ORDER BY concurrency_key, current_state_at, id")) {
                ps.setArray(1, keyArray);
                try (ResultSet rs = ps.executeQuery()) {
                    var plan = new StringBuilder();
                    while (rs.next()) plan.append(rs.getString(1)).append('\n');
                    assertThat(plan.toString()).contains("threadmill_jobs_concurrency_pending_idx");
                }
            } finally {
                keyArray.free();
            }
        }
    }

    @Test
    void workflowOutstandingCountUsesPartialIndex() throws SQLException {
        JobStore store = store();
        Job root = Job.builder()
                .spec(JobSpec.of("com.example.Root", new JobArgument("java.lang.String", "\"x\"")))
                .concurrencyKey("project:workflow")
                .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
                .build();
        store.insert(root);
        for (int i = 0; i < 80; i++) {
            store.insert(awaitingChildOf(root, i));
        }

        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            st.execute("SET enable_seqscan = off");
            try (PreparedStatement ps = conn.prepareStatement("EXPLAIN (FORMAT TEXT) SELECT count(*) "
                    + "FROM threadmill_jobs "
                    + "WHERE concurrency_key = ? AND workflow_root_id = ? "
                    + "AND state NOT IN ('SUCCEEDED','FAILED','DELETED','QUARANTINED')")) {
                ps.setString(1, "project:workflow");
                ps.setObject(2, root.id().asUuid());
                try (ResultSet rs = ps.executeQuery()) {
                    var plan = new StringBuilder();
                    while (rs.next()) plan.append(rs.getString(1)).append('\n');
                    assertThat(plan.toString()).contains("threadmill_jobs_workflow_outstanding_idx");
                }
            }
        }
    }

    @Test
    void orphanRecoveryUsesProcessingLivenessIndex() throws SQLException {
        JobStore store = store();
        var heartbeat = Instant.now().minusSeconds(120);
        for (int i = 0; i < 120; i++) {
            store.insert(Jobs.enqueued("com.example.H"));
        }
        store.claimReady(NodeId.newId(), "default", 120, heartbeat);

        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            st.execute("SET enable_seqscan = off");
            try (PreparedStatement ps = conn.prepareStatement("EXPLAIN (FORMAT TEXT) "
                    + "SELECT body FROM threadmill_jobs WHERE state = 'PROCESSING' "
                    + "AND GREATEST(owner_heartbeat_at, COALESCE(last_checkin_at, owner_heartbeat_at)) <= ? "
                    + "ORDER BY GREATEST(owner_heartbeat_at, COALESCE(last_checkin_at, owner_heartbeat_at)) LIMIT ?")) {
                ps.setTimestamp(1, Timestamp.from(Instant.now()));
                ps.setInt(2, 10);
                try (ResultSet rs = ps.executeQuery()) {
                    var plan = new StringBuilder();
                    while (rs.next()) plan.append(rs.getString(1)).append('\n');
                    assertThat(plan.toString()).contains("threadmill_jobs_processing_liveness_idx");
                }
            }
        }
    }

    @Test
    void workflowSuccessorLookupUsesParentIndex() throws SQLException {
        JobStore store = store();
        Job parent = Jobs.enqueued("com.example.Parent");
        Job otherParent = Jobs.enqueued("com.example.OtherParent");
        store.insert(parent);
        store.insert(otherParent);

        for (int i = 0; i < 80; i++) {
            store.insert(awaitingChildOf(i % 3 == 0 ? otherParent : parent, i));
        }

        assertThat(store.findAwaitingByParent(parent.id(), 10))
                .hasSize(10)
                .allSatisfy(job -> assertThat(job.relationship())
                        .hasValueSatisfying(relationship ->
                                assertThat(relationship.parentId()).isEqualTo(parent.id())));

        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            st.execute("SET enable_seqscan = off");
            try (PreparedStatement ps = conn.prepareStatement("EXPLAIN (FORMAT TEXT) "
                    + "SELECT body FROM threadmill_jobs WHERE state = 'AWAITING' AND parent_job_id = ? "
                    + "ORDER BY current_state_at, id LIMIT ?")) {
                ps.setObject(1, parent.id().asUuid());
                ps.setInt(2, 10);
                try (ResultSet rs = ps.executeQuery()) {
                    var plan = new StringBuilder();
                    while (rs.next()) plan.append(rs.getString(1)).append('\n');
                    assertThat(plan.toString()).contains("threadmill_jobs_awaiting_parent_idx");
                }
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
        store.upsertCronTaskState(
                new CronTaskScheduleState(task.name(), last, UUID.randomUUID(), next, UUID.randomUUID()));

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

    private static Job awaitingChildOf(Job parent, int index) {
        return Job.builder()
                .spec(JobSpec.of("com.example.Child", new JobArgument("java.lang.String", "\"" + index + "\"")))
                .relationship(new JobRelationship(parent.id(), JobRelationship.Kind.WORKFLOW_STEP))
                .initialState(JobState.AWAITING)
                .createdAt(Instant.now().plusMillis(index))
                .build();
    }

    private static void dropSchemaObjects() throws SQLException {
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS threadmill_mutexes CASCADE");
            st.execute("DROP TABLE IF EXISTS threadmill_concurrency_workflow_holds CASCADE");
            st.execute("DROP TABLE IF EXISTS threadmill_concurrency_groups CASCADE");
            st.execute("DROP TABLE IF EXISTS threadmill_dedup_keys CASCADE");
            st.execute("DROP TABLE IF EXISTS threadmill_cron_task_ownership CASCADE");
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
    }
}
