package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.store.JobStoreCapabilities;
import com.hemju.threadmill.store.postgres.MigrationRunner;
import com.hemju.threadmill.store.postgres.PostgresJobStore;

@EnabledIf("com.hemju.threadmill.spring.DockerAvailable#check")
class SpringPostgresTransactionBoundaryTest {

    @SuppressWarnings("resource")
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
                    DockerImageName.parse("postgres:18-alpine"))
            .withDatabaseName("threadmill")
            .withUsername("threadmill")
            .withPassword("threadmill");

    private static DataSource dataSource;

    private PostgresJobStore store;
    private TransactionTemplate transactions;
    private TransactionJoinedJobScheduler scheduler;
    private CopyOnWriteArrayList<String> wakes;

    public static final class GreetPayload implements JobPayload {
        public String tag;

        public GreetPayload() {}

        public GreetPayload(String tag) {
            this.tag = tag;
        }
    }

    public static final class GreetHandler implements JobHandler<GreetPayload> {
        @Override
        public void run(GreetPayload p, JobExecutionContext c) {}
    }

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
    void setUp() throws Exception {
        new MigrationRunner(dataSource).migrate();
        try (var conn = dataSource.getConnection();
                var st = conn.createStatement()) {
            st.executeUpdate("TRUNCATE threadmill_dedup_keys, threadmill_jobs RESTART IDENTITY CASCADE");
            st.executeUpdate("UPDATE threadmill_job_counts SET count = 0");
        }
        store = new PostgresJobStore(
                dataSource,
                new JsonJobSerializer(),
                JobStoreCapabilities.defaults(),
                new SpringPostgresTransactionBoundary(dataSource));
        transactions = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        var wakeBus = new LocalWakeBus();
        wakes = new CopyOnWriteArrayList<>();
        wakeBus.register(wakes::add);
        scheduler = new TransactionJoinedJobScheduler(
                store,
                new JsonJobSerializer(),
                new TestRegistry(),
                ProcessingNodeConfig.builder().build(),
                wakeBus);
    }

    @Test
    void enqueueCommitsWithCallerTransactionAndWakesAfterCommit() {
        AtomicReference<JobId> id = new AtomicReference<>();

        transactions.executeWithoutResult(status -> {
            id.set(scheduler.enqueue(GreetHandler.class, new GreetPayload("commit")));
            assertThat(wakes).isEmpty();
        });

        assertThat(store.findById(id.get())).isPresent();
        assertThat(wakes).containsExactly("default");
    }

    @Test
    void enqueueRollsBackWithCallerTransaction() {
        AtomicReference<JobId> id = new AtomicReference<>();

        transactions.executeWithoutResult(status -> {
            id.set(scheduler.enqueue(GreetHandler.class, new GreetPayload("rollback")));
            status.setRollbackOnly();
        });

        assertThat(store.findById(id.get())).isEmpty();
        assertThat(wakes).isEmpty();
    }

    @Test
    void dedupRollsBackWithCallerTransaction() {
        AtomicReference<EnqueueResult> first = new AtomicReference<>();

        transactions.executeWithoutResult(status -> {
            first.set(scheduler.enqueueIfAbsent(
                    GreetHandler.class, new GreetPayload("rollback"), "tenant:greet", Duration.ofMinutes(5)));
            status.setRollbackOnly();
        });

        assertThat(first.get()).isInstanceOf(EnqueueResult.Created.class);
        EnqueueResult second = scheduler.enqueueIfAbsent(
                GreetHandler.class, new GreetPayload("retry"), "tenant:greet", Duration.ofMinutes(5));
        assertThat(second).isInstanceOf(EnqueueResult.Created.class);
    }

    @Test
    void nudgeTakesEffectOnCommitAndRollsBackWithoutLockingTheTaskRow() {
        // Issue #108 requirement 5: nudged inside a Spring transaction, the
        // nudge takes effect on commit and is discarded on rollback — so a
        // producer can nudge in the same transaction that writes the work row.
        //
        // In this mode the nudge is deliberately the one write that does NOT
        // join the caller's transaction: joining would hold the task's single
        // schedule-state row lock for the whole business transaction and
        // serialize every concurrent producer of that task. The assertions
        // below pin both halves — nothing visible before commit (so a
        // rollback really discards it), and the row is not locked while the
        // caller's transaction is still open.
        var task = new CronTask(
                "outbox-pump",
                new CronTask.Trigger.Interval(Duration.ofHours(6)),
                GreetHandler.class.getName(),
                new JobArgument(GreetPayload.class.getName(), "{}"),
                "default",
                0,
                CronTask.MissedRunPolicy.DROP,
                ZoneId.of("UTC"),
                true);
        store.upsertCronTask(task);
        store.upsertCronTaskState(CronTaskScheduleState.initial(
                "outbox-pump",
                Instant.now().plus(Duration.ofHours(6)),
                CronTaskScheduleState.timingFingerprintOf(task)));

        transactions.executeWithoutResult(status -> {
            scheduler.nudgeRecurring("outbox-pump");
            status.setRollbackOnly();
        });
        assertThat(store.findCronTaskState("outbox-pump").orElseThrow().nudgeRequestedAt())
                .as("a rolled-back transaction leaves no nudge behind")
                .isNull();

        transactions.executeWithoutResult(status -> {
            scheduler.nudgeRecurring("outbox-pump");
            // Still inside the caller's transaction: the nudge has not been
            // written yet, and — the point of deferring it — the task's row
            // is not locked, so another producer can write it right now
            // instead of blocking until this transaction commits.
            assertThat(store.findCronTaskState("outbox-pump").orElseThrow().nudgeRequestedAt())
                    .as("the nudge write is deferred to after commit")
                    .isNull();
            assertThatCode(() -> lockTaskRowFromAnotherConnection("outbox-pump"))
                    .as("a concurrent producer must not block on the nudging transaction")
                    .doesNotThrowAnyException();
        });
        assertThat(store.findCronTaskState("outbox-pump").orElseThrow().nudgeRequestedAt())
                .as("a committed transaction lands the nudge")
                .isNotNull();
    }

    @Test
    void nudgeIsCommittedEvenWhenThePoolHandsOutNonAutoCommitConnections() {
        // Regression: the nudge is deferred to afterCommit, and at that point
        // Spring has committed the caller's transaction but has NOT yet
        // unbound its resources — so a store write routed through the joining
        // boundary would run in a fresh transaction on the caller's
        // connection with nobody left to commit it. With connections that
        // arrive autoCommit=true (an unpooled DataSource) pgjdbc hides the
        // bug: restoring autoCommit at cleanup issues an implicit COMMIT.
        // With a pool configured autoCommit=false — Hikari with JPA defaults,
        // and a supported Spring Boot setting — the write is instead rolled
        // back on release and every nudge is silently lost.
        DataSource nonAutoCommit = new NonAutoCommitDataSource(dataSource);
        var joiningStore = new PostgresJobStore(
                nonAutoCommit,
                new JsonJobSerializer(),
                JobStoreCapabilities.defaults(),
                new SpringPostgresTransactionBoundary(nonAutoCommit));
        var joiningScheduler = new TransactionJoinedJobScheduler(
                joiningStore,
                new JsonJobSerializer(),
                new TestRegistry(),
                ProcessingNodeConfig.builder().build(),
                new LocalWakeBus());
        var transactionsOnPool = new TransactionTemplate(new DataSourceTransactionManager(nonAutoCommit));
        // Register through the plain store: the definition writes are not
        // what this test is about, and several of them use a raw connection
        // that assumes autoCommit=true (a separate, pre-existing store issue).
        registerPumpTask(store, "pooled-pump");

        transactionsOnPool.executeWithoutResult(status -> joiningScheduler.nudgeRecurring("pooled-pump"));

        // Read back through the ORIGINAL DataSource: a fresh connection, so
        // this can only see a genuinely committed write.
        assertThat(store.findCronTaskState("pooled-pump").orElseThrow().nudgeRequestedAt())
                .as("the nudge must be committed by Threadmill's own transaction, not left to the caller's")
                .isNotNull();
    }

    @Test
    void repeatedNudgesOfOneTaskInATransactionCollapseToASingleWrite() {
        // The documented pattern is "nudge once per work item"; a batch
        // importer therefore calls this hundreds of times in one transaction.
        // Each distinct task must be validated and written once — the in-JVM
        // coalescer only collapses concurrent callers, so sequential calls
        // would otherwise be one store round trip each.
        registerPumpTask(store, "batch-pump");

        transactions.executeWithoutResult(status -> {
            for (int i = 0; i < 50; i++) {
                scheduler.nudgeRecurring("batch-pump");
            }
        });

        var state = store.findCronTaskState("batch-pump").orElseThrow();
        assertThat(state.nudgeRequestedAt()).isNotNull();
        assertThat(state.nudgeRevision())
                .as("50 nudges of the same task in one transaction must produce exactly one accepted write")
                .isEqualTo(1L);
    }

    private void registerPumpTask(PostgresJobStore target, String name) {
        var task = new CronTask(
                name,
                new CronTask.Trigger.Interval(Duration.ofHours(6)),
                GreetHandler.class.getName(),
                new JobArgument(GreetPayload.class.getName(), "{}"),
                "default",
                0,
                CronTask.MissedRunPolicy.DROP,
                ZoneId.of("UTC"),
                true);
        target.upsertCronTask(task);
        target.upsertCronTaskState(CronTaskScheduleState.initial(
                name, Instant.now().plus(Duration.ofHours(6)), CronTaskScheduleState.timingFingerprintOf(task)));
    }

    /** A pool-alike whose connections arrive with {@code autoCommit=false}. */
    private static final class NonAutoCommitDataSource extends DelegatingDataSource {
        NonAutoCommitDataSource(DataSource target) {
            super(target);
        }

        @Override
        public Connection getConnection() throws SQLException {
            Connection connection = super.getConnection();
            connection.setAutoCommit(false);
            return connection;
        }
    }

    /**
     * Take the task's schedule-state row lock on an independent connection
     * with {@code NOWAIT}, so a lock still held by the caller's transaction
     * surfaces as an exception instead of hanging the test.
     */
    private static void lockTaskRowFromAnotherConnection(String taskName) throws Exception {
        try (var conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (var ps = conn.prepareStatement(
                    "SELECT 1 FROM threadmill_cron_task_state WHERE task_name = ? FOR UPDATE NOWAIT")) {
                ps.setString(1, taskName);
                ps.executeQuery();
            }
            conn.rollback();
        }
    }

    @Test
    void joinTransactionFailsFastWhenCallerTransactionUsesDifferentDataSource() {
        var other = new PGSimpleDataSource();
        other.setUrl(POSTGRES.getJdbcUrl());
        other.setUser(POSTGRES.getUsername());
        other.setPassword(POSTGRES.getPassword());
        var otherTransactions = new TransactionTemplate(new DataSourceTransactionManager(other));

        otherTransactions.executeWithoutResult(
                status -> assertThatThrownBy(() -> scheduler.enqueue(GreetHandler.class, new GreetPayload("wrong-ds")))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("same DataSource"));

        assertThat(wakes).isEmpty();
    }

    private static final class TestRegistry extends ThreadmillJobRegistry {
        TestRegistry() {
            super(new ThreadmillJobRegistry.Registration(
                    GreetPayload.class, GreetHandler.class, "default", 0, 5, Duration.ofMinutes(5), null));
        }
    }
}
