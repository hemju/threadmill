package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
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
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
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
