package com.hemju.threadmill.store.postgres;

import java.sql.Connection;
import java.sql.Statement;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.test.AbstractJobStoreContractTest;

/**
 * Runs the {@link AbstractJobStoreContractTest} against real PostgreSQL via
 * Testcontainers. The exact same 20 tests that the in-memory store passes
 * must also pass here — that is the contract.
 */
class PostgresJobStoreContractTest extends AbstractJobStoreContractTest {

    @SuppressWarnings("resource")
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
                    DockerImageName.parse("postgres:18-alpine"))
            .withDatabaseName("threadmill")
            .withUsername("threadmill")
            .withPassword("threadmill")
            .withReuse(false);

    private static DataSource dataSource;

    @BeforeAll
    static void startContainer() {
        POSTGRES.start();
        var ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        dataSource = ds;
    }

    @AfterAll
    static void stopContainer() {
        if (POSTGRES.isRunning()) {
            POSTGRES.stop();
        }
    }

    @BeforeEach
    void truncateBetweenTests() throws Exception {
        new MigrationRunner(dataSource).migrate();
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            st.execute("TRUNCATE threadmill_jobs, threadmill_nodes, threadmill_metadata, "
                    + "threadmill_cron_tasks, threadmill_mutexes, threadmill_leases, "
                    + "threadmill_dedup_keys, threadmill_concurrency_groups, "
                    + "threadmill_concurrency_workflow_holds RESTART IDENTITY CASCADE");
            // The counts table is kept in sync by triggers, but TRUNCATE bypasses them — reset counts manually.
            st.execute("UPDATE threadmill_job_counts SET count = 0");
        }
    }

    @Override
    protected JobStore createStore() {
        return new PostgresJobStore(dataSource);
    }
}
