package com.hemju.threadmill.soak.harness;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

import javax.sql.DataSource;

import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.JobEngineFatalException;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.postgres.MigrationRunner;
import com.hemju.threadmill.store.postgres.PostgresJobStore;

/**
 * Boots a {@code postgres:18-alpine} Testcontainer (or, if
 * {@code -PpostgresUrl=...} is given, points at an external instance). The
 * threadmill schema is migrated and truncated before the run starts so each
 * invocation begins on a clean slate.
 */
public final class PostgresHarnessFixture implements BackendFixture {

    @SuppressWarnings({"resource", "deprecation"})
    private final PostgreSQLContainer container;

    private final DataSource dataSource;
    private final PostgresJobStore store;

    @SuppressWarnings({"resource", "deprecation"})
    public PostgresHarnessFixture(Optional<String> externalJdbcUrl) {
        if (externalJdbcUrl.isPresent()) {
            this.container = null;
            this.dataSource = dataSourceFromUrl(externalJdbcUrl.get());
        } else {
            this.container = new PostgreSQLContainer(DockerImageName.parse("postgres:18-alpine"))
                    .withDatabaseName("threadmill")
                    .withUsername("threadmill")
                    .withPassword("threadmill");
            container.start();
            this.dataSource = dataSourceFromContainer(container);
        }
        new MigrationRunner(dataSource).migrate();
        truncate();
        this.store = new PostgresJobStore(dataSource);
    }

    private void truncate() {
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            st.execute("TRUNCATE threadmill_jobs, threadmill_nodes, threadmill_metadata, "
                    + "threadmill_cron_task_state, threadmill_cron_tasks, threadmill_mutexes, "
                    + "threadmill_dedup_keys, threadmill_concurrency_groups, "
                    + "threadmill_concurrency_workflow_holds RESTART IDENTITY CASCADE");
            st.execute("UPDATE threadmill_job_counts SET count = 0");
        } catch (SQLException e) {
            throw new JobEngineFatalException("could not truncate Postgres tables before run: " + e.getMessage(), e);
        }
    }

    @Override
    public JobStore store() {
        return store;
    }

    @Override
    public void close() {
        if (container != null && container.isRunning()) container.stop();
    }

    @SuppressWarnings("deprecation")
    private static DataSource dataSourceFromContainer(PostgreSQLContainer container) {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(container.getJdbcUrl());
        ds.setUser(container.getUsername());
        ds.setPassword(container.getPassword());
        return ds;
    }

    private static DataSource dataSourceFromUrl(String jdbcUrl) {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(jdbcUrl);
        return ds;
    }
}
