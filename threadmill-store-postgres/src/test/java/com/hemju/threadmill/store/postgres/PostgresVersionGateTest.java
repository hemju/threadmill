package com.hemju.threadmill.store.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.JobEngineFatalException;

/**
 * Pins the "PostgreSQL 18 or later only" floor.
 *
 * <p>Threadmill's schema, queries, and migration runner target PostgreSQL 18+ only.
 * Older majors are not tested and not supported. This test boots a PostgreSQL 17
 * container and asserts that constructing {@link PostgresJobStore} against it
 * fails fast with a {@link JobEngineFatalException}.
 */
@SuppressWarnings("resource")
class PostgresVersionGateTest {

    private static final PostgreSQLContainer POSTGRES_17 = new PostgreSQLContainer(
                    DockerImageName.parse("postgres:17-alpine"))
            .withDatabaseName("threadmill")
            .withUsername("threadmill")
            .withPassword("threadmill");

    private static DataSource dataSource;

    @BeforeAll
    static void start() {
        POSTGRES_17.start();
        var ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES_17.getJdbcUrl());
        ds.setUser(POSTGRES_17.getUsername());
        ds.setPassword(POSTGRES_17.getPassword());
        dataSource = ds;
    }

    @AfterAll
    static void stop() {
        if (POSTGRES_17.isRunning()) POSTGRES_17.stop();
    }

    @Test
    void refusesToStartAgainstPrePostgresEighteenServers() {
        assertThatThrownBy(() -> new PostgresJobStore(dataSource))
                .isInstanceOf(JobEngineFatalException.class)
                .satisfies(e -> assertThat(e.getMessage())
                        .contains("requires PostgreSQL 18")
                        .contains("server major 17"));
    }
}
