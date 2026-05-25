package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.ResultSet;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.postgres.MigrationRunner;
import com.hemju.threadmill.store.postgres.PostgresJobStore;

@EnabledIf("com.hemju.threadmill.spring.DockerAvailable#check")
class SpringPostgresSchemaAutoConfigurationTest {

    @SuppressWarnings("resource")
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
                    DockerImageName.parse("postgres:18-alpine"))
            .withDatabaseName("threadmill")
            .withUsername("threadmill")
            .withPassword("threadmill");

    private static DataSource dataSource;

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ThreadmillAutoConfiguration.class))
            .withBean(DataSource.class, () -> dataSource)
            .withPropertyValues("threadmill.enabled=false");

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
    void dropSchema() {
        new MigrationRunner(dataSource).dropThreadmillObjects();
    }

    @Test
    void defaultPostgresAutoConfigurationMigratesBeforeCreatingStore() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(JobStore.class)).isInstanceOf(PostgresJobStore.class);
        });

        new MigrationRunner(dataSource).validate();
    }

    @Test
    void validateSchemaModeFailsOnEmptySchema() {
        contextRunner
                .withPropertyValues("threadmill.store.postgres.schema-mode=validate")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasMessageContaining("threadmill_schema_history");
                });
    }

    @Test
    void validateSchemaModePassesAfterMigration() {
        new MigrationRunner(dataSource).migrate();

        contextRunner
                .withPropertyValues("threadmill.store.postgres.schema-mode=validate")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(JobStore.class)).isInstanceOf(PostgresJobStore.class);
                });
    }

    @Test
    void noneSchemaModeDoesNotCreateSchema() throws Exception {
        contextRunner
                .withPropertyValues("threadmill.store.postgres.schema-mode=none")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(JobStore.class)).isInstanceOf(PostgresJobStore.class);
                });

        assertThat(tableExists("threadmill_schema_history")).isFalse();
    }

    @Test
    void dropAndMigrateRequiresExplicitDestructiveResetFlag() {
        contextRunner
                .withPropertyValues("threadmill.store.postgres.schema-mode=drop-and-migrate")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasMessageContaining("allow-destructive-schema-reset=true");
                });
    }

    @Test
    void dropAndMigrateReinitializesWhenExplicitlyAllowed() {
        new MigrationRunner(dataSource).migrate();

        contextRunner
                .withPropertyValues(
                        "threadmill.store.postgres.schema-mode=drop-and-migrate",
                        "threadmill.store.postgres.allow-destructive-schema-reset=true")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(JobStore.class)).isInstanceOf(PostgresJobStore.class);
                });

        new MigrationRunner(dataSource).validate();
    }

    private static boolean tableExists(String table) throws Exception {
        try (var conn = dataSource.getConnection();
                var ps = conn.prepareStatement("SELECT to_regclass(?)")) {
            ps.setString(1, table);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getString(1) != null;
            }
        }
    }
}
