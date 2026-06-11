package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.memory.InMemoryJobStore;
import com.hemju.threadmill.store.postgres.PostgresJobStore;

/**
 * Regression for the auto-configuration ordering against the REAL Spring
 * Boot 4 {@code DataSourceAutoConfiguration} — the most common deployment
 * shape ({@code spring.datasource.*} + the JDBC starter). Every other
 * Postgres test injects the DataSource as a user bean, which registers
 * before auto-configurations and structurally cannot see the ordering bug:
 * without an {@code @AutoConfigureAfter} edge to the (SB4-relocated)
 * DataSource auto-config, {@code ThreadmillPostgresAutoConfiguration}'s
 * registration-time {@code @ConditionalOnBean(DataSource)} evaluated before
 * the DataSource bean definition existed and silently dropped the
 * application to the in-memory store.
 */
@EnabledIf("com.hemju.threadmill.spring.DockerAvailable#check")
class SpringAutoConfigurationOrderingTest {

    @SuppressWarnings("resource")
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
                    DockerImageName.parse("postgres:18-alpine"))
            .withDatabaseName("threadmill")
            .withUsername("threadmill")
            .withPassword("threadmill");

    @BeforeAll
    static void start() {
        POSTGRES.start();
    }

    @AfterAll
    static void stop() {
        if (POSTGRES.isRunning()) POSTGRES.stop();
    }

    @Test
    void autoConfiguredDataSourceResolvesThePostgresStore() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        DataSourceAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
                .withPropertyValues(
                        "threadmill.enabled=false",
                        "spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                        "spring.datasource.username=" + POSTGRES.getUsername(),
                        "spring.datasource.password=" + POSTGRES.getPassword())
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(JobStore.class)).isInstanceOf(PostgresJobStore.class);
                });
    }

    @Test
    void withoutADataSourceTheInMemoryFallbackStillApplies() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ThreadmillRedisAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
                .withPropertyValues("threadmill.enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(JobStore.class)).isInstanceOf(InMemoryJobStore.class);
                });
    }
}
