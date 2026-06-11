package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import org.springframework.mock.env.MockEnvironment;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.postgres.PostgresJobStore;
import com.hemju.threadmill.store.redis.RedisJobStore;

/**
 * Pins the store-precedence invariant after the auto-config split:
 * explicit Redis configuration beats a present DataSource, which beats the
 * in-memory fallback. The mechanism is cross-auto-config condition ordering
 * ({@code ThreadmillPostgresAutoConfiguration} gated by
 * {@link OnRedisStoreNotConfigured}), so both auto-configs and a DataSource
 * must be on the table for the test to mean anything.
 */
@EnabledIf("com.hemju.threadmill.spring.DockerAvailable#check")
class StorePrecedenceTest {

    @SuppressWarnings("resource")
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
                    DockerImageName.parse("postgres:18-alpine"))
            .withDatabaseName("threadmill")
            .withUsername("threadmill")
            .withPassword("threadmill");

    @SuppressWarnings("resource")
    private static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379)
            .withCommand("redis-server", "--appendonly", "yes")
            .waitingFor(Wait.forListeningPort());

    private static DataSource dataSource;

    @BeforeAll
    static void start() {
        POSTGRES.start();
        REDIS.start();
        var ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        dataSource = ds;
    }

    @AfterAll
    static void stop() {
        if (POSTGRES.isRunning()) POSTGRES.stop();
        if (REDIS.isRunning()) REDIS.stop();
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ThreadmillRedisAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
                .withBean(DataSource.class, () -> dataSource)
                .withPropertyValues("threadmill.enabled=false");
    }

    @Test
    void explicitRedisConfigurationBeatsAPresentDataSource() {
        runner().withPropertyValues(
                        "threadmill.store.redis.uri=redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(JobStore.class)).isInstanceOf(RedisJobStore.class);
                });
    }

    @Test
    void withoutRedisPropertiesTheDataSourceResolvesPostgres() {
        runner().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(JobStore.class)).isInstanceOf(PostgresJobStore.class);
        });
    }

    @Test
    void onRedisStoreNotConfiguredMirrorsEveryRedisPropertyShape() {
        // The Binder-based snapshot must stay in lockstep with
        // RedisProperties.isConfigured() (uri / sentinel master / cluster
        // nodes): a fourth way to configure Redis that this condition does
        // not see would silently flip precedence to Postgres.
        var condition = new OnRedisStoreNotConfigured();

        assertThat(matches(condition, new MockEnvironment())).isTrue();

        var uri = new MockEnvironment().withProperty("threadmill.store.redis.uri", "redis://localhost:6379");
        assertThat(matches(condition, uri)).isFalse();

        var sentinel = new MockEnvironment()
                .withProperty("threadmill.store.redis.sentinel.master-name", "mymaster")
                .withProperty("threadmill.store.redis.sentinel.nodes[0]", "localhost:26379");
        assertThat(matches(condition, sentinel)).isFalse();

        var cluster = new MockEnvironment().withProperty("threadmill.store.redis.cluster.nodes[0]", "localhost:7000");
        assertThat(matches(condition, cluster)).isFalse();

        var unrelated = new MockEnvironment().withProperty("threadmill.store.redis.mode", "standalone");
        assertThat(matches(condition, unrelated)).isTrue();
    }

    private static boolean matches(OnRedisStoreNotConfigured condition, MockEnvironment environment) {
        var context = new ConditionContext() {
            @Override
            public ConfigurableListableBeanFactory getBeanFactory() {
                return null;
            }

            @Override
            public Environment getEnvironment() {
                return environment;
            }

            @Override
            public ResourceLoader getResourceLoader() {
                return new DefaultResourceLoader();
            }

            @Override
            public ClassLoader getClassLoader() {
                return StorePrecedenceTest.class.getClassLoader();
            }

            @Override
            public BeanDefinitionRegistry getRegistry() {
                return null;
            }
        };
        return condition.getMatchOutcome(context, null).isMatch();
    }
}
