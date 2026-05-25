package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.redis.RedisJobStore;
import com.hemju.threadmill.store.redis.RedisKeys;

@EnabledIf("com.hemju.threadmill.spring.DockerAvailable#check")
class SpringRedisResetAutoConfigurationTest {

    @SuppressWarnings("resource")
    private static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379)
            .withCommand("redis-server", "--appendonly", "yes")
            .waitingFor(Wait.forListeningPort());

    private static RedisURI uri;
    private static RedisClient adminClient;
    private static StatefulRedisConnection<String, String> adminConnection;

    @BeforeAll
    static void start() {
        REDIS.start();
        uri = RedisURI.create("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
        adminClient = RedisClient.create(uri);
        adminConnection = adminClient.connect();
    }

    @AfterAll
    static void stop() {
        if (adminConnection != null) adminConnection.close();
        if (adminClient != null) adminClient.shutdown();
        if (REDIS.isRunning()) REDIS.stop();
    }

    @BeforeEach
    void flush() {
        adminConnection.sync().configSet("maxmemory", "0");
        adminConnection.sync().configSet("maxmemory-policy", "noeviction");
        adminConnection.sync().flushdb();
    }

    @Test
    void resetOnStartDropsThreadmillKeysWhenExplicitlyAllowed() {
        adminConnection.sync().set(RedisKeys.PREFIX + "job:existing", "stale");
        adminConnection.sync().set("other-app:keep", "yes");

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ThreadmillAutoConfiguration.class))
                .withPropertyValues(
                        "threadmill.enabled=false",
                        "threadmill.remote-wake.enabled=false",
                        "threadmill.store.redis.uri=" + uri,
                        "threadmill.store.redis.reset-on-start=true",
                        "threadmill.store.redis.allow-destructive-reset=true")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(JobStore.class)).isInstanceOf(RedisJobStore.class);
                });

        assertThat(adminConnection.sync().keys(RedisKeys.PREFIX + "*")).isEmpty();
        assertThat(adminConnection.sync().get("other-app:keep")).isEqualTo("yes");
    }
}
