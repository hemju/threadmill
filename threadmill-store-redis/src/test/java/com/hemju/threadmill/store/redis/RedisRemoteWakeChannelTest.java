package com.hemju.threadmill.store.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.lettuce.core.RedisURI;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

class RedisRemoteWakeChannelTest {

    @SuppressWarnings("resource")
    private static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379)
            .withCommand("redis-server", "--appendonly", "yes")
            .waitingFor(Wait.forListeningPort());

    private static RedisURI uri;

    @BeforeAll
    static void start() {
        REDIS.start();
        uri = RedisURI.create("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
    }

    @AfterAll
    static void stop() {
        if (REDIS.isRunning()) REDIS.stop();
    }

    @Test
    void publishWakesListenerWithQueueName() throws Exception {
        var listener = new RedisRemoteWakeChannel(uri);
        var publisher = new RedisRemoteWakeChannel(uri);
        var received = new CountDownLatch(1);
        try {
            listener.start(queue -> {
                if ("critical".equals(queue)) {
                    received.countDown();
                }
            });
            Thread.sleep(200);

            publisher.publish("critical");

            assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            publisher.close();
            listener.close();
        }
    }

    @Test
    void customChannelIsIsolated() throws Exception {
        var listener = new RedisRemoteWakeChannel(uri, "{threadmill}:wake:a");
        var publisher = new RedisRemoteWakeChannel(uri, "{threadmill}:wake:a");
        var noise = new RedisRemoteWakeChannel(uri, "{threadmill}:wake:b");
        var received = new CountDownLatch(1);
        try {
            listener.start(queue -> {
                if ("critical".equals(queue)) {
                    received.countDown();
                }
            });
            Thread.sleep(200);

            noise.publish("critical");
            assertThat(received.await(300, TimeUnit.MILLISECONDS)).isFalse();

            publisher.publish("critical");
            assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            noise.close();
            publisher.close();
            listener.close();
        }
    }
}
