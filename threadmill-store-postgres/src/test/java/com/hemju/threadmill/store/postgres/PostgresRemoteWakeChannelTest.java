package com.hemju.threadmill.store.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@EnabledIf("com.hemju.threadmill.store.postgres.DockerAvailable#check")
class PostgresRemoteWakeChannelTest {

    @SuppressWarnings("resource")
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
                    DockerImageName.parse("postgres:18-alpine"))
            .withDatabaseName("threadmill")
            .withUsername("threadmill")
            .withPassword("threadmill");

    private static DataSource dataSource;

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

    @Test
    void publishWakesListenerWithQueueName() throws Exception {
        var listener = new PostgresRemoteWakeChannel(dataSource);
        var publisher = new PostgresRemoteWakeChannel(dataSource);
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
        var listener = new PostgresRemoteWakeChannel(dataSource, "threadmill_test_wake_a");
        var publisher = new PostgresRemoteWakeChannel(dataSource, "threadmill_test_wake_a");
        var noise = new PostgresRemoteWakeChannel(dataSource, "threadmill_test_wake_b");
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
