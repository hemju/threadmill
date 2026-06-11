package com.hemju.threadmill.store.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

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
    void runtimeExceptionFromTheDataSourceDoesNotKillTheListener() throws Exception {
        var failuresLeft = new AtomicInteger(2);
        DataSource flaky = new DelegatingDataSource(dataSource) {
            @Override
            public Connection getConnection() throws SQLException {
                if (failuresLeft.getAndDecrement() > 0) {
                    // A pooled/proxy DataSource can throw unchecked during pool
                    // transitions; the daemon must keep retrying.
                    throw new IllegalStateException("pool not started");
                }
                return super.getConnection();
            }
        };
        var listener = new PostgresRemoteWakeChannel(dataSource, flaky, "threadmill_test_wake_rt");
        var publisher = new PostgresRemoteWakeChannel(dataSource, "threadmill_test_wake_rt");
        var received = new CountDownLatch(1);
        try {
            listener.start(queue -> {
                if ("critical".equals(queue)) {
                    received.countDown();
                }
            });
            // Wait out the two failing connect attempts plus their backoff.
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (failuresLeft.get() >= 0 && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            Thread.sleep(300);

            publisher.publish("critical");
            assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            publisher.close();
            listener.close();
        }
    }

    @Test
    void closeStartCycleLeavesExactlyOneLiveListener() throws Exception {
        var listener = new PostgresRemoteWakeChannel(dataSource, "threadmill_test_wake_cycle");
        var publisher = new PostgresRemoteWakeChannel(dataSource, "threadmill_test_wake_cycle");
        var staleWakes = new AtomicInteger();
        var freshWakes = new CountDownLatch(1);
        try {
            listener.start(queue -> staleWakes.incrementAndGet());
            Thread.sleep(200);
            listener.close();

            listener.start(queue -> {
                if ("critical".equals(queue)) {
                    freshWakes.countDown();
                }
            });
            Thread.sleep(300);

            publisher.publish("critical");
            assertThat(freshWakes.await(5, TimeUnit.SECONDS)).isTrue();
            // The pre-cycle loop must not have survived the close: a stale
            // listener would still be bound to the old wake sink.
            Thread.sleep(300);
            assertThat(staleWakes.get()).isZero();
        } finally {
            publisher.close();
            listener.close();
        }
    }

    /** Minimal forwarding DataSource for fault injection. */
    private abstract static class DelegatingDataSource implements DataSource {
        private final DataSource delegate;

        DelegatingDataSource(DataSource delegate) {
            this.delegate = delegate;
        }

        @Override
        public Connection getConnection() throws SQLException {
            return delegate.getConnection();
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return delegate.getConnection(username, password);
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return delegate.getLogWriter();
        }

        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {
            delegate.setLogWriter(out);
        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {
            delegate.setLoginTimeout(seconds);
        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return delegate.getLoginTimeout();
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getLogger("test");
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return delegate.unwrap(iface);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return delegate.isWrapperFor(iface);
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
