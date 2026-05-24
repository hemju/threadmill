package com.hemju.threadmill.store.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.Names;
import com.hemju.threadmill.core.engine.RemoteWakeChannel;

/** PostgreSQL {@code LISTEN}/{@code NOTIFY} implementation of {@link RemoteWakeChannel}. */
public final class PostgresRemoteWakeChannel implements RemoteWakeChannel {

    public static final String CHANNEL = "threadmill_wake";

    private static final Logger LOG = LoggerFactory.getLogger(PostgresRemoteWakeChannel.class);

    private final DataSource dataSource;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<Connection> listenerConnection = new AtomicReference<>();
    private final AtomicReference<Thread> listenerThread = new AtomicReference<>();

    public PostgresRemoteWakeChannel(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public void publish(String queue) {
        Names.requireName("queue", queue);
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT pg_notify('" + CHANNEL + "', ?)")) {
            ps.setString(1, queue);
            ps.execute();
        } catch (SQLException e) {
            LOG.debug("Threadmill remote wake publish failed; dispatcher polling remains the fallback", e);
        }
    }

    @Override
    public void start(Consumer<String> wakeSink) {
        Objects.requireNonNull(wakeSink, "wakeSink");
        if (!running.compareAndSet(false, true)) return;
        Thread thread = Thread.ofPlatform()
                .name("threadmill-postgres-remote-wake")
                .daemon(true)
                .start(() -> listen(wakeSink));
        listenerThread.set(thread);
    }

    @Override
    public void close() {
        running.set(false);
        Connection conn = listenerConnection.getAndSet(null);
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                LOG.debug("Failed to close PostgreSQL remote wake listener connection", e);
            }
        }
        Thread thread = listenerThread.getAndSet(null);
        if (thread != null) thread.interrupt();
    }

    private void listen(Consumer<String> wakeSink) {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            Connection current = null;
            try (Connection conn = dataSource.getConnection();
                    Statement st = conn.createStatement()) {
                current = conn;
                listenerConnection.set(conn);
                st.execute("LISTEN " + CHANNEL);
                PGConnection pg = conn.unwrap(PGConnection.class);
                while (running.get() && !Thread.currentThread().isInterrupted()) {
                    PGNotification[] notifications = pg.getNotifications(1000);
                    if (notifications == null) continue;
                    for (PGNotification notification : notifications) {
                        wake(notification.getParameter(), wakeSink);
                    }
                }
            } catch (SQLException e) {
                if (running.get()) {
                    LOG.debug("PostgreSQL remote wake listener failed; retrying while polling remains fallback", e);
                    sleepBeforeRetry();
                }
            } finally {
                listenerConnection.compareAndSet(current, null);
            }
        }
    }

    private static void wake(String queue, Consumer<String> wakeSink) {
        try {
            wakeSink.accept(Names.requireName("queue", queue));
        } catch (RuntimeException e) {
            LOG.debug("Ignoring invalid PostgreSQL remote wake payload", e);
        }
    }

    private static void sleepBeforeRetry() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
