package com.hemju.threadmill.store.postgres;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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

    private final DataSource publisherDataSource;
    private final DataSource listenerDataSource;
    private final String channel;
    private static final int FAILURES_BEFORE_WARN = 10;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong generation = new AtomicLong();
    private final AtomicReference<Connection> listenerConnection = new AtomicReference<>();
    private final AtomicReference<Thread> listenerThread = new AtomicReference<>();

    public PostgresRemoteWakeChannel(DataSource dataSource) {
        this(dataSource, dataSource, CHANNEL);
    }

    public PostgresRemoteWakeChannel(DataSource dataSource, String channel) {
        this(dataSource, dataSource, channel);
    }

    public PostgresRemoteWakeChannel(DataSource publisherDataSource, DataSource listenerDataSource) {
        this(publisherDataSource, listenerDataSource, CHANNEL);
    }

    public PostgresRemoteWakeChannel(DataSource publisherDataSource, DataSource listenerDataSource, String channel) {
        this.publisherDataSource = Objects.requireNonNull(publisherDataSource, "publisherDataSource");
        this.listenerDataSource = Objects.requireNonNull(listenerDataSource, "listenerDataSource");
        this.channel = normalizeChannel(channel);
    }

    @Override
    public void publish(String queue) {
        Names.requireName("queue", queue);
        try (Connection conn = publisherDataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT pg_notify(?, ?)")) {
            ps.setString(1, channel);
            ps.setString(2, queue);
            ps.execute();
        } catch (SQLException e) {
            LOG.debug("Threadmill remote wake publish failed; dispatcher polling remains the fallback", e);
        }
    }

    @Override
    public void start(Consumer<String> wakeSink) {
        Objects.requireNonNull(wakeSink, "wakeSink");
        if (!running.compareAndSet(false, true)) return;
        // Per-start generation token: getNotifications(1000) is not
        // interruptible, so a close()/start() cycle could otherwise revive a
        // stale loop whose running.get() re-reads true — leaving two live
        // listeners bound to different wake sinks.
        long startedGeneration = generation.incrementAndGet();
        Thread thread = Thread.ofPlatform()
                .name("threadmill-postgres-remote-wake")
                .daemon(true)
                .start(() -> listen(wakeSink, startedGeneration));
        listenerThread.set(thread);
    }

    @Override
    public void close() {
        running.set(false);
        generation.incrementAndGet();
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

    private void listen(Consumer<String> wakeSink, long myGeneration) {
        int consecutiveFailures = 0;
        while (live(myGeneration)) {
            Connection current = null;
            try (Connection conn = listenerDataSource.getConnection();
                    Statement st = conn.createStatement()) {
                current = conn;
                listenerConnection.set(conn);
                st.execute("LISTEN " + quoteIdentifier(channel));
                PGConnection pg = conn.unwrap(PGConnection.class);
                consecutiveFailures = 0;
                while (live(myGeneration)) {
                    PGNotification[] notifications = pg.getNotifications(1000);
                    if (notifications == null) continue;
                    for (PGNotification notification : notifications) {
                        wake(notification.getParameter(), wakeSink);
                    }
                }
            } catch (SQLException | RuntimeException e) {
                // RuntimeException too: a pooled/proxy DataSource throwing
                // IllegalStateException during pool transitions must not kill
                // the daemon silently — wake is latency-only, but the listener
                // is required to keep retrying.
                if (live(myGeneration)) {
                    consecutiveFailures++;
                    if (consecutiveFailures >= FAILURES_BEFORE_WARN) {
                        LOG.warn(
                                "PostgreSQL remote wake listener failing repeatedly ({} consecutive); "
                                        + "retrying while polling remains the fallback",
                                consecutiveFailures,
                                e);
                    } else {
                        LOG.debug("PostgreSQL remote wake listener failed; retrying while polling remains fallback", e);
                    }
                    sleepBeforeRetry();
                }
            } finally {
                listenerConnection.compareAndSet(current, null);
            }
        }
    }

    private boolean live(long myGeneration) {
        return running.get()
                && generation.get() == myGeneration
                && !Thread.currentThread().isInterrupted();
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

    private static String normalizeChannel(String channel) {
        if (channel == null || channel.isBlank()) return CHANNEL;
        String value = Names.requireName("channel", channel);
        if (value.getBytes(StandardCharsets.UTF_8).length > 63) {
            throw new IllegalArgumentException("channel must be at most 63 UTF-8 bytes for PostgreSQL LISTEN");
        }
        return value;
    }

    private static String quoteIdentifier(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }
}
