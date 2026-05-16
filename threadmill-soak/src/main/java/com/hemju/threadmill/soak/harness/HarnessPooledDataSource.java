package com.hemju.threadmill.soak.harness;

import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import javax.sql.DataSource;

/**
 * Tiny fixed-size connection pool for the load harness.
 *
 * <p>The production store deliberately accepts a host-owned {@link DataSource}
 * and does not create a pool. The soak harness still needs to behave like a
 * production host: one physical connection per operation would measure TCP
 * churn more than Threadmill.
 */
final class HarnessPooledDataSource implements DataSource, AutoCloseable {

    private final DataSource delegate;
    private final ArrayBlockingQueue<Connection> idle;
    private final int maxConnections;
    private final AtomicInteger created = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile PrintWriter logWriter;
    private volatile int loginTimeoutSeconds;

    HarnessPooledDataSource(DataSource delegate, int maxConnections) {
        if (maxConnections <= 0) {
            throw new IllegalArgumentException("maxConnections must be positive");
        }
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.maxConnections = maxConnections;
        this.idle = new ArrayBlockingQueue<>(maxConnections);
    }

    @Override
    public Connection getConnection() throws SQLException {
        ensureOpen();
        Connection physical;
        while ((physical = idle.poll()) != null) {
            if (!physical.isClosed()) {
                return pooledConnection(physical);
            }
            created.decrementAndGet();
        }

        while (true) {
            int current = created.get();
            if (current < maxConnections && created.compareAndSet(current, current + 1)) {
                try {
                    return pooledConnection(delegate.getConnection());
                } catch (SQLException e) {
                    created.decrementAndGet();
                    throw e;
                }
            }
            try {
                physical = idle.take();
                if (!physical.isClosed()) {
                    return pooledConnection(physical);
                }
                created.decrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("interrupted while waiting for a pooled Postgres connection", e);
            }
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        throw new SQLFeatureNotSupportedException("credential-specific pooled connections are not supported");
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return logWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        this.logWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        this.loginTimeoutSeconds = seconds;
        delegate.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeoutSeconds;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("parent logger is not supported");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this) || delegate.isWrapperFor(iface);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        Connection physical;
        while ((physical = idle.poll()) != null) {
            closePhysical(physical);
        }
    }

    private void ensureOpen() throws SQLException {
        if (closed.get()) {
            throw new SQLException("pooled Postgres datasource is closed");
        }
    }

    private Connection pooledConnection(Connection physical) {
        InvocationHandler handler = new PooledConnectionHandler(physical);
        return (Connection)
                Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, handler);
    }

    private void release(Connection physical) throws SQLException {
        if (closed.get() || physical.isClosed()) {
            closePhysical(physical);
            created.decrementAndGet();
            return;
        }
        reset(physical);
        if (!idle.offer(physical)) {
            closePhysical(physical);
            created.decrementAndGet();
        }
    }

    private static void reset(Connection physical) throws SQLException {
        if (!physical.getAutoCommit()) {
            physical.rollback();
            physical.setAutoCommit(true);
        }
        if (physical.isReadOnly()) {
            physical.setReadOnly(false);
        }
    }

    private static void closePhysical(Connection physical) {
        try {
            physical.close();
        } catch (SQLException ignored) {
        }
    }

    private final class PooledConnectionHandler implements InvocationHandler {

        private final Connection physical;
        private boolean logicalClosed;

        private PooledConnectionHandler(Connection physical) {
            this.physical = physical;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            if (name.equals("close") && method.getParameterCount() == 0) {
                if (!logicalClosed) {
                    logicalClosed = true;
                    release(physical);
                }
                return null;
            }
            if (name.equals("isClosed") && method.getParameterCount() == 0) {
                return logicalClosed || physical.isClosed();
            }
            if (logicalClosed) {
                throw new SQLException("connection is closed");
            }
            try {
                return method.invoke(physical, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }
    }
}
