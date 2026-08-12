package com.hemju.threadmill.store.postgres;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;

/** Pool-alike test fixture whose borrowed connections start and must end with auto-commit disabled. */
final class NonAutoCommitDataSource implements DataSource {

    private final DataSource delegate;

    NonAutoCommitDataSource(DataSource delegate) {
        this.delegate = delegate;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return guarded(delegate.getConnection());
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return guarded(delegate.getConnection(username, password));
    }

    private static Connection guarded(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        return (Connection) Proxy.newProxyInstance(
                NonAutoCommitDataSource.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("close") && method.getParameterCount() == 0) {
                        SQLException modeFailure = connection.getAutoCommit()
                                ? new SQLException("borrowed connection was not restored to autoCommit=false")
                                : null;
                        try {
                            connection.close();
                        } catch (SQLException closeFailure) {
                            if (modeFailure == null) throw closeFailure;
                            modeFailure.addSuppressed(closeFailure);
                        }
                        if (modeFailure != null) throw modeFailure;
                        return null;
                    }
                    try {
                        return method.invoke(connection, arguments);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
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
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return delegate.getParentLogger();
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
