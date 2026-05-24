package com.hemju.threadmill.store.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

import javax.sql.DataSource;

final class OwningPostgresTransactionBoundary implements PostgresTransactionBoundary {

    private final DataSource dataSource;

    OwningPostgresTransactionBoundary(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public <T> T inTransaction(PostgresConnectionWork<T> work) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            boolean previousAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                T result = work.execute(conn);
                conn.commit();
                return result;
            } catch (RuntimeException | SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(previousAutoCommit);
            }
        }
    }
}
