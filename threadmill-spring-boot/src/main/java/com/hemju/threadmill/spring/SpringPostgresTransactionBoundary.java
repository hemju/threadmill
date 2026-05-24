package com.hemju.threadmill.spring;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.hemju.threadmill.store.postgres.PostgresConnectionWork;
import com.hemju.threadmill.store.postgres.PostgresTransactionBoundary;

final class SpringPostgresTransactionBoundary implements PostgresTransactionBoundary {

    private final DataSource dataSource;
    private final PostgresTransactionBoundary owningBoundary;

    SpringPostgresTransactionBoundary(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.owningBoundary = PostgresTransactionBoundary.owning(dataSource);
    }

    @Override
    public <T> T inTransaction(PostgresConnectionWork<T> work) throws SQLException {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            return owningBoundary.inTransaction(work);
        }
        if (!TransactionSynchronizationManager.hasResource(dataSource)) {
            throw new IllegalStateException(
                    "threadmill.spring.enqueue-mode=join_transaction requires the caller's Spring transaction"
                            + " to be bound to the same DataSource as Threadmill's PostgresJobStore");
        }
        Connection conn = DataSourceUtils.getConnection(dataSource);
        try {
            return work.execute(conn);
        } finally {
            DataSourceUtils.releaseConnection(conn, dataSource);
        }
    }

    @Override
    public boolean supportsExternalTransactions() {
        return true;
    }

    @Override
    public boolean externallyManagedTransactionActive() {
        return TransactionSynchronizationManager.isActualTransactionActive()
                && TransactionSynchronizationManager.hasResource(dataSource);
    }
}
