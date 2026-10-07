package com.hemju.threadmill.spring;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.hemju.threadmill.store.oracle.OracleConnectionWork;
import com.hemju.threadmill.store.oracle.OracleTransactionBoundary;

/**
 * Joins Oracle store writes to the caller's Spring JDBC transaction on the same
 * {@link DataSource}; outside a transaction, Threadmill owns its own.
 */
final class SpringOracleTransactionBoundary implements OracleTransactionBoundary {

  private final DataSource dataSource;
  private final OracleTransactionBoundary owningBoundary;

  SpringOracleTransactionBoundary(DataSource dataSource) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    this.owningBoundary = OracleTransactionBoundary.owning(dataSource);
  }

  @Override
  public <T> T inTransaction(OracleConnectionWork<T> work) throws SQLException {
    if (!TransactionSynchronizationManager.isActualTransactionActive()) {
      return owningBoundary.inTransaction(work);
    }
    if (!TransactionSynchronizationManager.hasResource(dataSource)) {
      throw new IllegalStateException(
          "threadmill.spring.enqueue-mode=join_transaction requires the caller's Spring transaction"
              + " to be bound to the same DataSource as Threadmill's OracleJobStore");
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
