package com.hemju.threadmill.store.oracle;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

import javax.sql.DataSource;

final class OwningOracleTransactionBoundary implements OracleTransactionBoundary {

  private final DataSource dataSource;

  OwningOracleTransactionBoundary(DataSource dataSource) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
  }

  @Override
  public <T> T inTransaction(OracleConnectionWork<T> work) throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      return OracleTransactions.execute(conn, work);
    }
  }

  @Override
  public boolean supportsExternalTransactions() {
    return false;
  }

  @Override
  public boolean externallyManagedTransactionActive() {
    return false;
  }
}
