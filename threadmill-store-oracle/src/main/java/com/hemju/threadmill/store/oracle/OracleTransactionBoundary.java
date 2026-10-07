package com.hemju.threadmill.store.oracle;

import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * Strategy that supplies the JDBC transaction boundary for Oracle store writes.
 *
 * <p>The default boundary opens a connection, starts a JDBC transaction, commits
 * successful work, and rolls back failed work. Framework integrations may
 * provide a boundary that joins an already active host transaction without
 * putting framework types in this module.
 */
public interface OracleTransactionBoundary {

  /**
   * Run one store write unit with a JDBC connection scoped to the desired
   * transaction boundary.
   *
   * @param work the callback to run
   * @param <T> result type
   * @return the callback result
   * @throws SQLException when the connection or callback fails
   */
  <T> T inTransaction(OracleConnectionWork<T> work) throws SQLException;

  /**
   * Whether this boundary can join an external transaction, such as a Spring
   * transaction bound to the same {@link DataSource}.
   *
   * @return {@code true} when writes may join a caller's transaction
   */
  boolean supportsExternalTransactions();

  /**
   * Whether the current call is already inside an externally managed
   * transaction. Store-level deadlock retry is disabled in this case: the
   * caller owns the transaction, so only the caller can decide to retry it.
   *
   * @return {@code true} while a caller-owned transaction is active
   */
  boolean externallyManagedTransactionActive();

  /**
   * Return the default boundary that owns its own JDBC transactions.
   *
   * @param dataSource the connection source
   * @return a boundary that commits or rolls back every unit itself
   */
  static OracleTransactionBoundary owning(DataSource dataSource) {
    return new OwningOracleTransactionBoundary(dataSource);
  }
}
