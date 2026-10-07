package com.hemju.threadmill.store.oracle;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Shared cleanup policy for self-owned store and migration transactions.
 *
 * <p>The connection's previous auto-commit mode is restored afterwards: a pool
 * may hand out {@code autoCommit=false} connections, and closing such a
 * connection does not commit it, so every self-owned write commits explicitly.
 */
final class OracleTransactions {
  private OracleTransactions() {}

  static <T> T execute(Connection connection, OracleConnectionWork<T> work) throws SQLException {
    boolean previousAutoCommit = connection.getAutoCommit();
    connection.setAutoCommit(false);
    Throwable failure = null;
    boolean restoreMode = true;
    try {
      var result = work.execute(connection);
      connection.commit();
      return result;
    } catch (SQLException | RuntimeException | Error original) {
      failure = original;
      try {
        connection.rollback();
      } catch (SQLException | RuntimeException | Error rollbackFailure) {
        original.addSuppressed(rollbackFailure);
        // Restoring auto-commit after an unsuccessful rollback would commit
        // the failed work. Discard this connection instead of returning it.
        restoreMode = false;
        try {
          connection.abort(Runnable::run);
        } catch (SQLException | RuntimeException | Error abortFailure) {
          original.addSuppressed(abortFailure);
        }
      }
      throw original;
    } finally {
      if (restoreMode) {
        try {
          connection.setAutoCommit(previousAutoCommit);
        } catch (SQLException | RuntimeException | Error resetFailure) {
          if (failure == null) throw resetFailure;
          failure.addSuppressed(resetFailure);
        }
      }
    }
  }
}
