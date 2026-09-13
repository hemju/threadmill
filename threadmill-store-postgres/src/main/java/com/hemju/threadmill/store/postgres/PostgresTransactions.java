package com.hemju.threadmill.store.postgres;

import java.sql.Connection;
import java.sql.SQLException;

/** Shared cleanup policy for self-owned store and migration transactions. */
final class PostgresTransactions {
  private PostgresTransactions() {}

  static <T> T execute(Connection connection, PostgresConnectionWork<T> work) throws SQLException {
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
        // Restoring auto-commit after an unsuccessful rollback could commit
        // failed work. Discard this connection instead of returning it dirty.
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
