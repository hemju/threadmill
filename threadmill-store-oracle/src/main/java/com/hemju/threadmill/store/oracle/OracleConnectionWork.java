package com.hemju.threadmill.store.oracle;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Unit of Oracle store work that runs with one JDBC connection.
 *
 * @param <T> result type
 */
@FunctionalInterface
public interface OracleConnectionWork<T> {

  /**
   * Run the work.
   *
   * @param connection the connection scoped to the active transaction boundary
   * @return the work result
   * @throws SQLException when a statement fails
   */
  T execute(Connection connection) throws SQLException;
}
