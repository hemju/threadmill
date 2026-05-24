package com.hemju.threadmill.store.postgres;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Unit of PostgreSQL store work that runs with one JDBC connection.
 *
 * @param <T> result type
 */
@FunctionalInterface
public interface PostgresConnectionWork<T> {

    T execute(Connection connection) throws SQLException;
}
