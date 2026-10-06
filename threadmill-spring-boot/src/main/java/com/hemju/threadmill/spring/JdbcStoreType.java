package com.hemju.threadmill.spring;

/**
 * The JDBC store Spring Boot auto-configures from the application's
 * {@code DataSource}, set with {@code threadmill.store.jdbc-type}.
 *
 * <p>Only needed when both {@code threadmill-store-postgres} and
 * {@code threadmill-store-oracle} are on the classpath and the database cannot
 * be told from {@code spring.datasource.url}. Without it, a
 * {@code jdbc:oracle:} URL selects Oracle, a {@code jdbc:postgresql:} URL
 * selects PostgreSQL, and otherwise the one store module present is used
 * (PostgreSQL when both are).
 */
public enum JdbcStoreType {
  /** {@code PostgresJobStore} (PostgreSQL 18+). */
  POSTGRES,

  /** {@code OracleJobStore} (Oracle Database 19c+). */
  ORACLE
}
