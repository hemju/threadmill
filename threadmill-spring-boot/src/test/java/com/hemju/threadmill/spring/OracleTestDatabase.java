package com.hemju.threadmill.spring;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * One Oracle Free container per test JVM for the Spring integration tests
 * (Oracle starts slowly). The Oracle store module's own suite covers 21c XE
 * and real 19c; these tests exercise the Spring wiring.
 */
final class OracleTestDatabase {

  private static final String CREDENTIAL = "threadmill";
  private static String url;
  private static HikariDataSource pooled;

  private OracleTestDatabase() {}

  @SuppressWarnings("resource") // lives for the whole test JVM; Ryuk reaps it
  static synchronized String url() {
    if (url == null) {
      var container = new GenericContainer<>(
              DockerImageName.parse("gvenzl/oracle-free:23-slim-faststart"))
          .withEnv("ORACLE_PASSWORD", CREDENTIAL)
          .withEnv("APP_USER", CREDENTIAL)
          .withEnv("APP_USER_PASSWORD", CREDENTIAL)
          .withExposedPorts(1521)
          .waitingFor(Wait.forLogMessage(".*DATABASE IS READY TO USE!.*\\n", 1)
              .withStartupTimeout(Duration.ofMinutes(10)));
      container.start();
      url = "jdbc:oracle:thin:@//" + container.getHost() + ":" + container.getMappedPort(1521)
          + "/FREEPDB1";
    }
    return url;
  }

  static String user() {
    return CREDENTIAL;
  }

  static String password() {
    return CREDENTIAL;
  }

  static synchronized DataSource dataSource() {
    if (pooled == null) {
      pooled = newPool();
    }
    return pooled;
  }

  /** A separate pool on the same database: a different {@code DataSource} identity. */
  static HikariDataSource newPool() {
    var config = new HikariConfig();
    config.setJdbcUrl(url());
    config.setUsername(user());
    config.setPassword(password());
    config.setMaximumPoolSize(8);
    return new HikariDataSource(config);
  }

  /** Delete every Threadmill row and zero the counters, keeping the schema. */
  static void reset() throws SQLException {
    try (Connection conn = dataSource().getConnection();
        Statement st = conn.createStatement()) {
      conn.setAutoCommit(true);
      for (String table : new String[] {
        "threadmill_dedup_keys",
        "threadmill_cron_task_ownership",
        "threadmill_cron_task_state",
        "threadmill_cron_tasks",
        "threadmill_jobs",
        "threadmill_concurrency_groups",
        "threadmill_concurrency_workflow_holds",
        "threadmill_queue_counts"
      }) {
        st.executeUpdate("DELETE FROM " + table);
      }
      st.executeUpdate("UPDATE threadmill_job_counts SET job_count = 0");
    }
  }
}
