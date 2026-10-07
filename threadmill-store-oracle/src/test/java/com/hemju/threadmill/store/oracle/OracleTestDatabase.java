package com.hemju.threadmill.store.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
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
 * One Oracle database per test JVM, shared by every test class because Oracle
 * starts slowly.
 *
 * <p>By default a Testcontainers {@code gvenzl/oracle-free} container (multi-arch,
 * Oracle 23ai Free) is started. CI selects the closest free release to the
 * supported 19c baseline with {@code -Dthreadmill.oracle.image=gvenzl/oracle-xe:21-slim-faststart}.
 * A real 19c (or any other) database is used instead of a container when
 * {@code threadmill.oracle.jdbcUrl}, {@code threadmill.oracle.user} and
 * {@code threadmill.oracle.password} are set; the account's schema is reset by
 * the tests, so point it at a disposable schema only.
 *
 * <p>Every session runs with {@code OPTIMIZER_FEATURES_ENABLE='19.1.0'}, so
 * execution-plan assertions reflect the 19c optimizer on newer servers too.
 */
final class OracleTestDatabase {

  static final String DEFAULT_IMAGE = "gvenzl/oracle-free:23-slim-faststart";

  private static final String USER = "threadmill";
  private static final String PASSWORD = "threadmill";
  private static final String SESSION_INIT =
      "ALTER SESSION SET OPTIMIZER_FEATURES_ENABLE = '19.1.0'";

  private static DataSource pooled;

  private OracleTestDatabase() {}

  /** A pooled data source for the shared database, starting it on first use. */
  static synchronized DataSource dataSource() {
    if (pooled == null) {
      pooled = start();
      new OracleMigrationRunner(pooled).migrate();
    }
    return pooled;
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
        "threadmill_nodes",
        "threadmill_mutexes",
        "threadmill_leases",
        "threadmill_queue_pauses",
        "threadmill_concurrency_groups",
        "threadmill_concurrency_workflow_holds",
        "threadmill_queue_counts"
      }) {
        st.executeUpdate("DELETE FROM " + table);
      }
      st.executeUpdate("UPDATE threadmill_job_counts SET job_count = 0");
    }
  }

  @SuppressWarnings("resource") // lives for the whole test JVM; Testcontainers' Ryuk reaps it
  private static DataSource start() {
    String url = System.getProperty("threadmill.oracle.jdbcUrl");
    String user = System.getProperty("threadmill.oracle.user", USER);
    String password = System.getProperty("threadmill.oracle.password", PASSWORD);
    if (url == null || url.isBlank()) {
      String image = System.getProperty("threadmill.oracle.image", DEFAULT_IMAGE);
      var container = new GenericContainer<>(DockerImageName.parse(image))
          .withEnv("ORACLE_PASSWORD", PASSWORD)
          .withEnv("APP_USER", USER)
          .withEnv("APP_USER_PASSWORD", PASSWORD)
          .withExposedPorts(1521)
          .waitingFor(Wait.forLogMessage(".*DATABASE IS READY TO USE!.*\\n", 1)
              .withStartupTimeout(Duration.ofMinutes(10)));
      container.start();
      String service = image.contains("oracle-xe") ? "XEPDB1" : "FREEPDB1";
      url = "jdbc:oracle:thin:@//" + container.getHost() + ":" + container.getMappedPort(1521) + "/"
          + service;
      user = USER;
      password = PASSWORD;
      grantPlanInspection(url);
    }
    var config = new HikariConfig();
    config.setJdbcUrl(url);
    config.setUsername(user);
    config.setPassword(password);
    config.setMaximumPoolSize(24);
    config.setConnectionInitSql(SESSION_INIT);
    config.setPoolName("threadmill-oracle-test");
    return new HikariDataSource(config);
  }

  /**
   * Let the test user read executed-cursor plans ({@code DBMS_XPLAN.DISPLAY_CURSOR}
   * needs the {@code V$} views). Only the throwaway container's admin account
   * is used for this.
   */
  private static void grantPlanInspection(String url) {
    try (Connection admin = DriverManager.getConnection(url, "system", PASSWORD);
        Statement st = admin.createStatement()) {
      st.execute("GRANT SELECT ANY DICTIONARY TO " + USER);
    } catch (SQLException e) {
      throw new IllegalStateException("Could not grant plan inspection to the test user", e);
    }
  }

  /** Whether the test user can read executed-cursor plans. */
  static boolean canInspectPlans() {
    try (Connection conn = dataSource().getConnection();
        Statement st = conn.createStatement()) {
      st.executeQuery("SELECT COUNT(*) FROM v$session WHERE ROWNUM = 1").close();
      return true;
    } catch (SQLException e) {
      return false;
    }
  }
}
