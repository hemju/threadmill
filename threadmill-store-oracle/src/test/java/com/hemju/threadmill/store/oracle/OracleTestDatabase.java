package com.hemju.threadmill.store.oracle;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Locale;
import java.util.Optional;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.store.JobStoreCapabilities;

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
 *
 * <p>{@code threadmill.oracle.textType} selects the {@link OracleTextType} the
 * schema and every store use ({@code STANDARD} by default). With
 * {@code threadmill.oracle.characterSet} (for example {@code WE8MSWIN1252}) the
 * container's pluggable database is relabelled to that database character set
 * before any test runs: {@code ALTER DATABASE CHARACTER SET INTERNAL_USE}
 * changes the declared character set without converting data, which is exact
 * for a fresh database whose dictionary holds only ASCII and never acceptable
 * for real data. Sessions then convert text exactly as on a database created
 * with that character set, so a bind that bypasses the national character set
 * loses characters and fails the round-trip tests. The gvenzl images ship only
 * {@code AL32UTF8} databases; this is how CI tests a non-Unicode one. An
 * existing database ({@code threadmill.oracle.jdbcUrl}) is never relabelled
 * and keeps its own character set, which {@link #databaseCharacterSet()} reads.
 */
final class OracleTestDatabase {

  static final String DEFAULT_IMAGE = "gvenzl/oracle-free:23-slim-faststart";

  private static final String USER = "threadmill";
  private static final String PASSWORD = "threadmill";
  private static final String SESSION_INIT =
      "ALTER SESSION SET OPTIMIZER_FEATURES_ENABLE = '19.1.0'";

  private static DataSource pooled;
  private static String databaseCharacterSet;

  private OracleTestDatabase() {}

  /** A pooled data source for the shared database, starting it on first use. */
  static synchronized DataSource dataSource() {
    if (pooled == null) {
      pooled = start();
      new OracleMigrationRunner(pooled, textType()).migrate();
    }
    return pooled;
  }

  /** The text type this test run uses for the schema and every store. */
  static OracleTextType textType() {
    return OracleTextType.valueOf(
        System.getProperty("threadmill.oracle.textType", "STANDARD").toUpperCase(Locale.ROOT));
  }

  /** The test database's character set ({@code NLS_CHARACTERSET}), as the database reports it. */
  static synchronized String databaseCharacterSet() {
    if (databaseCharacterSet == null) {
      databaseCharacterSet = readDatabaseCharacterSet(dataSource());
    }
    return databaseCharacterSet;
  }

  /** The character set to relabel a container database to, if this run asks for one. */
  private static Optional<String> requestedCharacterSet() {
    String value = System.getProperty("threadmill.oracle.characterSet");
    return value == null || value.isBlank() ? Optional.empty() : Optional.of(value);
  }

  private static String readDatabaseCharacterSet(DataSource dataSource) {
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT value FROM nls_database_parameters "
            + "WHERE parameter = 'NLS_CHARACTERSET'")) {
      rs.next();
      return rs.getString(1);
    } catch (SQLException e) {
      throw new IllegalStateException("Could not read the test database's character set", e);
    }
  }

  /** Binds application-supplied text the way this run's stores do. */
  static OracleText text() {
    return new OracleText(textType());
  }

  /** A store over {@code dataSource} for this run's text type. */
  static OracleJobStore store(DataSource dataSource) {
    return new OracleJobStore(dataSource, textType());
  }

  /** A store over {@code dataSource} for this run's text type. */
  static OracleJobStore store(
      DataSource dataSource, JobSerializer serializer, JobStoreCapabilities capabilities) {
    return new OracleJobStore(
        dataSource,
        serializer,
        capabilities,
        OracleTransactionBoundary.owning(dataSource),
        textType());
  }

  /** A migration runner over {@code dataSource} for this run's text type. */
  static OracleMigrationRunner migrationRunner(DataSource dataSource) {
    return new OracleMigrationRunner(dataSource, textType());
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
    Optional<String> relabelled = Optional.empty();
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
      relabelled = requestedCharacterSet();
      relabelled.ifPresent(set -> relabelCharacterSet(container, service, set));
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
    var dataSource = new HikariDataSource(config);
    relabelled.ifPresent(set -> {
      String reported = readDatabaseCharacterSet(dataSource);
      if (!reported.equals(set)) {
        throw new IllegalStateException(
            "The test database reports character set " + reported + " after relabelling to " + set);
      }
    });
    return dataSource;
  }

  /**
   * Declare {@code characterSet} as the pluggable database's character set.
   * Only valid on the fresh, otherwise empty test database (see the class
   * documentation).
   */
  private static void relabelCharacterSet(
      GenericContainer<?> container, String service, String characterSet) {
    if (!characterSet.matches("[A-Z0-9]+")) {
      throw new IllegalArgumentException("Not an Oracle character set name: " + characterSet);
    }
    String script = String.join(
        "\n",
        "WHENEVER SQLERROR EXIT FAILURE",
        "ALTER PLUGGABLE DATABASE " + service + " CLOSE IMMEDIATE;",
        "ALTER PLUGGABLE DATABASE " + service + " OPEN RESTRICTED;",
        "ALTER SESSION SET CONTAINER = " + service + ";",
        "ALTER DATABASE CHARACTER SET INTERNAL_USE " + characterSet + ";",
        "ALTER SESSION SET CONTAINER = CDB$ROOT;",
        "ALTER PLUGGABLE DATABASE " + service + " CLOSE IMMEDIATE;",
        "ALTER PLUGGABLE DATABASE " + service + " OPEN;",
        "EXIT");
    try {
      var result = container.execInContainer(
          "bash", "-c", "sqlplus -s / as sysdba <<'SQL'\n" + script + "\nSQL");
      if (result.getExitCode() != 0) {
        throw new IllegalStateException("Relabelling the test database to " + characterSet
            + " failed: " + result.getStdout() + result.getStderr());
      }
    } catch (IOException e) {
      throw new IllegalStateException("Could not relabel the test database", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while relabelling the test database", e);
    }
  }

  /**
   * Let the test user read executed-cursor plans ({@code DBMS_XPLAN.DISPLAY_CURSOR}
   * needs the {@code V$} views). Only the throwaway container's admin account
   * is used for this.
   */
  private static void grantPlanInspection(String url) {
    // A relabelled pluggable database was just reopened; its service may take
    // a moment to register with the listener again.
    long deadline = System.nanoTime() + Duration.ofMinutes(1).toNanos();
    while (true) {
      try (Connection admin = DriverManager.getConnection(url, "system", PASSWORD);
          Statement st = admin.createStatement()) {
        st.execute("GRANT SELECT ANY DICTIONARY TO " + USER);
        return;
      } catch (SQLException e) {
        if (System.nanoTime() > deadline) {
          throw new IllegalStateException("Could not grant plan inspection to the test user", e);
        }
      }
      try {
        Thread.sleep(1_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while waiting for the test database", e);
      }
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
