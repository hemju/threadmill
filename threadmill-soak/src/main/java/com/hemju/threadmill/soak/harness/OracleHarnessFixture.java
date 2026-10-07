package com.hemju.threadmill.soak.harness;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import oracle.jdbc.pool.OracleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.JobEngineFatalException;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.oracle.OracleJobStore;
import com.hemju.threadmill.store.oracle.OracleMigrationRunner;

/**
 * Boots a {@code gvenzl/oracle-free:23-slim-faststart} Testcontainer (or, if
 * {@code -PoracleUrl=...} is given, points at an external database with
 * {@code -PoracleUser} / {@code -PoraclePassword}). The Threadmill schema is
 * migrated and every Threadmill row deleted before the run starts so each
 * invocation begins on a clean slate. An external database's Threadmill tables
 * are emptied, so point it at a disposable schema only.
 */
public final class OracleHarnessFixture implements BackendFixture {

  /** The container image used when no external database is configured. */
  public static final String IMAGE = "gvenzl/oracle-free:23-slim-faststart";

  private static final String CONTAINER_USER = "threadmill";
  private static final String CONTAINER_PASSWORD = "threadmill";
  private static final int MAX_CONNECTIONS = 48;

  /**
   * Oracle has no {@code TRUNCATE ... CASCADE} for these foreign keys, so the
   * reset deletes children before parents.
   */
  private static final List<String> RESET_ORDER = List.of(
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
      "threadmill_queue_counts");

  private final GenericContainer<?> container;
  private final HarnessPooledDataSource dataSource;
  private final OracleJobStore store;

  /** Start a throwaway Oracle container. */
  public OracleHarnessFixture() {
    this(Optional.empty(), Optional.empty(), Optional.empty());
  }

  /**
   * Use the external database at {@code externalJdbcUrl} when present, otherwise
   * start a throwaway container.
   */
  @SuppressWarnings("resource") // the container is stopped in close()
  public OracleHarnessFixture(
      Optional<String> externalJdbcUrl, Optional<String> user, Optional<String> password) {
    if (externalJdbcUrl.isPresent()) {
      this.container = null;
      this.dataSource = pooled(externalJdbcUrl.get(), user.orElse(null), password.orElse(null));
    } else {
      this.container = new GenericContainer<>(DockerImageName.parse(IMAGE))
          .withEnv("ORACLE_PASSWORD", CONTAINER_PASSWORD)
          .withEnv("APP_USER", CONTAINER_USER)
          .withEnv("APP_USER_PASSWORD", CONTAINER_PASSWORD)
          .withExposedPorts(1521)
          .waitingFor(Wait.forLogMessage(".*DATABASE IS READY TO USE!.*\\n", 1)
              .withStartupTimeout(Duration.ofMinutes(10)));
      container.start();
      this.dataSource = pooled(jdbcUrl(container), CONTAINER_USER, CONTAINER_PASSWORD);
    }
    new OracleMigrationRunner(dataSource).migrate();
    reset();
    this.store = new OracleJobStore(dataSource);
  }

  /** The thin-driver URL of the container's pluggable database. */
  public static String jdbcUrl(GenericContainer<?> container) {
    return "jdbc:oracle:thin:@//" + container.getHost() + ":" + container.getMappedPort(1521)
        + "/FREEPDB1";
  }

  /**
   * Delete every Threadmill row and zero the per-state counters, keeping the
   * schema. Must not run while a node or producer uses the store.
   */
  public void reset() {
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      conn.setAutoCommit(true);
      for (String table : RESET_ORDER) {
        st.executeUpdate("DELETE FROM " + table);
      }
      st.executeUpdate("UPDATE threadmill_job_counts SET job_count = 0");
    } catch (SQLException e) {
      throw new JobEngineFatalException(
          "could not reset Oracle tables before run: " + e.getMessage(), e);
    }
  }

  /** The pooled data source the store runs on; closed with this fixture. */
  public DataSource dataSource() {
    return dataSource;
  }

  /** The Docker id of the owned container, or empty for an external database. */
  public Optional<String> containerId() {
    return Optional.ofNullable(container).map(GenericContainer::getContainerId);
  }

  @Override
  public JobStore store() {
    return store;
  }

  @Override
  public void close() {
    dataSource.close();
    if (container != null && container.isRunning()) container.stop();
  }

  private static HarnessPooledDataSource pooled(String url, String user, String password) {
    try {
      var ds = new OracleDataSource();
      ds.setURL(url);
      if (user != null) ds.setUser(user);
      if (password != null) ds.setPassword(password);
      return new HarnessPooledDataSource(ds, MAX_CONNECTIONS);
    } catch (SQLException e) {
      throw new JobEngineFatalException("could not create the Oracle data source", e);
    }
  }
}
