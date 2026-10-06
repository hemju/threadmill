package com.hemju.threadmill.store.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;

/**
 * Oracle migration-runner regressions. Oracle commits each DDL statement, so
 * the runner records progress per statement and must resume, serialize
 * migrators, and stay honest about incomplete or edited migrations.
 */
class OracleMigrationRunnerTest {

  private DataSource dataSource;
  private OracleMigrationRunner runner;

  @BeforeEach
  void migrated() {
    dataSource = OracleTestDatabase.dataSource();
    runner = new OracleMigrationRunner(dataSource);
    runner.migrate();
  }

  @AfterEach
  void restoreSchema() throws SQLException {
    // Leave a complete, empty schema for the other test classes.
    runner.dropThreadmillObjects();
    runner.migrate();
    OracleTestDatabase.reset();
  }

  @Test
  void migrationsAreIdempotentAndValidate() {
    runner.migrate();
    runner.validate();
    assertThat(runner.emitPendingSql()).isEmpty();
  }

  @Test
  void emitPendingSqlOnAFreshSchemaIsReadOnlyAndPrependsHistoryDdl() throws SQLException {
    runner.dropThreadmillObjects();

    String sql = runner.emitPendingSql();

    assertThat(sql)
        .contains("CREATE TABLE threadmill_schema_history")
        .contains("-- V1__baseline.sql")
        .contains("CREATE TABLE threadmill_jobs")
        .contains("INSERT INTO threadmill_schema_history");
    assertThat(tableExists("THREADMILL_SCHEMA_HISTORY")).isFalse();
    assertThat(tableExists("THREADMILL_JOBS")).isFalse();
  }

  @Test
  void emittedCleanInstallSqlAppliesToACleanSchemaAndValidates() throws SQLException {
    runner.dropThreadmillObjects();

    // Apply the emitted script the way SQL*Plus does: statement by statement.
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      conn.setAutoCommit(true);
      for (String statement : OracleMigrationRunner.splitStatements(
          "clean-install.sql", runner.emitCleanInstallSql())) {
        st.execute(statement);
      }
    }

    runner.validate();
    var store = new OracleJobStore(dataSource);
    var job = job();
    store.insert(job);
    assertThat(store.findById(job.id())).isPresent();
  }

  @Test
  void interruptedMigrationResumesAndToleratesTheUnrecordedCommittedStatement() throws Exception {
    runner.dropThreadmillObjects();
    var statements = baselineStatements();
    // Simulate a crash after statement 3 committed its DDL but before its
    // progress record: statements 1-3 exist, history says 2 were applied.
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      conn.setAutoCommit(true);
      for (int i = 0; i < 3; i++) st.execute(statements.get(i));
      st.execute(historyDdl());
      try (PreparedStatement ps = conn.prepareStatement(
          "INSERT INTO threadmill_schema_history (version, description, checksum, "
              + "statements_applied, statements_total, success) VALUES (1, 'baseline', ?, 2, ?, 0)")) {
        ps.setString(1, baselineChecksum());
        ps.setInt(2, statements.size());
        ps.executeUpdate();
      }
    }
    assertThatThrownBy(runner::validate).hasMessageContaining("incomplete");

    runner.migrate();

    runner.validate();
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM threadmill_job_counts")) {
      rs.next();
      assertThat(rs.getInt(1)).isEqualTo(9 * 16);
    }
  }

  @Test
  void migrateAndValidateRefuseAnUnknownVersion() throws SQLException {
    execute("INSERT INTO threadmill_schema_history (version, description, checksum, "
        + "statements_applied, statements_total, success) VALUES (99, 'future', 'x', 1, 1, 1)");
    try {
      assertThatThrownBy(runner::migrate).hasMessageContaining("newer Threadmill binary");
      assertThatThrownBy(runner::validate)
          .isInstanceOf(OracleMigrationRunner.MigrationException.class);
    } finally {
      execute("DELETE FROM threadmill_schema_history WHERE version = 99");
    }
  }

  @Test
  void migrateAndValidateRefuseAnEditedMigration() throws SQLException {
    execute("UPDATE threadmill_schema_history SET checksum = 'edited' WHERE version = 1");

    assertThatThrownBy(runner::validate).hasMessageContaining("edited after it was applied");
    assertThatThrownBy(runner::migrate)
        .isInstanceOf(OracleMigrationRunner.MigrationException.class);
  }

  @Test
  void concurrentCleanMigrationsAreSerialized() throws Exception {
    runner.dropThreadmillObjects();
    var start = new CountDownLatch(1);
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var futures = new ArrayList<Future<?>>();
      for (int i = 0; i < 3; i++) {
        futures.add(executor.submit(() -> {
          start.await();
          new OracleMigrationRunner(dataSource).migrate();
          return null;
        }));
      }
      start.countDown();
      for (var future : futures) future.get(5, TimeUnit.MINUTES);
    }
    runner.validate();
  }

  @Test
  void migrationCommitsWhenConnectionsDefaultToNonAutoCommit() {
    runner.dropThreadmillObjects();

    new OracleMigrationRunner(new NonAutoCommitDataSource(dataSource)).migrate();

    runner.validate();
  }

  @Test
  void integrityConstraintsRejectInvalidScalarState() {
    assertThatThrownBy(() -> execute("INSERT INTO threadmill_jobs (id, state, queue, priority, "
            + "handler_signature, current_state_at, version, body, created_at, workflow_root_id) "
            + "VALUES (SYS_GUID(), 'BOGUS', 'q', 0, 'h', SYSTIMESTAMP, 1, '{}', SYSTIMESTAMP, "
            + "SYS_GUID())"))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("THREADMILL_JOBS_STATE_CK");
    assertThatThrownBy(() -> execute("INSERT INTO threadmill_jobs (id, state, queue, priority, "
            + "handler_signature, current_state_at, version, body, created_at, workflow_root_id, "
            + "concurrency_key) VALUES (SYS_GUID(), 'ENQUEUED', 'q', 0, 'h', SYSTIMESTAMP, 1, '{}', "
            + "SYSTIMESTAMP, SYS_GUID(), 'key-without-mode')"))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("THREADMILL_JOBS_SHAPE_CK");
  }

  private static Job job() {
    return Job.builder()
        .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
        .build();
  }

  private void execute(String sql) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      conn.setAutoCommit(true);
      st.execute(sql);
    }
  }

  private boolean tableExists(String name) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT COUNT(*) FROM user_tables WHERE table_name = ?")) {
      ps.setString(1, name);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1) > 0;
      }
    }
  }

  private static String baselineSql() throws IOException {
    try (var in = OracleMigrationRunnerTest.class
        .getClassLoader()
        .getResourceAsStream("com/hemju/threadmill/store/oracle/migrations/V1__baseline.sql")) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private static List<String> baselineStatements() throws IOException {
    return OracleMigrationRunner.splitStatements("V1__baseline.sql", baselineSql());
  }

  private String historyDdl() {
    // The runner bootstraps this table; take its exact DDL from the emitted script.
    return OracleMigrationRunner.splitStatements("emitted", runner.emitCleanInstallSql())
        .getFirst();
  }

  private static String baselineChecksum() throws Exception {
    var digest =
        MessageDigest.getInstance("SHA-256").digest(baselineSql().getBytes(StandardCharsets.UTF_8));
    return HexFormat.of().formatHex(digest);
  }
}
