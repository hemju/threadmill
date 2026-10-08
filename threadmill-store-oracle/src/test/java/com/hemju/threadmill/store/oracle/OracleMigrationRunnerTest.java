package com.hemju.threadmill.store.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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

  /** Checksum of {@code V1__baseline.sql} as shipped in v1.1.0 (frozen test resource). */
  private static final String RELEASED_BASELINE_CHECKSUM =
      "e95c99fc412a90e037ef8dabcccdbc3005820ccc90aa8d539fdbee8bca95e463";

  /** The 1-based v1.1.0 baseline statement that fails under {@code MAX_STRING_SIZE=EXTENDED}. */
  private static final int EXTENDED_FAILURE_STATEMENT = 28;

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

  /**
   * v1.1.0 declared {@code idle_sort RAW(2000)}, which fails with ORA-12899
   * under {@code MAX_STRING_SIZE=EXTENDED} at statement 28. The corrected
   * baseline must leave every earlier statement byte-identical so a schema
   * stopped there resumes with the corrected statement.
   */
  @Test
  void releasedBaselineDiffersOnlyInTheStatementThatFailsUnderExtendedStringSize()
      throws IOException {
    var released = releasedBaselineStatements();
    var current = baselineStatements();

    assertThat(sha256(releasedBaselineSql())).isEqualTo(RELEASED_BASELINE_CHECKSUM);
    assertThat(current).hasSameSizeAs(released);
    assertThat(current.subList(0, EXTENDED_FAILURE_STATEMENT - 1))
        .isEqualTo(released.subList(0, EXTENDED_FAILURE_STATEMENT - 1));
    assertThat(released.get(EXTENDED_FAILURE_STATEMENT - 1)).contains("idle_sort RAW(2000)");
    assertThat(current.get(EXTENDED_FAILURE_STATEMENT - 1))
        .contains("idle_sort GENERATED ALWAYS AS");
    assertThat(current.subList(EXTENDED_FAILURE_STATEMENT, current.size()))
        .isEqualTo(released.subList(EXTENDED_FAILURE_STATEMENT, released.size()));
  }

  @Test
  void releasedBaselineStoppedAtTheExtendedStringSizeFailureResumesAndRecordsTheCurrentChecksum()
      throws Exception {
    runner.dropThreadmillObjects();
    var released = releasedBaselineStatements();
    // The v1.1.0 runner committed statements 1-27, then statement 28 failed.
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      conn.setAutoCommit(true);
      for (int i = 0; i < EXTENDED_FAILURE_STATEMENT - 1; i++) st.execute(released.get(i));
      st.execute(historyDdl());
      try (PreparedStatement ps = conn.prepareStatement(
          "INSERT INTO threadmill_schema_history (version, description, checksum, "
              + "statements_applied, statements_total, success) VALUES (1, 'baseline', ?, ?, ?, 0)")) {
        ps.setString(1, RELEASED_BASELINE_CHECKSUM);
        ps.setInt(2, EXTENDED_FAILURE_STATEMENT - 1);
        ps.setInt(3, released.size());
        ps.executeUpdate();
      }
    }

    runner.migrate();

    runner.validate();
    assertThat(recordedBaselineChecksum()).isEqualTo(baselineChecksum());
    var store = new OracleJobStore(dataSource);
    var job = job();
    store.insert(job);
    assertThat(store.findById(job.id())).isPresent();
  }

  @Test
  void schemaCompletedByTheReleasedBaselineStillValidatesAndMigrates() throws Exception {
    execute("UPDATE threadmill_schema_history SET checksum = '" + RELEASED_BASELINE_CHECKSUM
        + "' WHERE version = 1");

    runner.validate();
    runner.migrate();
    assertThat(runner.emitPendingSql()).isEmpty();
    assertThat(recordedBaselineChecksum()).isEqualTo(RELEASED_BASELINE_CHECKSUM);
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

  private static String releasedBaselineSql() throws IOException {
    try (var in = OracleMigrationRunnerTest.class
        .getClassLoader()
        .getResourceAsStream(
            "com/hemju/threadmill/store/oracle/released/v1.1.0/V1__baseline.sql")) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private static List<String> releasedBaselineStatements() throws IOException {
    return OracleMigrationRunner.splitStatements("V1__baseline.sql", releasedBaselineSql());
  }

  private String recordedBaselineChecksum() throws SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs =
            st.executeQuery("SELECT checksum FROM threadmill_schema_history WHERE version = 1")) {
      rs.next();
      return rs.getString(1);
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

  private static String baselineChecksum() throws IOException {
    return sha256(baselineSql());
  }

  private static String sha256(String sql) {
    try {
      var digest =
          MessageDigest.getInstance("SHA-256").digest(sql.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
