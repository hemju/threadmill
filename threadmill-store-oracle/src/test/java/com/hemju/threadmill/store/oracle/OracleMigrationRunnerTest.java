package com.hemju.threadmill.store.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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
    runner = OracleTestDatabase.migrationRunner(dataSource);
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

    applyScript(runner.emitCleanInstallSql());

    runner.validate();
    var store = OracleTestDatabase.store(dataSource);
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
    var current = baselineStatements(OracleTextType.STANDARD);

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
    assumeStandardRun();
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
    var store = OracleTestDatabase.store(dataSource);
    var job = job();
    store.insert(job);
    assertThat(store.findById(job.id())).isPresent();
  }

  @Test
  void schemaCompletedByTheReleasedBaselineStillValidatesAndMigrates() throws Exception {
    assumeStandardRun();
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
          OracleTestDatabase.migrationRunner(dataSource).migrate();
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

    OracleTestDatabase.migrationRunner(new NonAutoCommitDataSource(dataSource)).migrate();

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

  /**
   * The text type decides exactly the columns that hold application-supplied
   * text; Threadmill's own fixed ASCII values stay {@code VARCHAR2}. A column
   * missing from the national set would silently corrupt non-Latin text on a
   * non-Unicode database.
   */
  @ParameterizedTest
  @EnumSource(OracleTextType.class)
  void textTypeDecidesTheTypeOfEveryApplicationTextColumn(OracleTextType type) throws SQLException {
    runner.dropThreadmillObjects();
    new OracleMigrationRunner(dataSource, type).migrate();

    assertThat(characterColumnTypes()).isEqualTo(expectedColumnTypes(type));
  }

  @Test
  void aSchemaInstalledForTheOtherTextTypeIsRefusedWithBothTypesNamed() {
    var other = otherThan(OracleTestDatabase.textType());
    var mismatched = new OracleMigrationRunner(dataSource, other);

    assertThatThrownBy(mismatched::validate)
        .isInstanceOf(OracleMigrationRunner.MigrationException.class)
        .hasMessageContaining("installed for the " + name(OracleTestDatabase.textType()))
        .hasMessageContaining("configured for the " + name(other));
    assertThatThrownBy(mismatched::migrate)
        .isInstanceOf(OracleMigrationRunner.MigrationException.class)
        .hasMessageContaining("installed for the");
    assertThatThrownBy(mismatched::emitPendingSql)
        .isInstanceOf(OracleMigrationRunner.MigrationException.class)
        .hasMessageContaining("installed for the");
  }

  /**
   * A DBA applying {@code emitPendingSql()} output must get the same text-type
   * check as {@code migrate()}: emitting the rest of an interrupted install of
   * the other type would leave columns of both types and stamp this type's
   * checksum, which {@code validate()} then accepts. A runner of the type the
   * install started with still resumes it.
   */
  @ParameterizedTest
  @EnumSource(OracleTextType.class)
  void emitPendingSqlRefusesToResumeAnInstallOfTheOtherTextType(OracleTextType installed)
      throws SQLException {
    runner.dropThreadmillObjects();
    var statements = baselineStatements(installed);
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      conn.setAutoCommit(true);
      for (int i = 0; i < 3; i++) st.execute(statements.get(i));
      st.execute(historyDdl());
      try (PreparedStatement ps = conn.prepareStatement(
          "INSERT INTO threadmill_schema_history (version, description, checksum, "
              + "statements_applied, statements_total, success) VALUES (1, 'baseline', ?, 3, ?, 0)")) {
        ps.setString(1, sha256(OracleMigrationRunner.migrationSql("V1__baseline.sql", installed)));
        ps.setInt(2, statements.size());
        ps.executeUpdate();
      }
    }
    var mismatched = new OracleMigrationRunner(dataSource, otherThan(installed));

    assertThatThrownBy(mismatched::emitPendingSql)
        .isInstanceOf(OracleMigrationRunner.MigrationException.class)
        .hasMessageContaining("installed for the " + name(installed))
        .hasMessageContaining("configured for the " + name(otherThan(installed)));

    var matching = new OracleMigrationRunner(dataSource, installed);
    String pending = matching.emitPendingSql();
    assertThat(pending).contains("(resuming at statement 4)");
    applyScript(pending);
    matching.validate();
    assertThat(characterColumnTypes()).isEqualTo(expectedColumnTypes(installed));
  }

  @Test
  void templatesExpandEveryTextTypeAndRejectAnUnknownOne() {
    String template = "a {{VARCHAR2(128 CHAR)}} b {{VARCHAR2(4000 BYTE)}} c {{CLOB}}";

    assertThat(OracleMigrationRunner.expand("t.sql", template, OracleTextType.STANDARD))
        .isEqualTo("a VARCHAR2(128 CHAR) b VARCHAR2(4000 BYTE) c CLOB");
    assertThat(OracleMigrationRunner.expand("t.sql", template, OracleTextType.NATIONAL))
        .isEqualTo("a NVARCHAR2(128) b NVARCHAR2(2000) c NCLOB");
    for (var type : OracleTextType.values()) {
      assertThatThrownBy(() -> OracleMigrationRunner.expand("t.sql", "x {{VARCHAR2(9)}}", type))
          .isInstanceOf(OracleMigrationRunner.MigrationException.class)
          .hasMessageContaining("{{VARCHAR2(9)}}");
      assertThat(OracleMigrationRunner.migrationSql("V1__baseline.sql", type)).doesNotContain("{{");
    }
  }

  /** Character columns that hold Threadmill's own ASCII values under both text types. */
  private static final Set<String> STANDARD_ASCII_COLUMNS = Set.of(
      "THREADMILL_JOBS.STATE",
      "THREADMILL_JOBS.CONCURRENCY_MODE",
      "THREADMILL_CRON_TASKS.TRIGGER_KIND",
      "THREADMILL_CRON_TASKS.MISSED_RUN_POLICY",
      "THREADMILL_CRON_TASKS.TIME_ZONE",
      "THREADMILL_LEASES.NAME",
      "THREADMILL_JOB_COUNTS.STATE",
      "THREADMILL_SCHEMA_HISTORY.DESCRIPTION",
      "THREADMILL_SCHEMA_HISTORY.CHECKSUM");

  /** Application-supplied text columns and their standard types. */
  private static final Map<String, String> APPLICATION_TEXT_COLUMNS = Map.ofEntries(
      Map.entry("THREADMILL_JOBS.QUEUE", "VARCHAR2"),
      Map.entry("THREADMILL_JOBS.HANDLER_SIGNATURE", "VARCHAR2"),
      Map.entry("THREADMILL_JOBS.BODY", "CLOB"),
      Map.entry("THREADMILL_JOBS.CONCURRENCY_KEY", "VARCHAR2"),
      Map.entry("THREADMILL_JOBS.UNKEYED_QUEUE", "VARCHAR2"),
      Map.entry("THREADMILL_JOBS.KEYED_QUEUE", "VARCHAR2"),
      Map.entry("THREADMILL_JOBS.KEYED_KEY", "VARCHAR2"),
      Map.entry("THREADMILL_JOBS.PENDING_KEY", "VARCHAR2"),
      Map.entry("THREADMILL_JOBS.EXCLUSIVE_KEY", "VARCHAR2"),
      Map.entry("THREADMILL_JOBS.OUTSTANDING_KEY", "VARCHAR2"),
      Map.entry("THREADMILL_JOBS.ENQUEUED_QUEUE", "VARCHAR2"),
      Map.entry("THREADMILL_CRON_TASKS.NAME", "VARCHAR2"),
      Map.entry("THREADMILL_CRON_TASKS.TRIGGER_VALUE", "VARCHAR2"),
      Map.entry("THREADMILL_CRON_TASKS.HANDLER_SIGNATURE", "VARCHAR2"),
      Map.entry("THREADMILL_CRON_TASKS.PAYLOAD_TYPE_TAG", "VARCHAR2"),
      Map.entry("THREADMILL_CRON_TASKS.PAYLOAD_SERIALIZED", "CLOB"),
      Map.entry("THREADMILL_CRON_TASKS.QUEUE", "VARCHAR2"),
      Map.entry("THREADMILL_CRON_TASK_STATE.TASK_NAME", "VARCHAR2"),
      Map.entry("THREADMILL_CRON_TASK_STATE.TIMING_FINGERPRINT", "VARCHAR2"),
      Map.entry("THREADMILL_CRON_TASK_OWNERSHIP.NAMESPACE", "VARCHAR2"),
      Map.entry("THREADMILL_CRON_TASK_OWNERSHIP.TASK_NAME", "VARCHAR2"),
      Map.entry("THREADMILL_MUTEXES.NAME", "VARCHAR2"),
      Map.entry("THREADMILL_MUTEXES.HOLDER", "VARCHAR2"),
      Map.entry("THREADMILL_DEDUP_KEYS.QUEUE", "VARCHAR2"),
      Map.entry("THREADMILL_DEDUP_KEYS.DEDUP_KEY", "VARCHAR2"),
      Map.entry("THREADMILL_CONCURRENCY_GROUPS.CONCURRENCY_KEY", "VARCHAR2"),
      Map.entry("THREADMILL_CONCURRENCY_WORKFLOW_HOLDS.CONCURRENCY_KEY", "VARCHAR2"),
      Map.entry("THREADMILL_QUEUE_PAUSES.QUEUE", "VARCHAR2"),
      Map.entry("THREADMILL_QUEUE_PAUSES.PAUSED_BY", "CLOB"),
      Map.entry("THREADMILL_QUEUE_COUNTS.QUEUE", "VARCHAR2"));

  private static Map<String, String> expectedColumnTypes(OracleTextType type) {
    var expected = new TreeMap<String, String>();
    for (String column : STANDARD_ASCII_COLUMNS) expected.put(column, "VARCHAR2");
    for (var column : APPLICATION_TEXT_COLUMNS.entrySet()) {
      boolean lob = column.getValue().equals("CLOB");
      expected.put(
          column.getKey(),
          type == OracleTextType.NATIONAL ? (lob ? "NCLOB" : "NVARCHAR2") : column.getValue());
    }
    return expected;
  }

  private static OracleTextType otherThan(OracleTextType type) {
    return type == OracleTextType.STANDARD ? OracleTextType.NATIONAL : OracleTextType.STANDARD;
  }

  private static String name(OracleTextType type) {
    return type.name().toLowerCase(Locale.ROOT);
  }

  /** Apply an emitted script the way SQL*Plus does: statement by statement. */
  private void applyScript(String script) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      conn.setAutoCommit(true);
      for (String statement : OracleMigrationRunner.splitStatements("emitted.sql", script)) {
        st.execute(statement);
      }
    }
  }

  private Map<String, String> characterColumnTypes() throws SQLException {
    var types = new TreeMap<String, String>();
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT table_name, column_name, data_type "
            + "FROM user_tab_cols WHERE table_name LIKE 'THREADMILL\\_%' ESCAPE '\\' "
            + "AND table_name <> 'THREADMILL_SCHEMA_LOCK' AND hidden_column = 'NO' "
            + "AND data_type IN ('VARCHAR2', 'CLOB', 'NVARCHAR2', 'NCLOB', 'CHAR', 'NCHAR')")) {
      while (rs.next()) types.put(rs.getString(1) + "." + rs.getString(2), rs.getString(3));
    }
    return types;
  }

  private static void assumeStandardRun() {
    assumeTrue(
        OracleTestDatabase.textType() == OracleTextType.STANDARD,
        "the released v1.1.x history exists only for the standard text type");
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

  private static String baselineSql() {
    return OracleMigrationRunner.migrationSql("V1__baseline.sql", OracleTestDatabase.textType());
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

  private static List<String> baselineStatements() {
    return OracleMigrationRunner.splitStatements("V1__baseline.sql", baselineSql());
  }

  private static List<String> baselineStatements(OracleTextType type) {
    return OracleMigrationRunner.splitStatements(
        "V1__baseline.sql", OracleMigrationRunner.migrationSql("V1__baseline.sql", type));
  }

  private String historyDdl() {
    // The runner bootstraps this table; take its exact DDL from the emitted script.
    return OracleMigrationRunner.splitStatements("emitted", runner.emitCleanInstallSql())
        .getFirst();
  }

  private static String baselineChecksum() {
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
