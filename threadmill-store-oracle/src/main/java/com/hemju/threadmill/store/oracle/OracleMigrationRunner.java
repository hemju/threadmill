package com.hemju.threadmill.store.oracle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal in-process schema migrator for Oracle Database.
 *
 * <p>Migrations are SQL files on the classpath named {@code V<n>__<description>.sql}
 * and listed explicitly (no classpath scanning, for native-image friendliness).
 * Statements inside a file end with a line holding only {@code /}, the SQL*Plus
 * convention, so {@link #emitPendingSql()} output runs unchanged in SQL*Plus or
 * SQLcl for hosts whose application user has no DDL rights.
 *
 * <p>Oracle commits every DDL statement implicitly, so a migration cannot be
 * applied atomically. The runner therefore:
 * <ul>
 *   <li>serializes concurrent migrators with a row lock on
 *       {@code threadmill_schema_lock}, held on a second connection because
 *       each DDL statement would commit (and so release) a lock taken on the
 *       migrating connection;</li>
 *   <li>records progress in {@code threadmill_schema_history} after every
 *       statement, and resumes a crashed migration at the first unrecorded
 *       statement;</li>
 *   <li>tolerates an "object already exists" error only for that first resumed
 *       statement, the one statement whose DDL may have committed before the
 *       crash prevented its progress record;</li>
 *   <li>validates the description and SHA-256 checksum of every applied
 *       migration before applying anything.</li>
 * </ul>
 */
public final class OracleMigrationRunner {

  private static final Pattern FILE_PATTERN = Pattern.compile("V(\\d+)__([A-Za-z0-9_]+)\\.sql");
  private static final String RESOURCE_ROOT = "com/hemju/threadmill/store/oracle/migrations/";
  private static final List<String> SHIPPED_MIGRATIONS = List.of("V1__baseline.sql");
  private static final Logger LOG = LoggerFactory.getLogger(OracleMigrationRunner.class);
  private static final Duration LOCK_ACQUIRE_TIMEOUT = Duration.ofMinutes(5);
  private static final Duration LOCK_POLL_INTERVAL = Duration.ofMillis(250);

  /** Tables dropped by {@link #dropThreadmillObjects()}, children before parents. */
  private static final List<String> THREADMILL_TABLES = List.of(
      "threadmill_mutexes",
      "threadmill_concurrency_workflow_holds",
      "threadmill_concurrency_groups",
      "threadmill_dedup_keys",
      "threadmill_cron_task_ownership",
      "threadmill_cron_task_state",
      "threadmill_cron_tasks",
      "threadmill_jobs",
      "threadmill_nodes",
      "threadmill_leases",
      "threadmill_job_counts",
      "threadmill_queue_counts",
      "threadmill_queue_pauses",
      "threadmill_schema_history");

  /** ORA-00054: resource busy and acquire with NOWAIT specified. */
  private static final int RESOURCE_BUSY = 54;

  /** ORA-00942: table or view does not exist. */
  private static final int TABLE_MISSING = 942;

  /** ORA-00955: name is already used by an existing object. */
  private static final int NAME_IN_USE = 955;

  /**
   * Errors that prove a re-executed DDL statement already took effect before
   * a crash: the object, column, constraint, or index exists, or the dropped
   * object is already gone.
   */
  private static final Set<Integer> ALREADY_APPLIED = Set.of(
      NAME_IN_USE,
      1408, // such column list already indexed
      1418, // specified index does not exist (DROP INDEX)
      1430, // column being added already exists
      1442, // column to be modified to NOT NULL is already NOT NULL
      1451, // column to be modified to NULL cannot be modified to NULL
      2260, // table can have only one primary key
      2261, // such unique or primary key already exists
      2264, // name already used by an existing constraint
      2275, // such a referential constraint already exists
      TABLE_MISSING);

  private final DataSource dataSource;

  /**
   * Create a runner for the schema reachable through {@code dataSource}.
   *
   * @param dataSource connections whose current schema receives the Threadmill objects
   */
  public OracleMigrationRunner(DataSource dataSource) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
  }

  /** Apply every pending migration, resuming one that a crash interrupted. Idempotent. */
  public void migrate() {
    List<Migration> all = loadAll();
    try (Connection lock = dataSource.getConnection();
        Connection conn = dataSource.getConnection()) {
      bootstrapLockTable(conn);
      boolean previousLockAutoCommit = lock.getAutoCommit();
      acquireMigrationLock(lock);
      try {
        ensureHistoryTable(conn);
        Map<Integer, AppliedMigration> applied = validateAppliedMigrations(conn, all, false);
        for (Migration m : all) {
          AppliedMigration progress = applied.get(m.version());
          if (progress != null && progress.complete()) continue;
          applyOne(conn, m, progress);
        }
      } finally {
        releaseMigrationLock(lock, previousLockAutoCommit);
      }
    } catch (SQLException e) {
      throw new MigrationException("Migration failed", e);
    }
  }

  /**
   * Return the SQL for migrations that have not been applied yet, in SQL*Plus
   * format. Strictly read-only, so it works for roles without DDL rights. When
   * the history table does not exist, its DDL is prepended (never executed).
   *
   * @return the pending SQL script, empty when the schema is current
   */
  public String emitPendingSql() {
    try (Connection conn = dataSource.getConnection()) {
      var sb = new StringBuilder();
      Map<Integer, AppliedMigration> applied;
      if (tableExists(conn, "THREADMILL_SCHEMA_HISTORY")) {
        applied = readAppliedMigrations(conn);
      } else {
        appendStatement(sb, historyTableSql());
        applied = Map.of();
      }
      for (Migration m : loadAll()) {
        AppliedMigration progress = applied.get(m.version());
        if (progress != null && progress.complete()) continue;
        appendMigrationSql(sb, m, progress == null ? 0 : progress.statementsApplied());
      }
      return sb.toString();
    } catch (SQLException e) {
      throw new MigrationException("Emit-pending-SQL failed", e);
    }
  }

  /**
   * Return the SQL that installs Threadmill's full schema into an empty schema.
   *
   * @return the clean-install SQL script in SQL*Plus format
   */
  public String emitCleanInstallSql() {
    var sb = new StringBuilder();
    appendStatement(sb, historyTableSql());
    for (Migration m : loadAll()) {
      appendMigrationSql(sb, m, 0);
    }
    return sb.toString();
  }

  /** Validate that the schema has exactly the shipped migrations, each completely applied. */
  public void validate() {
    try (Connection conn = dataSource.getConnection()) {
      if (!tableExists(conn, "THREADMILL_SCHEMA_HISTORY")) {
        throw new MigrationException(
            "Threadmill schema is missing: threadmill_schema_history does not exist");
      }
      validateAppliedMigrations(conn, loadAll(), true);
    } catch (SQLException e) {
      throw new MigrationException("Schema validation failed", e);
    }
  }

  /**
   * Drop every Threadmill table (with its indexes and trigger). Destroys all
   * Threadmill data; intended for disposable environments. The small
   * {@code threadmill_schema_lock} table is kept because this call holds its lock.
   */
  public void dropThreadmillObjects() {
    try (Connection lock = dataSource.getConnection();
        Connection conn = dataSource.getConnection()) {
      bootstrapLockTable(conn);
      boolean previousLockAutoCommit = lock.getAutoCommit();
      acquireMigrationLock(lock);
      try (Statement st = conn.createStatement()) {
        for (String table : THREADMILL_TABLES) {
          try {
            st.execute("DROP TABLE " + table + " CASCADE CONSTRAINTS PURGE");
          } catch (SQLException e) {
            if (e.getErrorCode() != TABLE_MISSING) throw e;
          }
        }
      } finally {
        releaseMigrationLock(lock, previousLockAutoCommit);
      }
    } catch (SQLException e) {
      throw new MigrationException("Failed to drop Threadmill schema objects", e);
    }
  }

  // ------------------------------------------------------------------ applying

  private void applyOne(Connection conn, Migration m, AppliedMigration progress)
      throws SQLException {
    int next = progress == null ? 0 : progress.statementsApplied();
    if (progress == null) {
      OracleTransactions.execute(conn, tx -> {
        try (PreparedStatement ps = tx.prepareStatement(
            "INSERT INTO threadmill_schema_history (version, description, checksum, "
                + "statements_applied, statements_total, success) VALUES (?, ?, ?, 0, ?, 0)")) {
          ps.setInt(1, m.version());
          ps.setString(2, m.description());
          ps.setString(3, m.checksum());
          ps.setInt(4, m.statements().size());
          ps.executeUpdate();
        }
        return null;
      });
    } else {
      LOG.warn(
          "Resuming interrupted Threadmill migration {} at statement {} of {}",
          m.fileName(),
          next + 1,
          m.statements().size());
    }
    boolean resumed = progress != null;
    for (int i = next; i < m.statements().size(); i++) {
      String sql = m.statements().get(i);
      int applied = i + 1;
      boolean firstResumed = resumed && i == next;
      try {
        OracleTransactions.execute(conn, tx -> {
          // DML commits atomically with its progress record; DDL commits on
          // its own, which is why a resumed first statement may already exist.
          executeStatement(tx, sql, firstResumed);
          try (PreparedStatement ps = tx.prepareStatement(
              "UPDATE threadmill_schema_history SET statements_applied = ? WHERE version = ?")) {
            ps.setInt(1, applied);
            ps.setInt(2, m.version());
            ps.executeUpdate();
          }
          return null;
        });
      } catch (SQLException e) {
        throw new MigrationException(
            "Migration " + m.fileName() + " failed at statement " + applied + " of "
                + m.statements().size() + "; rerun to resume after fixing the cause",
            e);
      }
    }
    OracleTransactions.execute(conn, tx -> {
      try (PreparedStatement ps = tx.prepareStatement("UPDATE threadmill_schema_history "
          + "SET success = 1, applied_at = SYS_EXTRACT_UTC(SYSTIMESTAMP) WHERE version = ?")) {
        ps.setInt(1, m.version());
        ps.executeUpdate();
      }
      return null;
    });
  }

  private static void executeStatement(Connection conn, String sql, boolean tolerateApplied)
      throws SQLException {
    try (Statement st = conn.createStatement()) {
      st.setEscapeProcessing(false);
      st.execute(sql);
    } catch (SQLException e) {
      if (tolerateApplied && ALREADY_APPLIED.contains(e.getErrorCode())) {
        LOG.info("Statement already applied before the interruption ({})", e.getMessage());
        return;
      }
      throw e;
    }
  }

  // ------------------------------------------------------------------ history

  private static Map<Integer, AppliedMigration> validateAppliedMigrations(
      Connection conn, List<Migration> shipped, boolean requireComplete) throws SQLException {
    Map<Integer, AppliedMigration> applied = readAppliedMigrations(conn);
    if (requireComplete && applied.size() != shipped.size()) {
      throw new MigrationException("Threadmill schema history has " + applied.size()
          + " migration(s), but this version ships " + shipped.size()
          + "; run migrations or repair the schema history");
    }
    for (AppliedMigration actual : applied.values()) {
      Migration expected = shipped.stream()
          .filter(m -> m.version() == actual.version())
          .findFirst()
          .orElseThrow(() -> new MigrationException("Threadmill schema history contains unknown "
              + "version " + actual.version()
              + "; the schema was migrated by a newer Threadmill binary"));
      if (!expected.description().equals(actual.description())) {
        throw new MigrationException("Threadmill schema history version " + actual.version()
            + " with description '" + actual.description() + "', expected '"
            + expected.description() + "'");
      }
      if (!expected.checksum().equals(actual.checksum())) {
        throw new MigrationException("Threadmill schema history version " + actual.version()
            + " has checksum " + actual.checksum() + " but the shipped migration hashes to "
            + expected.checksum() + " — the migration file was edited after it was applied");
      }
      if (requireComplete && !actual.complete()) {
        throw new MigrationException("Threadmill migration " + expected.fileName()
            + " is incomplete (" + actual.statementsApplied() + " of "
            + expected.statements().size() + " statements); run migrations to resume it");
      }
    }
    return applied;
  }

  private static Map<Integer, AppliedMigration> readAppliedMigrations(Connection conn)
      throws SQLException {
    var applied = new HashMap<Integer, AppliedMigration>();
    try (Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT version, description, checksum, "
            + "statements_applied, success FROM threadmill_schema_history ORDER BY version")) {
      while (rs.next()) {
        applied.put(
            rs.getInt(1),
            new AppliedMigration(
                rs.getInt(1), rs.getString(2), rs.getString(3), rs.getInt(4), rs.getInt(5) == 1));
      }
    }
    return applied;
  }

  private static void ensureHistoryTable(Connection conn) throws SQLException {
    createIgnoringExisting(conn, historyTableSql());
  }

  private static String historyTableSql() {
    return "CREATE TABLE threadmill_schema_history ("
        + "version NUMBER(10) NOT NULL, "
        + "description VARCHAR2(200 CHAR) NOT NULL, "
        + "checksum VARCHAR2(64) NOT NULL, "
        + "statements_applied NUMBER(10) NOT NULL, "
        + "statements_total NUMBER(10) NOT NULL, "
        + "success NUMBER(1) NOT NULL, "
        + "applied_at TIMESTAMP(6) DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP) NOT NULL, "
        + "CONSTRAINT threadmill_schema_history_pk PRIMARY KEY (version), "
        + "CONSTRAINT threadmill_schema_history_ck CHECK (success IN (0, 1)))";
  }

  private static boolean tableExists(Connection conn, String upperCaseName) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM all_tables "
        + "WHERE owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AND table_name = ?")) {
      ps.setString(1, upperCaseName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && rs.getInt(1) > 0;
      }
    }
  }

  // ------------------------------------------------------------------ locking

  private static void bootstrapLockTable(Connection conn) throws SQLException {
    createIgnoringExisting(
        conn,
        "CREATE TABLE threadmill_schema_lock (id NUMBER(1) NOT NULL, "
            + "CONSTRAINT threadmill_schema_lock_pk PRIMARY KEY (id), "
            + "CONSTRAINT threadmill_schema_lock_ck CHECK (id = 1))");
    OracleTransactions.execute(conn, tx -> {
      try (Statement st = tx.createStatement()) {
        st.executeUpdate("INSERT INTO threadmill_schema_lock (id) "
            + "SELECT 1 FROM dual WHERE NOT EXISTS (SELECT 1 FROM threadmill_schema_lock)");
      } catch (SQLException e) {
        // A concurrent migrator inserted the row first.
        if (e.getErrorCode() != OracleJdbc.UNIQUE_VIOLATION) throw e;
      }
      return null;
    });
  }

  private static void createIgnoringExisting(Connection conn, String ddl) throws SQLException {
    try (Statement st = conn.createStatement()) {
      st.execute(ddl);
    } catch (SQLException e) {
      if (e.getErrorCode() != NAME_IN_USE) throw e;
    }
  }

  /**
   * Take the migration row lock on a dedicated connection with auto-commit off.
   * Polls with {@code NOWAIT} and a bounded timeout, so a rolling deploy where
   * another node is migrating logs a diagnostic and eventually fails with an
   * actionable message instead of hanging startup silently.
   */
  private static void acquireMigrationLock(Connection lock) throws SQLException {
    lock.setAutoCommit(false);
    long deadlineNanos = System.nanoTime() + LOCK_ACQUIRE_TIMEOUT.toNanos();
    boolean warned = false;
    while (true) {
      try (Statement st = lock.createStatement();
          ResultSet rs = st.executeQuery(
              "SELECT id FROM threadmill_schema_lock WHERE id = 1 FOR UPDATE NOWAIT")) {
        if (rs.next()) return;
        throw new MigrationException("threadmill_schema_lock has no lock row");
      } catch (SQLException e) {
        if (e.getErrorCode() != RESOURCE_BUSY) throw e;
      }
      if (System.nanoTime() >= deadlineNanos) {
        throw new MigrationException("Timed out after " + LOCK_ACQUIRE_TIMEOUT
            + " waiting for the Threadmill migration lock (row lock on threadmill_schema_lock)."
            + " Another node is likely migrating — inspect v$locked_object.");
      }
      if (!warned) {
        LOG.warn("Waiting for the Threadmill migration lock; another node is migrating.");
        warned = true;
      }
      try {
        Thread.sleep(LOCK_POLL_INTERVAL.toMillis());
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new MigrationException("Interrupted while waiting for the migration lock", ie);
      }
    }
  }

  private static void releaseMigrationLock(Connection lock, boolean previousAutoCommit) {
    // Swallow and log so an unlock failure cannot supersede the migration
    // failure that names the failing statement. Oracle releases the row lock
    // when the session ends anyway.
    try {
      lock.rollback();
      lock.setAutoCommit(previousAutoCommit);
    } catch (SQLException unlockError) {
      LOG.warn("Failed to release the Threadmill migration lock", unlockError);
    }
  }

  // ------------------------------------------------------------------ emitting

  private static void appendMigrationSql(StringBuilder sb, Migration m, int fromStatement) {
    String nl = System.lineSeparator();
    sb.append("-- ").append(m.fileName());
    if (fromStatement > 0) {
      sb.append(" (resuming at statement ").append(fromStatement + 1).append(')');
    }
    sb.append(nl);
    sb.append("-- Oracle commits each DDL statement; stop at the first error and rerun from it.")
        .append(nl);
    for (int i = fromStatement; i < m.statements().size(); i++) {
      appendStatement(sb, m.statements().get(i));
    }
    String description = m.description().replace("'", "''");
    if (fromStatement == 0) {
      appendStatement(
          sb,
          "INSERT INTO threadmill_schema_history (version, description, checksum, "
              + "statements_applied, statements_total, success) VALUES (" + m.version() + ", '"
              + description + "', '" + m.checksum() + "', " + m.statements().size() + ", "
              + m.statements().size() + ", 1)");
    } else {
      appendStatement(
          sb,
          "UPDATE threadmill_schema_history SET statements_applied = "
              + m.statements().size()
              + ", success = 1, applied_at = SYS_EXTRACT_UTC(SYSTIMESTAMP) WHERE version = "
              + m.version());
    }
    appendStatement(sb, "COMMIT");
  }

  private static void appendStatement(StringBuilder sb, String sql) {
    String nl = System.lineSeparator();
    sb.append(sql).append(nl).append('/').append(nl);
  }

  // ------------------------------------------------------------------ loading

  private List<Migration> loadAll() {
    var out = new ArrayList<Migration>();
    for (String name : SHIPPED_MIGRATIONS) {
      var matcher = FILE_PATTERN.matcher(name);
      if (!matcher.matches()) {
        throw new MigrationException("Migration resource has invalid name: " + name);
      }
      String sql = readResource(RESOURCE_ROOT + name);
      out.add(new Migration(
          Integer.parseInt(matcher.group(1)),
          name,
          matcher.group(2).replace('_', ' '),
          checksum(sql),
          splitStatements(name, sql)));
    }
    out.sort(Comparator.comparingInt(Migration::version));
    return out;
  }

  /**
   * Split a migration into statements at lines holding only {@code /}. Leading
   * comment lines are removed from each statement so a driver never has to
   * classify a statement that starts with a comment.
   */
  static List<String> splitStatements(String fileName, String sql) {
    var statements = new ArrayList<String>();
    var current = new StringBuilder();
    for (String line : sql.split("\n", -1)) {
      if (line.strip().equals("/")) {
        addStatement(statements, current);
        current.setLength(0);
      } else {
        current.append(line).append('\n');
      }
    }
    if (!stripLeadingComments(current.toString()).isBlank()) {
      throw new MigrationException(
          fileName + " has text after its last '/' terminator; end every statement with '/'");
    }
    return List.copyOf(statements);
  }

  private static void addStatement(List<String> statements, StringBuilder text) {
    String statement = stripLeadingComments(text.toString()).strip();
    if (!statement.isEmpty()) {
      statements.add(statement);
    }
  }

  private static String stripLeadingComments(String text) {
    var lines = text.split("\n", -1);
    int first = 0;
    while (first < lines.length
        && (lines[first].isBlank() || lines[first].stripLeading().startsWith("--"))) {
      first++;
    }
    return String.join("\n", List.of(lines).subList(first, lines.length));
  }

  private static String checksum(String sql) {
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(sql.getBytes(StandardCharsets.UTF_8));
      var sb = new StringBuilder(digest.length * 2);
      for (byte b : digest) {
        sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new MigrationException("SHA-256 unavailable", e);
    }
  }

  private String readResource(String path) {
    try (InputStream in = OracleMigrationRunner.class.getClassLoader().getResourceAsStream(path)) {
      if (in == null) throw new MigrationException("Migration resource not found: " + path);
      try (var reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
        var sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
          sb.append(line).append('\n');
        }
        return sb.toString();
      }
    } catch (IOException e) {
      throw new MigrationException("Failed to read migration: " + path, e);
    }
  }

  private record AppliedMigration(
      int version, String description, String checksum, int statementsApplied, boolean complete) {}

  private record Migration(
      int version, String fileName, String description, String checksum, List<String> statements) {}

  /** Thrown when a migration operation fails. */
  public static class MigrationException extends RuntimeException {
    public MigrationException(String message) {
      super(message);
    }

    public MigrationException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
