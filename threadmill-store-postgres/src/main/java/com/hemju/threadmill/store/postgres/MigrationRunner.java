package com.hemju.threadmill.store.postgres;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

/**
 * Minimal in-process schema migrator.
 *
 * <p>Migrations are SQL files on the classpath, named {@code V<n>__<description>.sql}.
 * They are applied in numeric order; applied versions are recorded in
 * {@code threadmill_schema_history}, which the runner creates on first use
 * (via the bootstrap migration).
 *
 * <p>This is intentionally tiny: Threadmill does not want a Flyway/Liquibase
 * dependency in production. Hosts that already use one can call
 * {@link #emitPendingSql()} to obtain the migrations as SQL and apply them
 * themselves.
 */
public final class MigrationRunner {

    private static final Pattern FILE_PATTERN = Pattern.compile("V(\\d+)__([A-Za-z0-9_]+)\\.sql");
    private static final String RESOURCE_ROOT = "com/hemju/threadmill/store/postgres/migrations/";
    private static final List<String> SHIPPED_MIGRATIONS = List.of("V1__baseline.sql");
    private static final long MIGRATION_LOCK_KEY = 0x5468726561646D6CL;
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
            "threadmill_metadata",
            "threadmill_job_counts",
            "threadmill_queue_pauses",
            "threadmill_schema_history");
    private static final List<String> THREADMILL_FUNCTIONS = List.of("threadmill_maintain_counts()");

    private final DataSource dataSource;

    public MigrationRunner(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /** Apply every pending migration. Idempotent — already-applied versions are skipped. */
    public void migrate() {
        List<Migration> all = loadAll();
        try (Connection conn = dataSource.getConnection()) {
            acquireMigrationLock(conn);
            try {
                ensureHistoryTable(conn);
                List<Integer> applied = readApplied(conn);
                for (Migration m : all) {
                    if (applied.contains(m.version())) continue;
                    applyOne(conn, m);
                }
            } finally {
                releaseMigrationLock(conn);
            }
        } catch (SQLException e) {
            throw new MigrationException("Migration failed", e);
        }
    }

    /** Return the SQL for migrations that have not yet been applied. */
    public String emitPendingSql() {
        try (Connection conn = dataSource.getConnection()) {
            ensureHistoryTable(conn);
            List<Integer> applied = readApplied(conn);
            var sb = new StringBuilder();
            for (Migration m : loadAll()) {
                if (applied.contains(m.version())) continue;
                sb.append("-- ").append(m.fileName()).append(System.lineSeparator());
                sb.append(m.sql()).append(System.lineSeparator());
                sb.append("INSERT INTO threadmill_schema_history (version, description) VALUES (")
                        .append(m.version())
                        .append(", '")
                        .append(m.description().replace("'", "''"))
                        .append("');")
                        .append(System.lineSeparator());
            }
            return sb.toString();
        } catch (SQLException e) {
            throw new MigrationException("Emit-pending-SQL failed", e);
        }
    }

    /** Return the SQL needed to install Threadmill's full schema into a clean database. */
    public String emitCleanInstallSql() {
        var sb = new StringBuilder();
        appendHistoryTableSql(sb);
        for (Migration m : loadAll()) {
            appendMigrationSql(sb, m);
        }
        return sb.toString();
    }

    /** Validate that the database has exactly the shipped Threadmill migrations applied. */
    public void validate() {
        try (Connection conn = dataSource.getConnection()) {
            if (!historyTableExists(conn)) {
                throw new MigrationException("Threadmill schema is missing: threadmill_schema_history does not exist");
            }
            List<AppliedMigration> applied = readAppliedMigrations(conn);
            List<Migration> shipped = loadAll();
            if (applied.size() != shipped.size()) {
                throw new MigrationException("Threadmill schema history has "
                        + applied.size()
                        + " migration(s), but this version ships "
                        + shipped.size()
                        + "; run migrations or repair the schema history");
            }
            Set<Integer> seen = new HashSet<>();
            for (AppliedMigration actual : applied) {
                if (!seen.add(actual.version())) {
                    throw new MigrationException(
                            "Threadmill schema history contains duplicate version " + actual.version());
                }
                Migration expected = shipped.stream()
                        .filter(m -> m.version() == actual.version())
                        .findFirst()
                        .orElseThrow(() -> new MigrationException(
                                "Threadmill schema history contains unknown version " + actual.version()));
                if (!expected.description().equals(actual.description())) {
                    throw new MigrationException("Threadmill schema history version "
                            + actual.version()
                            + " has description '"
                            + actual.description()
                            + "', expected '"
                            + expected.description()
                            + "'");
                }
            }
        } catch (SQLException e) {
            throw new MigrationException("Schema validation failed", e);
        }
    }

    /** Drop only Threadmill-owned schema objects. Intended for disposable environments. */
    public void dropThreadmillObjects() {
        try (Connection conn = dataSource.getConnection()) {
            acquireMigrationLock(conn);
            try (Statement st = conn.createStatement()) {
                for (String table : THREADMILL_TABLES) {
                    st.execute("DROP TABLE IF EXISTS " + table + " CASCADE");
                }
                for (String function : THREADMILL_FUNCTIONS) {
                    st.execute("DROP FUNCTION IF EXISTS " + function + " CASCADE");
                }
            } finally {
                releaseMigrationLock(conn);
            }
        } catch (SQLException e) {
            throw new MigrationException("Failed to drop Threadmill schema objects", e);
        }
    }

    private void applyOne(Connection conn, Migration m) throws SQLException {
        boolean priorAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try (Statement st = conn.createStatement()) {
            st.execute(m.sql());
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO threadmill_schema_history (version, description) VALUES (?, ?)")) {
                ps.setInt(1, m.version());
                ps.setString(2, m.description());
                ps.executeUpdate();
            }
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
            throw new MigrationException("Migration " + m.fileName() + " failed", e);
        } finally {
            conn.setAutoCommit(priorAutoCommit);
        }
    }

    private void ensureHistoryTable(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(historyTableSql());
        }
    }

    private void acquireMigrationLock(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT pg_advisory_lock(?)")) {
            ps.setLong(1, MIGRATION_LOCK_KEY);
            ps.execute();
        }
    }

    private void releaseMigrationLock(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT pg_advisory_unlock(?)")) {
            ps.setLong(1, MIGRATION_LOCK_KEY);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next() || !rs.getBoolean(1)) {
                    throw new SQLException("Failed to release Threadmill migration advisory lock");
                }
            }
        }
    }

    private List<Integer> readApplied(Connection conn) throws SQLException {
        List<Integer> applied = new ArrayList<>();
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT version FROM threadmill_schema_history ORDER BY version")) {
            while (rs.next()) {
                applied.add(rs.getInt(1));
            }
        }
        return applied;
    }

    private List<AppliedMigration> readAppliedMigrations(Connection conn) throws SQLException {
        List<AppliedMigration> applied = new ArrayList<>();
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT version, description FROM threadmill_schema_history ORDER BY version")) {
            while (rs.next()) {
                applied.add(new AppliedMigration(rs.getInt(1), rs.getString(2)));
            }
        }
        return applied;
    }

    private boolean historyTableExists(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                + "WHERE table_schema = current_schema() AND table_name = 'threadmill_schema_history')")) {
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    private static void appendMigrationSql(StringBuilder sb, Migration m) {
        sb.append("-- ").append(m.fileName()).append(System.lineSeparator());
        sb.append(m.sql()).append(System.lineSeparator());
        sb.append("INSERT INTO threadmill_schema_history (version, description) VALUES (")
                .append(m.version())
                .append(", '")
                .append(m.description().replace("'", "''"))
                .append("');")
                .append(System.lineSeparator());
    }

    private static void appendHistoryTableSql(StringBuilder sb) {
        sb.append(historyTableSql()).append(';').append(System.lineSeparator());
    }

    private static String historyTableSql() {
        return "CREATE TABLE IF NOT EXISTS threadmill_schema_history ("
                + "version INT PRIMARY KEY, "
                + "description TEXT NOT NULL, "
                + "applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())";
    }

    private List<Migration> loadAll() {
        List<Migration> out = new ArrayList<>();
        for (String name : SHIPPED_MIGRATIONS) {
            Matcher m = FILE_PATTERN.matcher(name);
            if (!m.matches()) {
                throw new MigrationException("Migration resource has invalid name: " + name);
            }
            int version = Integer.parseInt(m.group(1));
            String description = m.group(2).replace("_", " ");
            String sql = readResource(RESOURCE_ROOT + name);
            out.add(new Migration(version, name, description, sql));
        }
        Collections.sort(out, (a, b) -> Integer.compare(a.version(), b.version()));
        return out;
    }

    private String readResource(String path) {
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(path)) {
            if (in == null) throw new MigrationException("Migration resource not found: " + path);
            try (BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                var sb = new StringBuilder();
                String line;
                while ((line = r.readLine()) != null) {
                    sb.append(line).append('\n');
                }
                return sb.toString();
            }
        } catch (IOException e) {
            throw new MigrationException("Failed to read migration: " + path, e);
        }
    }

    private record AppliedMigration(int version, String description) {}

    private record Migration(int version, String fileName, String description, String sql) {}

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
