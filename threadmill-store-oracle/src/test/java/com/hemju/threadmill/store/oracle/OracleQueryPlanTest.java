package com.hemju.threadmill.store.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Execution-plan regressions for the store's exact SQL (package-private
 * constants and builders of {@link OracleJobStore}).
 *
 * <p>Every hot or periodic query must reach its rows through the index built
 * for it, with cost bounded by the page size, the number of keys, or a key's
 * first entries — never by backlog depth. Each statement runs for real with
 * typed binds (bind types change plans: a {@code RAW} column compared with a
 * character bind is converted and can no longer use its index), and the test
 * reads the plan of the executed cursor. The schema is seeded with tens of
 * thousands of mixed-state rows, including one hot key, and real statistics.
 * Sessions run with the 19c optimizer feature set (see {@link OracleTestDatabase}).
 */
class OracleQueryPlanTest {

  private static final int ROWS = 40_000;

  @BeforeAll
  static void seed() throws SQLException {
    assumeTrue(
        OracleTestDatabase.canInspectPlans(),
        "plan tests need SELECT ANY DICTIONARY (or V$SESSION/V$SQL/V$SQL_PLAN access)");
    OracleTestDatabase.reset();
    try (Connection conn = OracleTestDatabase.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      conn.setAutoCommit(true);
      st.executeUpdate("INSERT INTO threadmill_jobs (id, state, queue, priority, "
          + "handler_signature, current_state_at, version, body, created_at, concurrency_key, "
          + "concurrency_mode, workflow_root_id, scheduled_at, owner_node_id, owner_heartbeat_at, "
          + "parent_job_id) "
          + "SELECT SYS_GUID(), CASE MOD(LEVEL, 10) WHEN 0 THEN 'SUCCEEDED' WHEN 1 THEN "
          + "'SCHEDULED' WHEN 2 THEN 'PROCESSING' WHEN 3 THEN 'AWAITING' WHEN 4 THEN 'FAILED' "
          + "ELSE 'ENQUEUED' END, 'q' || MOD(LEVEL, 2), MOD(LEVEL, 5), 'com.example.H', "
          + "SYS_EXTRACT_UTC(SYSTIMESTAMP) - NUMTODSINTERVAL(LEVEL, 'SECOND'), 1, '{}', "
          + "SYS_EXTRACT_UTC(SYSTIMESTAMP), "
          + "CASE WHEN MOD(LEVEL, 3) = 0 THEN CASE WHEN MOD(LEVEL, 2) = 0 THEN 'hot' "
          + "ELSE 'k' || MOD(LEVEL, 300) END END, "
          + "CASE WHEN MOD(LEVEL, 3) = 0 THEN CASE WHEN MOD(LEVEL, 4) = 0 THEN 'EXCLUSIVE' "
          + "ELSE 'SHARED' END END, SYS_GUID(), "
          + "CASE WHEN MOD(LEVEL, 10) = 1 THEN SYS_EXTRACT_UTC(SYSTIMESTAMP) "
          + "+ NUMTODSINTERVAL(LEVEL, 'SECOND') END, "
          + "CASE WHEN MOD(LEVEL, 10) = 2 THEN SYS_GUID() END, "
          + "CASE WHEN MOD(LEVEL, 10) = 2 THEN SYS_EXTRACT_UTC(SYSTIMESTAMP) END, "
          + "CASE WHEN MOD(LEVEL, 10) = 3 THEN SYS_GUID() END "
          + "FROM dual CONNECT BY LEVEL <= " + ROWS);
      st.executeUpdate("INSERT INTO threadmill_concurrency_groups (concurrency_key, "
          + "last_modified) SELECT 'g' || LEVEL, SYS_EXTRACT_UTC(SYSTIMESTAMP) - INTERVAL '1' HOUR "
          + "FROM dual CONNECT BY LEVEL <= 2000");
      st.execute("BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, 'THREADMILL_JOBS', cascade => TRUE); "
          + "DBMS_STATS.GATHER_TABLE_STATS(USER, 'THREADMILL_CONCURRENCY_GROUPS', "
          + "cascade => TRUE); END;");
    }
  }

  @AfterAll
  static void clear() throws SQLException {
    OracleTestDatabase.reset();
  }

  @Test
  void unkeyedClaimWalksItsIndexInOrderWithoutSorting() throws SQLException {
    assertThat(executedPlan(OracleJobStore.UNKEYED_CLAIM_SQL, ps -> ps.setString(1, "q1")))
        .contains("THREADMILL_JOBS_UNKEYED_IDX")
        .doesNotContain("SORT ORDER BY")
        .doesNotContain("TABLE ACCESS FULL");
  }

  @Test
  void keyEnumerationIsOneMinProbePerKey() throws SQLException {
    assertThat(executedPlan(OracleJobStore.pendingKeysSql(false), ps -> {
          ps.setString(1, "q1");
          ps.setString(2, "q1");
          ps.setInt(3, OracleJobStore.MAX_PENDING_KEYS_PER_PASS);
        }))
        .contains("INDEX RANGE SCAN (MIN/MAX)")
        .contains("THREADMILL_JOBS_KEYED_IDX")
        .doesNotContain("FAST FULL SCAN")
        .doesNotContain("TABLE ACCESS FULL");
    assertThat(executedPlan(OracleJobStore.pendingKeysSql(true), ps -> {
          ps.setString(1, "q1");
          ps.setString(2, "k1");
          ps.setString(3, "q1");
          ps.setInt(4, OracleJobStore.MAX_PENDING_KEYS_PER_PASS);
        }))
        .contains("INDEX RANGE SCAN (MIN/MAX)")
        .doesNotContain("FAST FULL SCAN")
        .doesNotContain("TABLE ACCESS FULL");
  }

  @Test
  void keyedHeadsStopAfterEachKeysFirstRowsEvenOnAHotKey() throws SQLException {
    assertThat(executedPlan(OracleJobStore.keyedHeadsSql(4), ps -> {
          int parameter = 1;
          for (String key : List.of("hot", "k3", "k9", "k15")) {
            ps.setString(parameter++, "q1");
            ps.setString(parameter++, key);
            ps.setInt(parameter++, 10);
          }
        }))
        .contains("THREADMILL_JOBS_KEYED_IDX")
        .contains("COUNT STOPKEY")
        .doesNotContain("SORT ORDER BY")
        .doesNotContain("WINDOW SORT")
        .doesNotContain("TABLE ACCESS FULL");
  }

  @Test
  void admissionProbesUseThePendingIndexes() throws SQLException {
    var ids = ids("SELECT id FROM threadmill_jobs WHERE state = 'ENQUEUED' "
        + "AND concurrency_key IS NOT NULL AND ROWNUM <= 8");
    assertThat(executedPlan(OracleJobStore.admissionSql(8), ps -> bindRaw(ps, 1, ids)))
        .contains("THREADMILL_JOBS_PENDING_IDX")
        .contains("THREADMILL_JOBS_EXCLUSIVE_IDX")
        .contains("THREADMILL_JOBS_PK")
        .doesNotContain("TABLE ACCESS FULL");
  }

  @Test
  void candidateLockingProbesThePrimaryKey() throws SQLException {
    var ids = ids("SELECT id FROM threadmill_jobs WHERE state = 'ENQUEUED' AND ROWNUM <= 8");
    assertThat(executedPlan(OracleJobStore.lockByIdsSql(8), ps -> bindRaw(ps, 1, ids)))
        .contains("THREADMILL_JOBS_PK")
        .doesNotContain("THREADMILL_JOBS_STATE_ID_IDX")
        .doesNotContain("TABLE ACCESS FULL");
  }

  @Test
  void holdMembersUseTheWorkflowIndex() throws SQLException {
    var roots = ids("SELECT workflow_root_id FROM threadmill_jobs WHERE state = 'ENQUEUED' "
        + "AND concurrency_key = 'hot' AND ROWNUM <= 4");
    assertThat(executedPlan(OracleJobStore.holdMembersSql(4), ps -> {
          int parameter = 1;
          for (byte[] root : roots) {
            ps.setString(parameter++, "hot");
            ps.setBytes(parameter++, root);
            ps.setString(parameter++, "q1");
            ps.setInt(parameter++, 10);
          }
        }))
        .contains("THREADMILL_JOBS_WORKFLOW_IDX")
        .doesNotContain("TABLE ACCESS FULL");
  }

  @Test
  void promotionAndOrphanScansReadTheirIndexesInOrder() throws SQLException {
    var now = Instant.now();
    assertThat(executedPlan(OracleJobStore.PROMOTION_SQL, ps -> {
          ps.setObject(1, utc(now.plusSeconds(ROWS)));
          ps.setInt(2, 500);
        }))
        .contains("THREADMILL_JOBS_SCHEDULED_IDX")
        .contains("COUNT STOPKEY")
        .doesNotContain("SORT ORDER BY")
        .doesNotContain("TABLE ACCESS FULL");
    assertThat(executedPlan(OracleJobStore.ORPHAN_SQL, ps -> {
          ps.setObject(1, utc(now));
          ps.setInt(2, 500);
        }))
        .contains("THREADMILL_JOBS_LIVENESS_IDX")
        .doesNotContain("SORT ORDER BY")
        .doesNotContain("TABLE ACCESS FULL");
  }

  @Test
  void workflowAndQueueMonitoringReadTheirIndexesInOrder() throws SQLException {
    var parents = ids(
        "SELECT parent_job_id FROM threadmill_jobs WHERE state = 'AWAITING' " + "AND ROWNUM <= 1");
    assertThat(executedPlan(OracleJobStore.AWAITING_SQL, ps -> {
          ps.setBytes(1, parents.getFirst());
          ps.setInt(2, 500);
        }))
        .contains("THREADMILL_JOBS_AWAITING_IDX")
        .doesNotContain("SORT ORDER BY")
        .doesNotContain("TABLE ACCESS FULL");
    assertThat(executedPlan(OracleJobStore.QUEUE_AGE_SQL, ps -> ps.setString(1, "q1")))
        .contains("INDEX RANGE SCAN (MIN/MAX)")
        .contains("THREADMILL_JOBS_QUEUE_AGE_IDX");
    assertThat(executedPlan(OracleJobStore.OLDEST_SCHEDULED_SQL, ps -> {}))
        .contains("(MIN/MAX)")
        .contains("THREADMILL_JOBS_SCHEDULED_IDX");
    assertThat(executedPlan(OracleJobStore.OLDEST_HEARTBEAT_SQL, ps -> {}))
        .contains("(MIN/MAX)")
        .contains("THREADMILL_JOBS_HEARTBEAT_IDX");
    assertThat(executedPlan(OracleJobStore.OLDEST_IN_STATE_SQL, ps -> ps.setString(1, "FAILED")))
        .contains("(MIN/MAX)")
        .doesNotContain("TABLE ACCESS FULL");
  }

  @Test
  void retentionAndIdleGroupPagesReadTheirIndexesInOrder() throws SQLException {
    var cutoff = utc(Instant.now());
    assertThat(executedPlan(OracleJobStore.retentionCandidatesSql(true, false), ps -> {
          ps.setString(1, "FAILED");
          ps.setObject(2, cutoff);
        }))
        .contains("THREADMILL_JOBS_RETENTION_IDX")
        .doesNotContain("SORT ORDER BY")
        .doesNotContain("TABLE ACCESS FULL");
    var cursorId = ids("SELECT id FROM threadmill_jobs WHERE state = 'FAILED' AND ROWNUM <= 1");
    assertThat(executedPlan(OracleJobStore.retentionCandidatesSql(true, true), ps -> {
          ps.setString(1, "FAILED");
          ps.setObject(2, cutoff);
          ps.setObject(3, utc(Instant.now().minusSeconds(ROWS / 2)));
          ps.setObject(4, utc(Instant.now().minusSeconds(ROWS / 2)));
          ps.setBytes(5, cursorId.getFirst());
        }))
        .contains("THREADMILL_JOBS_RETENTION_IDX")
        .doesNotContain("SORT ORDER BY")
        .doesNotContain("TABLE ACCESS FULL");
    assertThat(executedPlan(OracleJobStore.idleGroupsSql(false), ps -> {}))
        .contains("THREADMILL_CONCURRENCY_IDLE_IDX")
        .doesNotContain("SORT ORDER BY");
    assertThat(executedPlan(OracleJobStore.idleGroupsSql(true), ps -> ps.setString(1, "g5")))
        .contains("THREADMILL_CONCURRENCY_IDLE_IDX")
        .doesNotContain("SORT ORDER BY");
  }

  @Test
  void countsReadTheCounterTablesNeverTheJobsTable() throws SQLException {
    assertThat(executedPlan(OracleJobStore.COUNTS_SQL, ps -> {})).doesNotContain("THREADMILL_JOBS");
    assertThat(executedPlan(OracleJobStore.QUEUE_DEPTHS_SQL, ps -> {}))
        .doesNotContain("THREADMILL_JOBS");
  }

  @FunctionalInterface
  private interface Binder {
    void bind(PreparedStatement ps) throws SQLException;
  }

  /**
   * Run {@code sql} with typed binds and return the plan of the executed cursor.
   *
   * <p>A unique trailing comment gives the probe its own cursor, which is then
   * found in {@code V$SQL} by that tag. Relying on the session's previous
   * statement ({@code DISPLAY_CURSOR(NULL, NULL)}) returned no plan on 21c
   * XE. The comment does not change optimization: hints sit at the start.
   */
  private static String executedPlan(String sql, Binder binder) throws SQLException {
    String tag = "threadmill-plan-probe-" + UUID.randomUUID();
    try (Connection conn = OracleTestDatabase.dataSource().getConnection()) {
      conn.setAutoCommit(false);
      try {
        try (PreparedStatement ps = conn.prepareStatement(sql + " /* " + tag + " */")) {
          binder.bind(ps);
          try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
              // drain, so the cursor completes like the store's reads
            }
          }
        }
        String sqlId;
        int child;
        try (PreparedStatement ps = conn.prepareStatement("SELECT sql_id, child_number FROM v$sql "
            + "WHERE sql_fulltext LIKE ? AND sql_fulltext NOT LIKE '%v$sql%' "
            + "ORDER BY last_active_time DESC FETCH FIRST 1 ROW ONLY")) {
          ps.setString(1, "%" + tag + "%");
          try (ResultSet rs = ps.executeQuery()) {
            assertThat(rs.next()).as("cursor for %s in V$SQL", tag).isTrue();
            sqlId = rs.getString(1);
            child = rs.getInt(2);
          }
        }
        var plan = new StringBuilder();
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT plan_table_output FROM TABLE(" + "DBMS_XPLAN.DISPLAY_CURSOR(?, ?, 'BASIC'))")) {
          ps.setString(1, sqlId);
          ps.setInt(2, child);
          try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) plan.append(rs.getString(1)).append('\n');
          }
        }
        assertThat(plan.toString()).as("executed-cursor plan").contains("Plan hash value");
        return plan.toString();
      } finally {
        conn.rollback();
      }
    }
  }

  private static List<byte[]> ids(String sql) throws SQLException {
    var ids = new ArrayList<byte[]>();
    try (Connection conn = OracleTestDatabase.dataSource().getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(sql)) {
      while (rs.next()) ids.add(rs.getBytes(1));
    }
    assertThat(ids).as("seed rows for %s", sql).isNotEmpty();
    return ids;
  }

  private static void bindRaw(PreparedStatement ps, int start, List<byte[]> ids)
      throws SQLException {
    for (int i = 0; i < ids.size(); i++) ps.setBytes(start + i, ids.get(i));
  }

  private static LocalDateTime utc(Instant instant) {
    return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
  }
}
