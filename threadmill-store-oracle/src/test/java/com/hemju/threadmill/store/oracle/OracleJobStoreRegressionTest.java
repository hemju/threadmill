package com.hemju.threadmill.store.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.schedule.CronExpression;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStore;

/**
 * Oracle-specific regression tests. The contract suite
 * ({@link OracleJobStoreContractTest}) covers the SPI semantics; these pin the
 * Oracle-only mechanics behind them: lock-as-you-fetch claiming, plans bounded
 * by indexes rather than backlog, Oracle's empty-string-is-NULL rule, time-zone
 * independent timestamps, CLOB bodies, and counter-table exactness.
 */
class OracleJobStoreRegressionTest {

  private DataSource dataSource;

  @BeforeEach
  void reset() throws SQLException {
    dataSource = OracleTestDatabase.dataSource();
    OracleTestDatabase.reset();
  }

  private JobStore store() {
    return new OracleJobStore(dataSource);
  }

  private static Job job() {
    return Job.builder()
        .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
        .build();
  }

  private static Job keyedJob(String key, ConcurrencyMode mode, int priority) {
    return Job.builder()
        .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
        .concurrencyKey(key)
        .concurrencyMode(mode)
        .priority(priority)
        .build();
  }

  private static Job keyedJob(String key) {
    return keyedJob(key, ConcurrencyMode.EXCLUSIVE, 0);
  }

  // ---------------------------------------------------------------- claim mechanics

  @Test
  void unkeyedClaimCursorLocksOnlyTheRowsItFetches() throws Exception {
    // The claim path relies on Oracle locking SKIP LOCKED rows as they are
    // fetched (a locking query cannot carry a row limit). If the driver or
    // server locked the whole result at open time, every claimer would pin
    // the entire queue.
    var store = store();
    for (int i = 0; i < 50; i++) store.insert(job());
    try (Connection claimer = dataSource.getConnection();
        Connection observer = dataSource.getConnection()) {
      claimer.setAutoCommit(false);
      observer.setAutoCommit(false);
      try (PreparedStatement ps = claimer.prepareStatement(OracleJobStore.UNKEYED_CLAIM_SQL)) {
        ps.setString(1, "default");
        ps.setFetchSize(4);
        ps.setMaxRows(4);
        try (ResultSet rs = ps.executeQuery()) {
          int read = 0;
          while (rs.next()) read++;
          assertThat(read).isEqualTo(4);
        }
        assertThat(lockableEnqueuedRows(observer)).isEqualTo(46);
      } finally {
        claimer.rollback();
        observer.rollback();
      }
    }
  }

  private static int lockableEnqueuedRows(Connection conn) throws SQLException {
    try (Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(
            "SELECT id FROM threadmill_jobs WHERE state = 'ENQUEUED' FOR UPDATE SKIP LOCKED")) {
      int rows = 0;
      while (rs.next()) rows++;
      return rows;
    }
  }

  @Test
  void unkeyedClaimLocksOnlyANarrowPageSoConcurrentClaimersAreNotStarved() throws Exception {
    var store = store();
    var jobs = new ArrayList<Job>();
    for (int i = 0; i < 300; i++) jobs.add(job());
    store.insertAll(jobs);

    int claimers = 8;
    int perClaim = 10;
    var start = new CountDownLatch(1);
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var futures = new ArrayList<Future<List<Job>>>();
      for (int i = 0; i < claimers; i++) {
        futures.add(executor.submit(() -> {
          start.await();
          return store.claimReady(NodeId.newId(), "default", perClaim, Instant.now());
        }));
      }
      start.countDown();
      Set<UUID> seen = new HashSet<>();
      for (var future : futures) {
        var claimed = future.get(60, TimeUnit.SECONDS);
        assertThat(claimed).hasSize(perClaim);
        for (Job j : claimed) assertThat(seen.add(j.id().asUuid())).isTrue();
      }
    }
  }

  @Test
  void claimReadyIsAtomicAcrossManyConcurrentVirtualThreads() throws Exception {
    var store = store();
    int total = 200;
    var jobs = new ArrayList<Job>();
    for (int i = 0; i < total; i++) {
      jobs.add(i % 2 == 0 ? job() : keyedJob("atomic:" + (i % 20), ConcurrencyMode.SHARED, 0));
    }
    store.insertAll(jobs);
    var claimed = new HashSet<UUID>();
    var start = new CountDownLatch(1);
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var futures = new ArrayList<Future<List<Job>>>();
      for (int worker = 0; worker < 16; worker++) {
        futures.add(executor.submit(() -> {
          start.await();
          var mine = new ArrayList<Job>();
          List<Job> batch;
          do {
            batch = store.claimReady(NodeId.newId(), "default", 7, Instant.now());
            mine.addAll(batch);
          } while (!batch.isEmpty());
          return mine;
        }));
      }
      start.countDown();
      for (var future : futures) {
        for (Job j : future.get(120, TimeUnit.SECONDS)) {
          assertThat(claimed.add(j.id().asUuid()))
              .as("double claim of %s", j.id())
              .isTrue();
        }
      }
    }
    assertThat(claimed).hasSize(total);
    assertThat(store.countsByState().get(JobState.PROCESSING)).isEqualTo(total);
  }

  @Test
  void keyedClaimGathersHeadsFromEveryKeyInOneCall() {
    // Guards the per-key head gathering. A collection-driven LATERAL top-n
    // returned no rows on Oracle while an equivalent query returned all of
    // them; this pins that every key contributes its heads in one call.
    var store = store();
    var jobs = new ArrayList<Job>();
    for (int key = 0; key < 12; key++) {
      for (int i = 0; i < 3; i++) jobs.add(keyedJob("shared:" + key, ConcurrencyMode.SHARED, 0));
    }
    store.insertAll(jobs);

    assertThat(store.claimReady(NodeId.newId(), "default", jobs.size(), Instant.now()))
        .hasSize(jobs.size());
  }

  @Test
  void keyedClaimRotationReachesClaimableWorkBeyondTheFirstBlockedPage() {
    var setupStore = store();
    int blockedKeyCount = OracleJobStore.MAX_PENDING_KEYS_PER_PASS + 1;
    var blockers = new ArrayList<Job>();
    for (int i = 0; i < blockedKeyCount; i++) blockers.add(keyedJob("blocked:%04d".formatted(i)));
    setupStore.insertAll(blockers);
    assertThat(setupStore.claimReady(NodeId.newId(), "default", blockedKeyCount, Instant.now()))
        .hasSize(blockedKeyCount);

    var waiters = new ArrayList<Job>();
    for (int i = 0; i < blockedKeyCount; i++) waiters.add(keyedJob("blocked:%04d".formatted(i)));
    setupStore.insertAll(waiters);
    var laterClaimable = keyedJob("claimable:after-blocked-page");
    setupStore.insert(laterClaimable);

    // A fresh store starts at the first key: the first page holds only
    // blocked keys, the second the extra blocker plus the independent key.
    var claimingStore = store();
    assertThat(claimingStore.claimReady(NodeId.newId(), "default", 1, Instant.now()))
        .isEmpty();
    assertThat(claimingStore.claimReady(NodeId.newId(), "default", 1, Instant.now()))
        .extracting(Job::id)
        .containsExactly(laterClaimable.id());

    // The short tail clears the cursor; work at an early key is reached when
    // the next poll wraps to the beginning.
    var earlyClaimable = keyedJob("available:after-wrap", ConcurrencyMode.EXCLUSIVE, 10);
    setupStore.insert(earlyClaimable);
    assertThat(claimingStore.claimReady(NodeId.newId(), "default", 1, Instant.now()))
        .extracting(Job::id)
        .containsExactly(earlyClaimable.id());
  }

  @Test
  void drainedTailRestartsKeyedScanWithoutAnEmptyPoll() {
    var setupStore = store();
    int blockedKeyCount = OracleJobStore.MAX_PENDING_KEYS_PER_PASS + 1;
    var blockers = new ArrayList<Job>();
    for (int i = 0; i < blockedKeyCount; i++) blockers.add(keyedJob("blocked:%04d".formatted(i)));
    setupStore.insertAll(blockers);
    assertThat(setupStore.claimReady(NodeId.newId(), "default", blockedKeyCount, Instant.now()))
        .hasSize(blockedKeyCount);
    var waiters = new ArrayList<Job>();
    for (int i = 0; i < blockedKeyCount; i++) waiters.add(keyedJob("blocked:%04d".formatted(i)));
    setupStore.insertAll(waiters);

    var claimingStore = store();
    assertThat(claimingStore.claimReady(NodeId.newId(), "default", 1, Instant.now()))
        .isEmpty();

    // The only key after the first page was the look-ahead key. Drain it,
    // then add work before the saved cursor: the next tail scan is empty and
    // must restart from the beginning in the same poll.
    assertThat(setupStore.softDelete(waiters.getLast().id())).isTrue();
    var earlyClaimable = keyedJob("available:after-drained-tail", ConcurrencyMode.EXCLUSIVE, 10);
    setupStore.insert(earlyClaimable);

    assertThat(claimingStore.claimReady(NodeId.newId(), "default", 1, Instant.now()))
        .extracting(Job::id)
        .containsExactly(earlyClaimable.id());
  }

  @Test
  void insertAllWithReversedKeyOrdersDoesNotManufactureDeadlocks() throws Exception {
    var store = store();
    var keys = new ArrayList<String>();
    for (int i = 0; i < 8; i++) keys.add("lock-order:" + i);
    int rounds = 15;
    for (int round = 0; round < rounds; round++) {
      var ascending = new ArrayList<Job>();
      var descending = new ArrayList<Job>();
      for (int i = 0; i < keys.size(); i++) {
        ascending.add(keyedJob(keys.get(i)));
        descending.add(keyedJob(keys.get(keys.size() - 1 - i)));
      }
      var start = new CountDownLatch(1);
      try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
        Future<?> a = executor.submit(() -> {
          start.await();
          return store.insertAll(ascending);
        });
        Future<?> b = executor.submit(() -> {
          start.await();
          return store.insertAll(descending);
        });
        start.countDown();
        a.get(30, TimeUnit.SECONDS);
        b.get(30, TimeUnit.SECONDS);
      }
    }
    assertThat(store.countsByState().get(JobState.ENQUEUED)).isEqualTo(rounds * keys.size() * 2L);
  }

  @Test
  void unreadableBodyAtTheQueueHeadIsQuarantinedWithoutStallingTheQueue() throws SQLException {
    var store = store();
    var head = job();
    var good1 = job();
    var good2 = job();
    store.insertAll(List.of(head, good1, good2));
    corruptBody(head);

    assertThat(store.claimReady(NodeId.newId(), "default", 3, Instant.now()))
        .extracting(Job::id)
        .containsExactlyInAnyOrder(good1.id(), good2.id());
    assertThat(stateOf(head)).isEqualTo("QUARANTINED");
    assertThat(store.countsByState().get(JobState.QUARANTINED)).isEqualTo(1L);
  }

  @Test
  void quarantinedKeyedWorkflowMemberReleasesTheWorkflowHold() throws SQLException {
    var store = store();
    var root = keyedJob("project:corrupt");
    store.insert(root);
    var claimedRoot =
        store.claimReady(NodeId.newId(), "default", 1, Instant.now()).getFirst();
    var member = Job.builder()
        .spec(JobSpec.of("com.example.Member", new JobArgument("java.lang.String", "\"x\"")))
        .concurrencyKey("project:corrupt")
        .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
        .workflowRootId(claimedRoot.id())
        .build();
    store.insert(member);
    corruptBody(member);
    long rootVersion = claimedRoot.version();
    claimedRoot.transitionTo(JobState.SUCCEEDED, Instant.now(), "test", null);
    claimedRoot.clearOwner();
    store.saveAtomic(claimedRoot, rootVersion);

    assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now())).isEmpty();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM "
            + "threadmill_concurrency_workflow_holds WHERE concurrency_key = ?")) {
      ps.setString(1, "project:corrupt");
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        assertThat(rs.getInt(1)).as("hold rows left for the key").isZero();
      }
    }
    var next = keyedJob("project:corrupt");
    store.insert(next);
    assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
        .extracting(Job::id)
        .containsExactly(next.id());
  }

  private void corruptBody(Job job) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(
            "UPDATE threadmill_jobs SET body = '{not valid json' WHERE id = ?")) {
      ps.setBytes(1, OracleJdbc.bytes(job.id().asUuid()));
      ps.executeUpdate();
    }
  }

  private String stateOf(Job job) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT state FROM threadmill_jobs WHERE id = ?")) {
      ps.setBytes(1, OracleJdbc.bytes(job.id().asUuid()));
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        return rs.getString(1);
      }
    }
  }

  // ---------------------------------------------------------------- value mapping

  @Test
  void supplementaryCharactersSurviveLongClobBodiesAndPayloads() {
    // Binding a long string straight into a CLOB made the Oracle driver split
    // a surrogate pair that straddled its ~32 KiB chunk boundary, silently
    // corrupting emoji in large job bodies. Every character here is a
    // surrogate pair, so any chunk boundary falls inside one. Both bind paths
    // are covered: direct (short) and temporary-LOB (long).
    var store = store();
    for (int rockets : new int[] {
      OracleClobs.DIRECT_BIND_MAX_CHARS / 2 - 400, OracleClobs.DIRECT_BIND_MAX_CHARS, 15_000
    }) {
      String text = "\uD83D\uDE80".repeat(rockets);
      var job = Job.builder()
          .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
          .metadata("note", text)
          .build();
      job.log().info("𐀀 shipping ✈");
      store.insert(job);

      var loaded = store.findById(job.id()).orElseThrow();
      assertThat(loaded.metadata().get("note")).as("%s rockets", rockets).contains(text);
      assertThat(loaded.log().snapshot().getFirst().message()).isEqualTo("𐀀 shipping ✈");

      var task = new CronTask(
          "rockets-" + rockets,
          new CronTask.Trigger.CronExpr(CronExpression.parse("*/5 * * * *")),
          "com.example.H",
          new JobArgument("java.lang.String", text),
          "default",
          0,
          null,
          null,
          false,
          CronTask.MissedRunPolicy.DROP,
          ZoneId.of("UTC"),
          true);
      store.upsertCronTask(task);
      assertThat(store.findCronTask(task.name())).contains(task);
    }
  }

  @Test
  void instantsRoundTripIndependentlyOfJvmAndSessionTimeZones() throws SQLException {
    var original = TimeZone.getDefault();
    var scheduledFor = Instant.parse("2026-03-29T01:30:00.123456789Z"); // inside a DST change
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("Pacific/Chatham"));
      var store = new OracleJobStore(session("ALTER SESSION SET TIME_ZONE = 'Asia/Tokyo'"));
      var job = Job.builder()
          .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
          .initialState(JobState.SCHEDULED)
          .scheduledFor(scheduledFor)
          .build();
      store.insert(job);

      TimeZone.setDefault(TimeZone.getTimeZone("America/Sao_Paulo"));
      assertThat(store.findDueForPromotion(scheduledFor.minusNanos(1), 10)).isEmpty();
      assertThat(store.findDueForPromotion(scheduledFor, 10))
          .extracting(Job::id)
          .containsExactly(job.id());
      assertThat(store.oldestMaintenanceAt(JobState.SCHEDULED)).contains(scheduledFor);
    } finally {
      TimeZone.setDefault(original);
    }
  }

  /** Connections that run {@code alterSession} first, like a host's logon setting. */
  private DataSource session(String alterSession) {
    return (DataSource) Proxy.newProxyInstance(
        DataSource.class.getClassLoader(), new Class<?>[] {DataSource.class}, (proxy, m, args) -> {
          try {
            var result = m.invoke(dataSource, args);
            if (m.getName().equals("getConnection") && result instanceof Connection conn) {
              try (Statement st = conn.createStatement()) {
                st.execute(alterSession);
              }
            }
            return result;
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }

  @Test
  void keysetPagesFollowBinaryOrderUnderALinguisticClientLocale() throws SQLException {
    // The driver derives NLS_LANGUAGE (and so NLS_SORT) from the client JVM
    // locale. A German locale sorts a < Ä < B while keyset predicates compare
    // binary (B < a < Ä): ordering pages by the text column skipped rows.
    var store = new OracleJobStore(session("ALTER SESSION SET NLS_LANGUAGE = 'GERMAN'"));
    var names = List.of("a", "B", "Ä", "ab", "b", "Z", "z", "é", "A", "Ab", "aB", "ä", "zz", "0");
    for (String name : names) {
      store.upsertCronTask(new CronTask(
          name,
          new CronTask.Trigger.CronExpr(CronExpression.parse("*/5 * * * *")),
          "com.example.H",
          new JobArgument("java.lang.String", "\"x\""),
          "default",
          0,
          null,
          null,
          false,
          CronTask.MissedRunPolicy.DROP,
          ZoneId.of("UTC"),
          true));
    }
    var scanned = new ArrayList<String>();
    String after = null;
    for (int page = 0; page < names.size(); page++) {
      var batch = store.scanCronTasks(after, 3);
      batch.forEach(task -> scanned.add(task.name()));
      if (batch.size() < 3) break;
      after = batch.getLast().name();
    }
    assertThat(scanned).containsExactlyInAnyOrderElementsOf(names).doesNotHaveDuplicates();

    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement("INSERT INTO threadmill_concurrency_groups "
            + "(concurrency_key, last_modified) VALUES (?, SYS_EXTRACT_UTC(SYSTIMESTAMP) "
            + "- INTERVAL '1' HOUR)")) {
      conn.setAutoCommit(true);
      for (String name : names) {
        ps.setString(1, name);
        ps.executeUpdate();
      }
    }
    long removed = 0;
    for (int page = 0; page < names.size() / 3 + 1; page++) {
      long expected = Math.min(3, names.size() - removed);
      assertThat(store.deleteIdleConcurrencyGroups(3)).as("page %s", page).isEqualTo(expected);
      removed += expected;
    }
    assertThat(removed).isEqualTo(names.size());
  }

  @Test
  void emptyStringsSurviveOraclesEmptyStringIsNullRule() {
    var store = store();
    // Recurring payloads may serialize to the empty string.
    var task = new CronTask(
        "empty-payload",
        new CronTask.Trigger.CronExpr(CronExpression.parse("*/5 * * * *")),
        "com.example.H",
        new JobArgument("java.lang.String", ""),
        "default",
        0,
        null,
        null,
        false,
        CronTask.MissedRunPolicy.DROP,
        ZoneId.of("UTC"),
        true);
    store.upsertCronTask(task);
    assertThat(store.findCronTask("empty-payload")).contains(task);

    // A mutex holder may be empty; it must still match itself.
    assertThat(store.tryAcquireMutex("empty-holder", "", Duration.ofSeconds(30)))
        .isTrue();
    assertThat(store.tryAcquireMutex("empty-holder", "", Duration.ofSeconds(30)))
        .isTrue();
    assertThat(store.tryAcquireMutex("empty-holder", "other", Duration.ofSeconds(30)))
        .isFalse();
    store.releaseMutex("empty-holder", "");
    assertThat(store.tryAcquireMutex("empty-holder", "other", Duration.ofSeconds(30)))
        .isTrue();

    store.pauseQueue("empty-reason", "");
    assertThat(store.listPausedQueues()).contains("empty-reason");
  }

  // ---------------------------------------------------------------- counters & health

  @Test
  void queueCountersStayExactAcrossClaimsMovesDeletesAndCleanup() {
    var store = store();
    var jobs = new ArrayList<Job>();
    for (int i = 0; i < 6; i++) {
      jobs.add(Job.builder()
          .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
          .queue(i % 2 == 0 ? "alpha" : "beta")
          .build());
    }
    store.insertAll(jobs);
    assertThat(store.queueDepths()).containsEntry("alpha", 3L).containsEntry("beta", 3L);

    store.claimReady(NodeId.newId(), "alpha", 2, Instant.now());
    store.softDelete(jobs.get(1).id());
    assertThat(store.queueDepths()).containsEntry("alpha", 1L).containsEntry("beta", 2L);
    assertThat(store.countsByState())
        .containsEntry(JobState.ENQUEUED, 3L)
        .containsEntry(JobState.PROCESSING, 2L)
        .containsEntry(JobState.DELETED, 1L);

    store.claimReady(NodeId.newId(), "beta", 5, Instant.now());
    store.claimReady(NodeId.newId(), "alpha", 5, Instant.now());
    assertThat(store.queueDepths()).isEmpty();
    assertThat(store.deleteIdleQueueMetadata(100)).isPositive();
    assertThat(store.listEnqueuedQueues()).isEmpty();
  }

  @Test
  void verifyWritableProbesTheDatabaseAndFailsWhileUnreachable() {
    var down = new AtomicBoolean(false);
    var flaky = (DataSource) Proxy.newProxyInstance(
        DataSource.class.getClassLoader(), new Class<?>[] {DataSource.class}, (proxy, m, args) -> {
          if (m.getName().equals("getConnection") && down.get()) {
            throw new SQLException("simulated outage");
          }
          try {
            return m.invoke(dataSource, args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
        });
    var store = new OracleJobStore(flaky);

    store.verifyWritable();
    down.set(true);
    assertThatThrownBy(store::verifyWritable).isInstanceOf(OracleJobStore.JdbcException.class);
    down.set(false);
    store.verifyWritable();
  }

  @Test
  void describeNamesTheServerReleaseAndSchemaAndOffersNoRemoteWake() {
    var store = store();

    assertThat(store.describe()).startsWith("Oracle Database ").contains(" @ ");
    assertThat(store.createRemoteWakeChannel("threadmill_wake")).isEmpty();
  }
}
