package com.hemju.threadmill.store.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobRelationship;
import com.hemju.threadmill.core.JobReplacement;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.test.Jobs;
import com.hemju.threadmill.test.LegacyJobFixtures;

/**
 * PostgreSQL-specific regression tests.
 *
 * <p>The contract suite ({@link PostgresJobStoreContractTest}) already
 * covers the SPI semantics on real PostgreSQL. These tests pin down
 * Postgres-only invariants: encoding correctness on the body and
 * metadata columns, atomic claim across many concurrent virtual-thread
 * workers, the cheapness of per-state counts as the jobs table grows,
 * deadlock-state recognition, and migration-emit / cron-task /
 * mutex-lease semantics.
 */
@EnabledIf("com.hemju.threadmill.store.postgres.DockerAvailable#check")
class PostgresJobStoreRegressionTest {

  @SuppressWarnings("resource")
  private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
          DockerImageName.parse("postgres:18-alpine"))
      .withDatabaseName("threadmill")
      .withUsername("threadmill")
      .withPassword("threadmill");

  private static DataSource dataSource;

  @BeforeAll
  static void start() {
    POSTGRES.start();
    var ds = new PGSimpleDataSource();
    ds.setUrl(POSTGRES.getJdbcUrl());
    ds.setUser(POSTGRES.getUsername());
    ds.setPassword(POSTGRES.getPassword());
    dataSource = ds;
  }

  @AfterAll
  static void stop() {
    if (POSTGRES.isRunning()) POSTGRES.stop();
  }

  @BeforeEach
  void migrate() throws SQLException {
    new MigrationRunner(dataSource).migrate();
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("TRUNCATE threadmill_jobs, threadmill_nodes, threadmill_metadata, "
          + "threadmill_cron_tasks, threadmill_mutexes, threadmill_leases, "
          + "threadmill_dedup_keys, threadmill_queue_pauses, threadmill_concurrency_groups, "
          + "threadmill_concurrency_workflow_holds RESTART IDENTITY CASCADE");
      st.execute("UPDATE threadmill_job_counts SET count = 0");
      st.execute("TRUNCATE threadmill_queue_counts");
    }
  }

  private JobStore store() {
    return new PostgresJobStore(dataSource);
  }

  @Test
  void transactionErrorRollsBackWritesBeforeRestoringAutoCommit() throws SQLException {
    for (boolean autoCommit : List.of(true, false)) {
      try (var connection = dataSource.getConnection()) {
        connection.setAutoCommit(autoCommit);
        var failure = new AssertionError("after durable SQL write");
        assertThatThrownBy(() -> PostgresTransactions.execute(connection, transaction -> {
              try (var statement = transaction.createStatement()) {
                statement.executeUpdate(
                    "INSERT INTO threadmill_metadata VALUES ('error-test', 'value')");
              }
              throw failure;
            }))
            .isSameAs(failure);
        assertThat(connection.getAutoCommit()).isEqualTo(autoCommit);
      }
      assertNoFailedTransactionWrite();
    }
  }

  @Test
  void transactionCleanupFailuresDoNotMaskTheOriginalError() throws SQLException {
    for (var phase : List.of("rollback", "reset")) {
      try (var connection = dataSource.getConnection()) {
        var cleanupFailure = new SQLException("injected " + phase + " failure");
        var wrapped = (Connection) Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              try {
                var result = method.invoke(connection, args);
                if ((phase.equals("rollback") && method.getName().equals("rollback"))
                    || (phase.equals("reset")
                        && method.getName().equals("setAutoCommit")
                        && Boolean.TRUE.equals(args[0]))) {
                  throw cleanupFailure;
                }
                return result;
              } catch (InvocationTargetException error) {
                throw error.getCause();
              }
            });
        var failure = new AssertionError("original transaction failure");
        assertThatThrownBy(() -> PostgresTransactions.execute(wrapped, transaction -> {
              try (var statement = transaction.createStatement()) {
                statement.executeUpdate(
                    "INSERT INTO threadmill_metadata VALUES ('error-test', 'value')");
              }
              throw failure;
            }))
            .isSameAs(failure);
        assertThat(failure.getSuppressed()).contains(cleanupFailure);
      }
      assertNoFailedTransactionWrite();
    }
  }

  private void assertNoFailedTransactionWrite() throws SQLException {
    try (var connection = dataSource.getConnection();
        var statement = connection.createStatement();
        var result = statement.executeQuery(
            "SELECT value FROM threadmill_metadata WHERE key = 'error-test'")) {
      assertThat(result.next()).isFalse();
    }
  }

  @Test
  void unreadableBodyAtTheQueueHeadDoesNotStallClaimsForGoodJobs() throws SQLException {
    JobStore store = store();
    // Built in order, so the UUIDv7 ids are creation-ordered and `head` sits
    // at the queue head (ORDER BY priority DESC, id).
    Job head = sampleOnDefault();
    Job good1 = sampleOnDefault();
    Job good2 = sampleOnDefault();
    store.insert(head);
    store.insert(good1);
    store.insert(good2);

    // Corrupt the head job's persisted body so deserialize fails.
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(
            "UPDATE threadmill_jobs SET body = '{not valid json' WHERE id = ?")) {
      ps.setObject(1, head.id().asUuid());
      ps.executeUpdate();
    }

    List<Job> claimed = store.claimReady(NodeId.newId(), "default", 3, Instant.now());
    assertThat(claimed).extracting(Job::id).containsExactlyInAnyOrder(good1.id(), good2.id());

    // The poison job is quarantined, not left ENQUEUED to wedge the queue.
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT state FROM threadmill_jobs WHERE id = ?")) {
      ps.setObject(1, head.id().asUuid());
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString(1)).isEqualTo("QUARANTINED");
      }
    }
  }

  @Test
  void quarantinedKeyedWorkflowMemberReleasesTheWorkflowHold() throws SQLException {
    JobStore store = store();
    Job root = Job.builder()
        .spec(JobSpec.of("com.example.Root", new JobArgument("java.lang.String", "\"x\"")))
        .queue("default")
        .concurrencyKey("project:corrupt")
        .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
        .build();
    store.insert(root);
    Job claimedRoot =
        store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);

    // A member enqueued under the root's active hold, then corrupted.
    Job member = Job.builder()
        .spec(JobSpec.of("com.example.Member", new JobArgument("java.lang.String", "\"x\"")))
        .queue("default")
        .concurrencyKey("project:corrupt")
        .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
        .workflowRootId(claimedRoot.id())
        .build();
    store.insert(member);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(
            "UPDATE threadmill_jobs SET body = '{not valid json' WHERE id = ?")) {
      ps.setObject(1, member.id().asUuid());
      ps.executeUpdate();
    }

    // Root finishes; the hold survives on the member's account.
    long rootVersion = claimedRoot.version();
    claimedRoot.transitionTo(JobState.SUCCEEDED, Instant.now(), "test", null);
    claimedRoot.clearOwner();
    store.saveAtomic(claimedRoot, rootVersion);

    // The claim pass quarantines the member — and must release the hold,
    // or the key stays held forever (the queue-level wedge would just
    // move to the concurrency key).
    assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now())).isEmpty();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(
            "SELECT count(*) FROM threadmill_concurrency_workflow_holds WHERE concurrency_key = ?")) {
      ps.setString(1, "project:corrupt");
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt(1)).as("hold rows left for the key").isZero();
      }
    }

    // The key is claimable again for a fresh EXCLUSIVE.
    Job next = Job.builder()
        .spec(JobSpec.of("com.example.Next", new JobArgument("java.lang.String", "\"x\"")))
        .queue("default")
        .concurrencyKey("project:corrupt")
        .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
        .build();
    store.insert(next);
    assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
        .extracting(Job::id)
        .containsExactly(next.id());
  }

  private static Job sampleOnDefault() {
    return Job.builder()
        .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
        .queue("default")
        .build();
  }

  @Test
  void verifyWritableProbesTheDatabaseAndFailsWhileUnreachable() {
    // A trippable DataSource that throws on getConnection when "down".
    var down = new java.util.concurrent.atomic.AtomicBoolean(false);
    DataSource flaky = (DataSource) java.lang.reflect.Proxy.newProxyInstance(
        DataSource.class.getClassLoader(),
        new Class<?>[] {DataSource.class},
        (proxy, method, args) -> {
          if (method.getName().equals("getConnection") && down.get()) {
            throw new SQLException("simulated outage");
          }
          try {
            return method.invoke(dataSource, args);
          } catch (java.lang.reflect.InvocationTargetException e) {
            throw e.getCause();
          }
        });
    var store = new PostgresJobStore(flaky);

    store.verifyWritable(); // healthy: a real SELECT 1 round trip succeeds

    down.set(true);
    assertThatThrownBy(store::verifyWritable).isInstanceOf(PostgresJobStore.JdbcException.class);

    down.set(false);
    store.verifyWritable(); // recovered: the probe passes again
  }

  @Test
  void fourByteUnicodeRoundTripsThroughJsonBodyAndMetadata() {
    String exotic = "shipping ✈ 🚀 𐀀 𐀁 𐀂";
    JobStore store = store();
    Job j = Job.builder()
        .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
        .metadata("note", exotic)
        .build();
    j.log().info(exotic);
    store.insert(j);

    Job loaded = store.findById(j.id()).orElseThrow();
    assertThat(loaded.metadata().get("note")).contains(exotic);
    assertThat(loaded.log().snapshot().get(0).message()).isEqualTo(exotic);
  }

  @Test
  void insertAllWithReversedKeyOrdersDoesNotManufactureDeadlocks() throws Exception {
    // Concurrent batches whose keyed jobs arrive in opposite key orders:
    // per-snapshot group-row locking in batch order deadlocked (recovered
    // by retry in owning mode, fatal in join_transaction mode). With the
    // sorted single locking pass, no round may fail.
    JobStore store = store();
    int rounds = 25;
    var keys = new ArrayList<String>();
    for (int i = 0; i < 8; i++) keys.add("lock-order:" + i);

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

  private static Job keyedJob(String key) {
    return keyedJob(key, 0);
  }

  private static Job keyedJob(String key, int priority) {
    return Job.builder()
        .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
        .concurrencyKey(key)
        .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
        .priority(priority)
        .build();
  }

  private static DataSource countingLooseScanDataSource(AtomicInteger looseScans) {
    return (DataSource) Proxy.newProxyInstance(
        DataSource.class.getClassLoader(),
        new Class<?>[] {DataSource.class},
        (proxy, method, args) -> {
          try {
            var result = method.invoke(dataSource, args);
            if (method.getName().equals("getConnection") && result instanceof Connection conn) {
              return countingLooseScanConnection(conn, looseScans);
            }
            return result;
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }

  private static Connection countingLooseScanConnection(
      Connection connection, AtomicInteger looseScans) {
    return (Connection) Proxy.newProxyInstance(
        Connection.class.getClassLoader(),
        new Class<?>[] {Connection.class},
        (proxy, method, args) -> {
          if (method.getName().equals("prepareStatement")
              && args != null
              && args.length > 0
              && args[0] instanceof String sql
              && sql.contains("WITH RECURSIVE keys(k)")) {
            looseScans.incrementAndGet();
          }
          try {
            return method.invoke(connection, args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }

  @Test
  void unkeyedClaimLocksOnlyANarrowPageSoConcurrentClaimersAreNotStarved() throws Exception {
    // With no keyed jobs, each claim must lock only a narrow page. The
    // historical unconditional 64x page pinned the entire backlog here
    // (640 > 300), so overlapping claimers' SKIP LOCKED scans returned
    // empty while claimable work existed.
    JobStore store = store();
    int total = 300;
    for (int i = 0; i < total; i++) {
      store.insert(Job.builder()
          .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
          .build());
    }

    int claimers = 8;
    int perClaim = 10;
    var start = new CountDownLatch(1);
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      List<Future<List<Job>>> futures = new ArrayList<>();
      for (int i = 0; i < claimers; i++) {
        futures.add(executor.submit(() -> {
          start.await();
          return store.claimReady(NodeId.newId(), "default", perClaim, Instant.now());
        }));
      }
      start.countDown();
      Set<UUID> seen = new HashSet<>();
      for (Future<List<Job>> f : futures) {
        List<Job> claimed = f.get(60, TimeUnit.SECONDS);
        // 300 jobs comfortably cover 8 claimers x (2x10)-row pages:
        // every overlapping claimer must find its full batch.
        assertThat(claimed).hasSize(perClaim);
        for (Job j : claimed) {
          assertThat(seen.add(j.id().asUuid())).isTrue();
        }
      }
    }
  }

  @Test
  void keyedClaimRotationReachesClaimableWorkBeyondTheFirstBlockedPage() {
    var setupStore = store();
    var blockedKeyCount = PostgresJobStore.MAX_PENDING_KEYS_PER_PASS + 1;
    var blockers = new ArrayList<Job>(blockedKeyCount);
    for (int i = 0; i < blockedKeyCount; i++) {
      blockers.add(keyedJob("blocked:%04d".formatted(i)));
    }
    setupStore.insertAll(blockers);
    assertThat(setupStore.claimReady(NodeId.newId(), "default", blockedKeyCount, Instant.now()))
        .hasSize(blockedKeyCount);

    var blockedWaiters = new ArrayList<Job>(blockedKeyCount);
    for (int i = 0; i < blockedKeyCount; i++) {
      blockedWaiters.add(keyedJob("blocked:%04d".formatted(i)));
    }
    setupStore.insertAll(blockedWaiters);
    var laterClaimable = keyedJob("claimable:after-blocked-page");
    setupStore.insert(laterClaimable);

    // A fresh store starts at the lexicographically first key. The first
    // page contains only blocked keys; the second contains the extra
    // blocker plus the independent key.
    var claimingStore = store();
    assertThat(claimingStore.claimReady(NodeId.newId(), "default", 1, Instant.now()))
        .isEmpty();
    assertThat(claimingStore.claimReady(NodeId.newId(), "default", 1, Instant.now()))
        .extracting(Job::id)
        .containsExactly(laterClaimable.id());

    // The short tail clears the cursor. Work inserted at an early key is
    // reached when the next poll wraps to the beginning.
    var earlyClaimable = keyedJob("available:after-wrap", Integer.MAX_VALUE);
    setupStore.insert(earlyClaimable);
    assertThat(claimingStore.claimReady(NodeId.newId(), "default", 1, Instant.now()))
        .extracting(Job::id)
        .containsExactly(earlyClaimable.id());
  }

  @Test
  void smallKeyQueuesUseOneLooseScanPerClaimPoll() {
    var setupStore = store();
    var blockers = List.of(keyedJob("blocked:a"), keyedJob("blocked:b"), keyedJob("blocked:c"));
    setupStore.insertAll(blockers);
    assertThat(setupStore.claimReady(NodeId.newId(), "default", blockers.size(), Instant.now()))
        .hasSize(blockers.size());
    setupStore.insertAll(
        List.of(keyedJob("blocked:a"), keyedJob("blocked:b"), keyedJob("blocked:c")));

    var looseScans = new AtomicInteger();
    var measuredStore = new PostgresJobStore(countingLooseScanDataSource(looseScans));
    looseScans.set(0);
    for (int poll = 0; poll < 4; poll++) {
      var before = looseScans.get();
      assertThat(measuredStore.claimReady(NodeId.newId(), "default", 1, Instant.now()))
          .isEmpty();
      assertThat(looseScans.get() - before).as("loose scans in poll %s", poll).isEqualTo(1);
    }
  }

  @Test
  void drainedTailRestartsKeyedScanWithoutAnEmptyPoll() {
    var setupStore = store();
    var blockedKeyCount = PostgresJobStore.MAX_PENDING_KEYS_PER_PASS + 1;
    var blockers = new ArrayList<Job>(blockedKeyCount);
    for (int i = 0; i < blockedKeyCount; i++) {
      blockers.add(keyedJob("blocked:%04d".formatted(i)));
    }
    setupStore.insertAll(blockers);
    assertThat(setupStore.claimReady(NodeId.newId(), "default", blockedKeyCount, Instant.now()))
        .hasSize(blockedKeyCount);

    var blockedWaiters = new ArrayList<Job>(blockedKeyCount);
    for (int i = 0; i < blockedKeyCount; i++) {
      blockedWaiters.add(keyedJob("blocked:%04d".formatted(i)));
    }
    setupStore.insertAll(blockedWaiters);

    var claimingStore = store();
    assertThat(claimingStore.claimReady(NodeId.newId(), "default", 1, Instant.now()))
        .isEmpty();

    // The only key after the first page was the look-ahead key. Drain it
    // before the next poll, then add work before the saved cursor. The next
    // tail scan is empty and must restart from the beginning in this poll.
    assertThat(setupStore.softDelete(blockedWaiters.getLast().id())).isTrue();
    var earlyClaimable = keyedJob("available:after-drained-tail", Integer.MAX_VALUE);
    setupStore.insert(earlyClaimable);

    assertThat(claimingStore.claimReady(NodeId.newId(), "default", 1, Instant.now()))
        .extracting(Job::id)
        .containsExactly(earlyClaimable.id());
  }

  @Test
  void claimReadyIsAtomicAcrossManyConcurrentVirtualThreads() throws Exception {
    JobStore store = store();
    int total = 200;
    for (int i = 0; i < total; i++) {
      store.insert(Job.builder()
          .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
          .build());
    }

    int workers = 12;
    var start = new CountDownLatch(1);
    Set<UUID> seen = ConcurrentHashMap.newKeySet();
    var collisions = new ConcurrentHashMap<UUID, Integer>();
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      List<Future<List<Job>>> futures = new ArrayList<>();
      for (int w = 0; w < workers; w++) {
        NodeId node = NodeId.newId();
        futures.add(executor.submit(() -> {
          start.await();
          List<Job> mine = new ArrayList<>();
          while (true) {
            List<Job> got = store.claimReady(node, "default", 7, Instant.now());
            if (got.isEmpty()) break;
            mine.addAll(got);
          }
          return mine;
        }));
      }
      start.countDown();
      for (var f : futures) {
        for (Job j : f.get(60, TimeUnit.SECONDS)) {
          if (!seen.add(j.id().asUuid())) {
            collisions.merge(j.id().asUuid(), 1, Integer::sum);
          }
        }
      }
      executor.shutdown();
      assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }
    assertThat(collisions)
        .as("no double-claim across concurrent virtual-thread workers")
        .isEmpty();
    assertThat(seen).hasSize(total);
  }

  @Test
  void recentlyUsedConcurrencyKeysSurviveCleanupUntilTheirIdleGraceExpires() throws SQLException {
    var store = store();
    try (var connection = dataSource.getConnection();
        var statement = connection.createStatement()) {
      statement.executeUpdate(
          "INSERT INTO threadmill_concurrency_groups VALUES ('recent',0,0,clock_timestamp())");
      assertThat(store.deleteIdleConcurrencyGroups(100)).isZero();
      statement.executeUpdate(
          "UPDATE threadmill_concurrency_groups SET last_modified=clock_timestamp()-interval '2 minutes'");
      assertThat(store.deleteIdleConcurrencyGroups(100)).isEqualTo(1);
    }
  }

  @Test
  void emptyQueueCleanupRemovesBalancedShardsWithoutErasingLockedCounterChanges()
      throws SQLException {
    var store = store();
    var active =
        Job.builder().queue("active").spec(JobSpec.of("example.Handler")).build();
    store.insert(active);
    try (var connection = dataSource.getConnection();
        var statement = connection.createStatement()) {
      statement.executeUpdate(
          "INSERT INTO threadmill_queue_counts SELECT 'old-'||lpad(n::text,4,'0'),s,CASE WHEN s=0 THEN 5 ELSE -5 END FROM generate_series(1,250) n CROSS JOIN generate_series(0,1) s");
      connection.setAutoCommit(false);
      statement
          .executeQuery(
              "SELECT * FROM threadmill_queue_counts WHERE queue='old-0001' AND shard=0 FOR UPDATE")
          .close();
      for (int i = 0; i < 4; i++) store.deleteIdleQueueMetadata(1000);
      // A negative shard alone is not zero-sum and cannot be deleted while
      // its positive counterpart is locked by a concurrent writer.
      try (var rows = statement.executeQuery(
          "SELECT count(*),sum(count) FROM threadmill_queue_counts WHERE queue='old-0001'")) {
        rows.next();
        assertThat(rows.getLong(1)).isEqualTo(2);
        assertThat(rows.getLong(2)).isZero();
      }
      connection.rollback();
      for (int i = 0; i < 4; i++) store.deleteIdleQueueMetadata(1000);
      assertThat(store.queueDepths()).containsExactly(Map.entry("active", 1L));
      try (var rows = statement.executeQuery(
          "SELECT count(*) FROM threadmill_queue_counts WHERE queue<>'active'")) {
        rows.next();
        assertThat(rows.getLong(1)).isZero();
      }
    }
  }

  @Test
  void queueCleanupRacingProducerAndClaimTriggersPreservesExactCounts() throws Exception {
    var store = store();
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var futures = new ArrayList<Future<?>>();
      for (int worker = 0; worker < 4; worker++) {
        int lane = worker;
        futures.add(executor.submit(() -> {
          for (int i = 0; i < 30; i++) {
            var job = Job.builder()
                .queue("churn-" + lane + "-" + i)
                .spec(JobSpec.of("example.Handler"))
                .build();
            store.insert(job);
            store.softDelete(job.id());
          }
        }));
      }
      for (int sample = 0; sample < 30; sample++) {
        store.deleteIdleQueueMetadata(100);
        assertThat(store.queueDepths().values()).allMatch(depth -> depth >= 0);
      }
      for (var future : futures) future.get(30, TimeUnit.SECONDS);
    }
    for (int i = 0; i < 4; i++) store.deleteIdleQueueMetadata(100);
    assertThat(store.queueDepths()).isEmpty();
    try (var connection = dataSource.getConnection();
        var statement = connection.createStatement();
        var rows = statement.executeQuery("SELECT count(*) FROM threadmill_queue_counts")) {
      rows.next();
      assertThat(rows.getLong(1)).isZero();
    }
  }

  @Test
  void retentionCandidatePlanUsesCutoffAndTimeIdIndexWithoutSorting() throws SQLException {
    try (var connection = dataSource.getConnection();
        var statement = connection.createStatement()) {
      statement.execute("SET enable_seqscan=off");
      try (var rows = statement.executeQuery(
          "EXPLAIN (FORMAT TEXT) SELECT id,current_state_at FROM threadmill_jobs WHERE state='SUCCEEDED' AND current_state_at<='2026-01-01' AND (current_state_at,id)>('2025-01-01','00000000-0000-4000-8000-000000000001') ORDER BY current_state_at,id LIMIT 100 FOR UPDATE SKIP LOCKED")) {
        var plan = new StringBuilder();
        while (rows.next()) plan.append(rows.getString(1));
        assertThat(plan.toString())
            .contains("threadmill_jobs_retention_idx", "Index Cond")
            .doesNotContain("Sort", "Seq Scan");
      }
    }
  }

  @Test
  void idleConcurrencyReclamationIsBoundedAndResumesAfterDeletedPages() throws SQLException {
    var store = store();
    try (var connection = dataSource.getConnection();
        var statement = connection.createStatement()) {
      statement.executeUpdate(
          "INSERT INTO threadmill_concurrency_groups SELECT 'old-'||lpad(n::text,4,'0'),0,0,now()-interval '2 minutes' FROM generate_series(1,250) n");
    }
    assertThat(store.deleteIdleConcurrencyGroups(1000)).isEqualTo(100);
    assertThat(store.deleteIdleConcurrencyGroups(1000)).isEqualTo(100);
    assertThat(store.deleteIdleConcurrencyGroups(1000)).isEqualTo(50);
    assertThat(store.deleteIdleConcurrencyGroups(1000)).isZero();
    try (var connection = dataSource.getConnection();
        var statement = connection.createStatement();
        var rows = statement.executeQuery("SELECT count(*) FROM threadmill_concurrency_groups")) {
      rows.next();
      assertThat(rows.getLong(1)).isZero();
    }
  }

  @Test
  void queueCountersStayExactAcrossConcurrentClaimsMovesAndRollback() throws Exception {
    var store = store();
    var jobs = new ArrayList<Job>();
    for (int i = 0; i < 400; i++)
      jobs.add(Job.builder().spec(JobSpec.of("example.Handler")).build());
    store.insertAll(jobs);
    assertThat(store.replaceJob(
            jobs.getFirst().id(),
            jobs.getFirst().version(),
            JobReplacement.builder().queue("moved").build()))
        .isTrue();
    try (var connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      try (var update = connection.createStatement()) {
        update.executeUpdate("UPDATE threadmill_jobs SET queue='rollback' WHERE queue='default'");
      }
      connection.rollback();
    }
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var futures = new ArrayList<Future<?>>();
      for (int worker = 0; worker < 4; worker++) {
        futures.add(executor.submit(() -> {
          for (int pass = 0; pass < 5; pass++)
            store.claimReady(NodeId.newId(), "default", 10, Instant.now());
        }));
      }
      for (int sample = 0; sample < 20; sample++) {
        assertThat(store.queueDepths().values()).allMatch(depth -> depth >= 0);
        store.oldestEnqueuedAt("default");
      }
      for (var future : futures) future.get(30, TimeUnit.SECONDS);
    }
    var exact = new HashMap<String, Long>();
    try (var connection = dataSource.getConnection();
        var statement = connection.createStatement();
        var rows = statement.executeQuery(
            "SELECT queue,count(*) FROM threadmill_jobs WHERE state='ENQUEUED' GROUP BY queue")) {
      while (rows.next()) exact.put(rows.getString(1), rows.getLong(2));
    }
    assertThat(store.queueDepths())
        .isEqualTo(exact)
        .containsEntry("moved", 1L)
        .doesNotContainKey("rollback");
  }

  @Test
  void queueMonitoringUsesCounterRowsAndAnOrderedAgeIndex() throws SQLException {
    try (var connection = dataSource.getConnection();
        var statement = connection.createStatement()) {
      try (var rows = statement.executeQuery(
          "EXPLAIN (FORMAT TEXT) SELECT queue,SUM(count) FROM threadmill_queue_counts GROUP BY queue HAVING SUM(count)>0")) {
        var plan = new StringBuilder();
        while (rows.next()) plan.append(rows.getString(1));
        assertThat(plan.toString()).doesNotContain("threadmill_jobs");
      }
      statement.execute("SET enable_seqscan=off");
      try (var rows = statement.executeQuery(
          "EXPLAIN (FORMAT TEXT) SELECT current_state_at FROM threadmill_jobs WHERE state='ENQUEUED' AND queue='default' ORDER BY current_state_at LIMIT 1")) {
        var plan = new StringBuilder();
        while (rows.next()) plan.append(rows.getString(1));
        assertThat(plan.toString())
            .contains("threadmill_jobs_queue_age_idx")
            .doesNotContain("Sort");
      }
      try (var rows = statement.executeQuery(
          "EXPLAIN (FORMAT TEXT) SELECT body FROM threadmill_jobs WHERE state='ENQUEUED' ORDER BY current_state_at DESC,id LIMIT 20")) {
        var plan = new StringBuilder();
        while (rows.next()) plan.append(rows.getString(1));
        assertThat(plan.toString())
            .contains("threadmill_jobs_state_page_idx")
            .doesNotContain("Sort");
      }
    }
  }

  @Test
  void perStateCountsReadFromCounterTableNotFromJobsTable() throws SQLException {
    JobStore store = store();
    for (int i = 0; i < 100; i++) {
      store.insert(Job.builder()
          .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
          .build());
    }
    // After insert, the trigger should have ENQUEUED at 100.
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(
            "EXPLAIN (FORMAT TEXT) SELECT state, count FROM threadmill_job_counts")) {
      try (ResultSet rs = ps.executeQuery()) {
        var plan = new StringBuilder();
        while (rs.next()) plan.append(rs.getString(1)).append('\n');
        // Reading from the counter table must not touch threadmill_jobs.
        assertThat(plan.toString()).doesNotContainIgnoringCase("threadmill_jobs");
      }
    }

    var counts = store.countsByState();
    assertThat(counts.get(JobState.ENQUEUED)).isEqualTo(100L);

    // After claim, ENQUEUED should drop and PROCESSING should rise — proves the trigger fires on
    // UPDATE too.
    store.claimReady(NodeId.newId(), "default", 30, Instant.now());
    var afterClaim = store.countsByState();
    assertThat(afterClaim.get(JobState.ENQUEUED)).isEqualTo(70L);
    assertThat(afterClaim.get(JobState.PROCESSING)).isEqualTo(30L);
  }

  @Test
  void deadlockRetryRecognisesDeadlockSqlState() {
    var deadlock = new SQLException("simulated deadlock", "40P01");
    var unrelated = new SQLException("other", "23505");
    assertThat(DeadlockRetry.isRetryable(deadlock)).isTrue();
    assertThat(DeadlockRetry.isRetryable(unrelated)).isFalse();
  }

  @Test
  void nonemptyVersion030SchemaUpgradesWithoutChangingWireOrOperationalState() throws Exception {
    var schema = "upgrade_v030";
    try (var connection = dataSource.getConnection();
        var statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA " + schema);
    }
    var legacyDataSource = new PGSimpleDataSource();
    legacyDataSource.setUrl(POSTGRES.getJdbcUrl());
    legacyDataSource.setUser(POSTGRES.getUsername());
    legacyDataSource.setPassword(POSTGRES.getPassword());
    legacyDataSource.setCurrentSchema(schema);
    try {
      try (var connection = legacyDataSource.getConnection();
          var statement = connection.createStatement()) {
        statement.execute(
            "CREATE TABLE threadmill_schema_history (version INTEGER PRIMARY KEY, description TEXT NOT NULL, checksum TEXT, installed_at TIMESTAMPTZ DEFAULT now())");
        var migrations = List.of(
            "V1__baseline.sql",
            "V2__cron_task_overrides.sql",
            "V3__integrity_constraints.sql",
            "V4__cron_state_timing_fingerprint.sql",
            "V5__cron_state_nudge.sql",
            "V6__cron_task_exclusive.sql");
        for (var file : migrations) {
          String sql;
          try (var input = getClass().getResourceAsStream("/compatibility/v0.3.0/" + file)) {
            assertThat(input).isNotNull();
            sql = new String(input.readAllBytes(), StandardCharsets.UTF_8);
          }
          statement.execute(sql);
          try (var history = connection.prepareStatement(
              "INSERT INTO threadmill_schema_history(version,description,checksum) VALUES (?,?,?)")) {
            history.setInt(1, Integer.parseInt(file.substring(1, file.indexOf("__"))));
            history.setString(
                2, file.substring(file.indexOf("__") + 2, file.length() - 4).replace('_', ' '));
            history.setString(
                3,
                HexFormat.of()
                    .formatHex(MessageDigest.getInstance("SHA-256")
                        .digest(sql.getBytes(StandardCharsets.UTF_8))));
            history.executeUpdate();
          }
        }
        var serializer = new JsonJobSerializer();
        for (var name : LegacyJobFixtures.NAMES) {
          var wire = LegacyJobFixtures.wire(name);
          var job = serializer.deserializeJob(wire);
          var snapshot = job.snapshot();
          try (var insert = connection.prepareStatement(
              "INSERT INTO threadmill_jobs(id,state,queue,priority,handler_signature,scheduled_at,owner_node_id,owner_heartbeat_at,last_checkin_at,current_state_at,version,body,created_at,workflow_root_id,parent_job_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
            insert.setObject(1, job.id().asUuid());
            insert.setString(2, job.currentState().name());
            insert.setString(3, job.queue());
            insert.setInt(4, job.priority());
            insert.setString(5, job.spec().handlerType());
            insert.setTimestamp(
                6,
                snapshot.scheduledFor() == null ? null : Timestamp.from(snapshot.scheduledFor()));
            insert.setObject(
                7,
                snapshot.ownerNodeId() == null ? null : snapshot.ownerNodeId().asUuid());
            insert.setTimestamp(
                8,
                snapshot.ownerHeartbeatAt() == null
                    ? null
                    : Timestamp.from(snapshot.ownerHeartbeatAt()));
            insert.setTimestamp(
                9,
                snapshot.lastCheckinAt() == null ? null : Timestamp.from(snapshot.lastCheckinAt()));
            insert.setTimestamp(10, Timestamp.from(job.stateHistory().getLast().at()));
            insert.setLong(11, job.version());
            insert.setString(12, wire);
            insert.setTimestamp(13, Timestamp.from(job.createdAt()));
            insert.setObject(14, job.workflowRootId().asUuid());
            insert.setObject(
                15,
                job.relationship()
                    .map(relationship -> relationship.parentId().asUuid())
                    .orElse(null));
            insert.executeUpdate();
          }
        }
        statement.execute(
            "INSERT INTO threadmill_cron_tasks(name,trigger_kind,trigger_value,handler_signature,payload_type_tag,payload_serialized,exclusive) VALUES ('upgrade-cron','INTERVAL','PT1H','example.UpgradeHandler','example.UpgradePayload','{}',true)");
        statement.execute(
            "INSERT INTO threadmill_cron_task_state(task_name,next_run_at,in_flight_job_id,timing_fingerprint,nudge_requested_at,nudge_revision) VALUES ('upgrade-cron',now(),'01900000-0000-7000-8000-000000000003','legacy-fingerprint',now(),17)");
        statement.execute(
            "INSERT INTO threadmill_cron_task_ownership VALUES ('upgrade-app','upgrade-cron')");
        statement.execute(
            "INSERT INTO threadmill_dedup_keys VALUES ('upgrade','legacy-dedup','01900000-0000-7000-8000-000000000001',now() + interval '1 hour')");
        statement.execute(
            "INSERT INTO threadmill_queue_pauses(queue,paused_at,paused_by) VALUES ('empty-paused',now(),'upgrade')");
      }
      var runner = new MigrationRunner(legacyDataSource);
      runner.migrate();
      runner.migrate();
      runner.validate();
      var upgraded = new PostgresJobStore(legacyDataSource);
      var serializer = new JsonJobSerializer();
      try (var connection = legacyDataSource.getConnection();
          var query = connection.prepareStatement(
              "SELECT body,execution_revision FROM threadmill_jobs WHERE id = ?")) {
        for (var name : LegacyJobFixtures.NAMES) {
          var original = serializer.deserializeJob(LegacyJobFixtures.wire(name));
          query.setObject(1, original.id().asUuid());
          try (var row = query.executeQuery()) {
            assertThat(row.next()).isTrue();
            assertThat(row.getString(1)).isEqualTo(LegacyJobFixtures.wire(name));
            assertThat(row.getLong(2)).isZero();
          }
          assertThat(upgraded.findById(original.id())).hasValueSatisfying(job -> {
            assertThat(job.version()).isEqualTo(7);
            assertThat(job.currentState()).isEqualTo(original.currentState());
          });
        }
      }
      assertThat(upgraded.listCronTaskNamesOwnedBy("upgrade-app")).containsExactly("upgrade-cron");
      assertThat(upgraded.enqueueIfAbsent(
              Jobs.onQueue("example.Dedup", "upgrade"),
              "legacy-dedup",
              Duration.ofHours(1),
              Instant.now()))
          .isEqualTo(
              new EnqueueResult.Coalesced(JobId.parse("01900000-0000-7000-8000-000000000001")));
      assertThat(upgraded.queueDepths()).containsEntry("upgrade", 1L);
      assertThat(upgraded.listPausedQueues()).contains("empty-paused");
      var cron = upgraded.findCronTaskState("upgrade-cron").orElseThrow();
      assertThat(cron.nudgeRevision()).isEqualTo(17L);
      assertThat(cron.inFlightJobId())
          .isEqualTo(UUID.fromString("01900000-0000-7000-8000-000000000003"));
      assertThat(upgraded.findCronTask("upgrade-cron").orElseThrow().exclusive())
          .isTrue();
      assertThat(upgraded.deleteFinishedOlderThan(Instant.now(), JobState.FAILED, 100))
          .isZero();
      assertThat(upgraded.findAwaitingByParent(
              JobId.parse("01900000-0000-7000-8000-000000000005"), 10))
          .hasSize(1);
      assertThat(upgraded.claimReady(NodeId.newId(), "upgrade", 1, Instant.now()))
          .hasSize(1);
    } finally {
      try (var connection = dataSource.getConnection();
          var statement = connection.createStatement()) {
        statement.execute("DROP SCHEMA " + schema + " CASCADE");
      }
    }
  }

  @Test
  void migrationsAreIdempotent() {
    // Already applied by @BeforeEach; running again must be a no-op.
    new MigrationRunner(dataSource).migrate();
    new MigrationRunner(dataSource).migrate();
  }

  @Test
  void emitPendingSqlAfterMigrateIsEmpty() {
    String sql = new MigrationRunner(dataSource).emitPendingSql();
    assertThat(sql).isBlank();
  }

  @Test
  void emittedSqlWrapsEachMigrationAndItsHistoryInsertInOneTransaction() throws SQLException {
    dropSchemaObjects();
    String sql = new MigrationRunner(dataSource).emitPendingSql();

    // Every shipped migration's DDL and its history INSERT are bracketed by
    // BEGIN/COMMIT so an external psql apply cannot half-apply a migration.
    long begins = sql.lines().filter(l -> l.strip().equals("BEGIN;")).count();
    long commits = sql.lines().filter(l -> l.strip().equals("COMMIT;")).count();
    assertThat(begins).isEqualTo(commits).isGreaterThanOrEqualTo(1L);
    assertThat(sql).contains("BEGIN;").contains("COMMIT;");
    // The history INSERT lives inside a transaction block.
    int firstBegin = sql.indexOf("BEGIN;");
    int firstInsert = sql.indexOf("INSERT INTO threadmill_schema_history");
    int firstCommitAfter = sql.indexOf("COMMIT;", firstInsert);
    assertThat(firstBegin).isLessThan(firstInsert);
    assertThat(firstInsert).isLessThan(firstCommitAfter);
  }

  @Test
  void emitPendingSqlOnAFreshDatabaseIsReadOnlyAndPrependsHistoryDdl() throws SQLException {
    dropSchemaObjects();

    String sql = new MigrationRunner(dataSource).emitPendingSql();

    // The inspect-only API must not execute DDL: no history table (or any
    // other Threadmill table) may exist afterwards...
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT to_regclass('threadmill_schema_history')")) {
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString(1)).isNull();
    }
    // ...and the emitted SQL must carry the history-table DDL itself plus
    // every shipped migration, so an external apply works on a clean DB.
    assertThat(sql)
        .contains("threadmill_schema_history")
        .contains("V1__baseline.sql")
        .contains("V2__cron_task_overrides.sql")
        .contains("V3__integrity_constraints.sql")
        .contains("V4__cron_state_timing_fingerprint.sql")
        .contains("V5__cron_state_nudge.sql")
        .contains("V6__cron_task_exclusive.sql")
        .contains("V7__execution_revision.sql");
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute(sql);
      try (ResultSet rs = st.executeQuery("SELECT count(*) FROM threadmill_schema_history")) {
        assertThat(rs.next()).isTrue();
        // One history row per shipped migration.
        assertThat(rs.getInt(1)).isEqualTo(11);
      }
    }
    new MigrationRunner(dataSource).validate();
  }

  @Test
  void emittedMigrationSqlAppliesToACleanSchema() throws SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("DROP TABLE IF EXISTS threadmill_mutexes CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_concurrency_workflow_holds CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_concurrency_groups CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_dedup_keys CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_cron_task_ownership CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_cron_task_state CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_cron_tasks CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_jobs CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_nodes CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_leases CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_metadata CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_job_counts CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_queue_counts CASCADE");
      st.execute("DROP FUNCTION IF EXISTS threadmill_maintain_queue_counts() CASCADE");
      st.execute("DROP FUNCTION IF EXISTS threadmill_adjust_queue_count(TEXT, BIGINT) CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_queue_pauses CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_schema_history CASCADE");
    }

    String sql = new MigrationRunner(dataSource).emitCleanInstallSql();
    assertThat(sql)
        .contains("V1__baseline.sql")
        .contains("V2__cron_task_overrides.sql")
        .contains("V3__integrity_constraints.sql")
        .contains("V4__cron_state_timing_fingerprint.sql")
        .contains("V5__cron_state_nudge.sql")
        .contains("V6__cron_task_exclusive.sql")
        .contains("V7__execution_revision.sql");

    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute(sql);
      try (ResultSet rs = st.executeQuery("SELECT count(*) FROM threadmill_schema_history")) {
        assertThat(rs.next()).isTrue();
        // One history row per shipped migration.
        assertThat(rs.getInt(1)).isEqualTo(11);
      }
      try (ResultSet rs = st.executeQuery("SELECT count(*) FROM threadmill_job_counts")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt(1))
            .isEqualTo(JobState.values().length * 16); // 16 counter shards per state
      }
    }
  }

  @Test
  void validationPassesAfterMigrate() {
    new MigrationRunner(dataSource).validate();
  }

  @Test
  void migrateFailsFastWhenHistoryContainsAVersionThisBinaryDoesNotShip() throws SQLException {
    // Simulate a binary downgrade: a newer binary applied a future version.
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("INSERT INTO threadmill_schema_history (version, description, checksum) "
          + "VALUES (999, 'future', 'deadbeef')");
    }
    try {
      assertThatThrownBy(() -> new MigrationRunner(dataSource).migrate())
          .isInstanceOf(MigrationRunner.MigrationException.class)
          .hasMessageContaining("999")
          .hasMessageContaining("newer");
    } finally {
      try (Connection conn = dataSource.getConnection();
          Statement st = conn.createStatement()) {
        st.execute("DELETE FROM threadmill_schema_history WHERE version = 999");
      }
    }
  }

  @Test
  void validateFailsWhenAnAppliedMigrationFileWasEditedInPlace() throws SQLException {
    // Tamper with the stored checksum to mimic an edited migration file.
    String original;
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs =
            st.executeQuery("SELECT checksum FROM threadmill_schema_history WHERE version = 1")) {
      rs.next();
      original = rs.getString(1);
      assertThat(original).isNotBlank();
    }
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("UPDATE threadmill_schema_history SET checksum = 'tampered' WHERE version = 1");
    }
    try {
      assertThatThrownBy(() -> new MigrationRunner(dataSource).validate())
          .isInstanceOf(MigrationRunner.MigrationException.class)
          .hasMessageContaining("edited after it was applied");
    } finally {
      try (Connection conn = dataSource.getConnection();
          PreparedStatement ps = conn.prepareStatement(
              "UPDATE threadmill_schema_history SET checksum = ? WHERE version = 1")) {
        ps.setString(1, original);
        ps.executeUpdate();
      }
    }
  }

  @Test
  void migrateAlsoFailsWhenAnAppliedMigrationFileWasEditedInPlace() throws SQLException {
    String original;
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs =
            st.executeQuery("SELECT checksum FROM threadmill_schema_history WHERE version = 1")) {
      rs.next();
      original = rs.getString(1);
    }
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("UPDATE threadmill_schema_history SET checksum = 'tampered' WHERE version = 1");
    }
    try {
      assertThatThrownBy(() -> new MigrationRunner(dataSource).migrate())
          .isInstanceOf(MigrationRunner.MigrationException.class)
          .hasMessageContaining("edited after it was applied");
    } finally {
      try (Connection conn = dataSource.getConnection();
          PreparedStatement ps = conn.prepareStatement(
              "UPDATE threadmill_schema_history SET checksum = ? WHERE version = 1")) {
        ps.setString(1, original);
        ps.executeUpdate();
      }
    }
  }

  @Test
  void integrityMigrationRejectsInvalidScalarAndConcurrencyState() throws SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      assertThatThrownBy(() -> st.execute("INSERT INTO threadmill_concurrency_groups "
              + "(concurrency_key, exclusive_in_flight, shared_in_flight, last_modified) "
              + "VALUES ('invalid', -1, 0, now())"))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("threadmill_concurrency_groups_exclusive_check");
      assertThatThrownBy(
              () -> st.execute(
                  "INSERT INTO threadmill_cron_tasks "
                      + "(name, trigger_kind, trigger_value, handler_signature, payload_type_tag, "
                      + "payload_serialized, missed_run_policy) VALUES "
                      + "('invalid', 'UNKNOWN', 'PT1M', 'example.Handler', 'example.Payload', '{}', 'DROP')"))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("threadmill_cron_tasks_trigger_kind_check");
    }
  }

  @Test
  void validationFailsWhenHistoryTableIsMissing() throws SQLException {
    dropSchemaObjects();

    assertThatThrownBy(() -> new MigrationRunner(dataSource).validate())
        .isInstanceOf(MigrationRunner.MigrationException.class)
        .hasMessageContaining("threadmill_schema_history");
  }

  @Test
  void validationFailsWhenHistoryIsInconsistent() throws SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute(
          "UPDATE threadmill_schema_history SET description = 'old baseline' WHERE version = 1");
    }

    try {
      assertThatThrownBy(() -> new MigrationRunner(dataSource).validate())
          .isInstanceOf(MigrationRunner.MigrationException.class)
          .hasMessageContaining("expected 'baseline'");
    } finally {
      try (Connection conn = dataSource.getConnection();
          Statement st = conn.createStatement()) {
        st.execute(
            "UPDATE threadmill_schema_history SET description = 'baseline' WHERE version = 1");
      }
    }
  }

  @Test
  void dropThreadmillObjectsAllowsCleanReinitialize() throws SQLException {
    new MigrationRunner(dataSource).dropThreadmillObjects();

    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT to_regclass('threadmill_jobs')")) {
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString(1)).isNull();
    }

    new MigrationRunner(dataSource).migrate();
    new MigrationRunner(dataSource).validate();
  }

  @Test
  void migrationBootstrapCommitsWhenConnectionsDefaultToNonAutoCommit() throws SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("ALTER TABLE threadmill_schema_history DROP COLUMN checksum");
    }

    new MigrationRunner(new NonAutoCommitDataSource(dataSource)).migrate();

    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT checksum FROM threadmill_schema_history LIMIT 1")) {
      assertThat(rs.next()).isTrue();
    }
  }

  @Test
  void schemaDropAndRemigrateCommitWhenConnectionsDefaultToNonAutoCommit() throws SQLException {
    var runner = new MigrationRunner(new NonAutoCommitDataSource(dataSource));
    runner.dropThreadmillObjects();

    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT to_regclass('threadmill_jobs')")) {
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString(1)).isNull();
    }

    runner.migrate();
    new MigrationRunner(dataSource).validate();
  }

  @Test
  void concurrentCleanSchemaMigrationsAreSerialized() throws Exception {
    dropSchemaObjects();

    var start = new CountDownLatch(1);
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      Future<?> first = executor.submit(() -> {
        start.await();
        new MigrationRunner(dataSource).migrate();
        return null;
      });
      Future<?> second = executor.submit(() -> {
        start.await();
        new MigrationRunner(dataSource).migrate();
        return null;
      });
      start.countDown();
      first.get(60, TimeUnit.SECONDS);
      second.get(60, TimeUnit.SECONDS);
      executor.shutdown();
      assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }

    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT count(*) FROM threadmill_schema_history")) {
      assertThat(rs.next()).isTrue();
      // One history row per shipped migration.
      assertThat(rs.getInt(1)).isEqualTo(11);
    }
  }

  @Test
  void concurrencyPendingCheckUsesPartialIndex() throws SQLException {
    JobStore store = store();
    var base = Instant.now().minusSeconds(5);
    for (int i = 0; i < 100; i++) {
      store.insert(Job.builder()
          .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
          .concurrencyKey("project:hot")
          .concurrencyMode(i % 10 == 0 ? ConcurrencyMode.EXCLUSIVE : ConcurrencyMode.SHARED)
          .createdAt(base.plusMillis(i))
          .build());
    }

    JobId candidateId = JobId.newId();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement("EXPLAIN (FORMAT TEXT) SELECT EXISTS ("
            + "SELECT 1 FROM threadmill_jobs "
            + "WHERE concurrency_key = ? "
            + "AND concurrency_mode = 'EXCLUSIVE' "
            + "AND state IN ('ENQUEUED','SCHEDULED','AWAITING') "
            + "AND (current_state_at < ? OR (current_state_at = ? AND id < ?)))")) {
      ps.setString(1, "project:hot");
      ps.setTimestamp(2, Timestamp.from(base.plusSeconds(1)));
      ps.setTimestamp(3, Timestamp.from(base.plusSeconds(1)));
      ps.setObject(4, candidateId.asUuid());
      try (ResultSet rs = ps.executeQuery()) {
        var plan = new StringBuilder();
        while (rs.next()) plan.append(rs.getString(1)).append('\n');
        // Either pending partial index is a win — since V4 the planner
        // rightly prefers the dedicated exclusive-pending index for an
        // EXCLUSIVE-filtered check; what must never appear is a scan of
        // the jobs table itself.
        assertThat(plan.toString())
            .containsAnyOf(
                "threadmill_jobs_concurrency_pending_idx", "threadmill_jobs_exclusive_pending_idx");
      }
    }
  }

  @Test
  void batchedConcurrencyPendingLookupUsesPartialIndex() throws SQLException {
    JobStore store = store();
    var base = Instant.now().minusSeconds(5);
    for (int i = 0; i < 100; i++) {
      store.insert(Job.builder()
          .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
          .concurrencyKey(i % 2 == 0 ? "project:hot" : "project:warm")
          .concurrencyMode(i % 10 == 0 ? ConcurrencyMode.EXCLUSIVE : ConcurrencyMode.SHARED)
          .createdAt(base.plusMillis(i))
          .build());
    }

    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("SET enable_seqscan = off");
      var keyArray = conn.createArrayOf("text", new String[] {"project:hot", "project:warm"});
      try {
        // The plain earliest-pending head probe must ride the pending
        // partial index; the EXCLUSIVE-only probe must ride the V4
        // exclusive-pending index. Both are LATERAL head probes with
        // LIMIT 1 — DISTINCT ON was retired because it scans the whole
        // pending population per call (no per-group early termination).
        try (PreparedStatement ps = conn.prepareStatement("EXPLAIN (FORMAT TEXT) "
            + "SELECT f.* FROM unnest(?) AS k(key) "
            + "CROSS JOIN LATERAL (SELECT concurrency_key, current_state_at, id FROM threadmill_jobs "
            + "WHERE concurrency_key = k.key "
            + "AND state IN ('ENQUEUED','SCHEDULED','AWAITING') "
            + "ORDER BY current_state_at, id LIMIT 1) f")) {
          ps.setArray(1, keyArray);
          try (ResultSet rs = ps.executeQuery()) {
            var plan = new StringBuilder();
            while (rs.next()) plan.append(rs.getString(1)).append('\n');
            assertThat(plan.toString()).contains("threadmill_jobs_concurrency_pending_idx");
          }
        }
        try (PreparedStatement ps = conn.prepareStatement("EXPLAIN (FORMAT TEXT) "
            + "SELECT f.* FROM unnest(?) AS k(key) "
            + "CROSS JOIN LATERAL (SELECT concurrency_key, current_state_at, id FROM threadmill_jobs "
            + "WHERE concurrency_key = k.key "
            + "AND concurrency_mode = 'EXCLUSIVE' "
            + "AND state IN ('ENQUEUED','SCHEDULED','AWAITING') "
            + "ORDER BY current_state_at, id LIMIT 1) f")) {
          ps.setArray(1, keyArray);
          try (ResultSet rs = ps.executeQuery()) {
            var plan = new StringBuilder();
            while (rs.next()) plan.append(rs.getString(1)).append('\n');
            assertThat(plan.toString()).contains("threadmill_jobs_exclusive_pending_idx");
          }
        }
      } finally {
        keyArray.free();
      }
    }
  }

  @Test
  void workflowOutstandingCountUsesPartialIndex() throws SQLException {
    JobStore store = store();
    Job root = Job.builder()
        .spec(JobSpec.of("com.example.Root", new JobArgument("java.lang.String", "\"x\"")))
        .concurrencyKey("project:workflow")
        .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
        .build();
    store.insert(root);
    for (int i = 0; i < 80; i++) {
      store.insert(awaitingChildOf(root, i));
    }

    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("SET enable_seqscan = off");
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN (FORMAT TEXT) SELECT count(*) "
          + "FROM threadmill_jobs "
          + "WHERE concurrency_key = ? AND workflow_root_id = ? "
          + "AND state NOT IN ('SUCCEEDED','FAILED','DELETED','QUARANTINED')")) {
        ps.setString(1, "project:workflow");
        ps.setObject(2, root.id().asUuid());
        try (ResultSet rs = ps.executeQuery()) {
          var plan = new StringBuilder();
          while (rs.next()) plan.append(rs.getString(1)).append('\n');
          assertThat(plan.toString()).contains("threadmill_jobs_workflow_outstanding_idx");
        }
      }
    }
  }

  @Test
  void keysetPagedKeyEnumerationUsesQueuePendingIndex() throws SQLException {
    var store = store();
    for (int i = 0; i < 80; i++) {
      store.insert(keyedJob("plan:%04d".formatted(i)));
    }

    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("SET enable_seqscan = off");
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN (FORMAT TEXT) "
          + "WITH RECURSIVE keys(k) AS ("
          + "  (SELECT concurrency_key FROM threadmill_jobs "
          + "   WHERE queue = ? AND concurrency_key IS NOT NULL AND state = 'ENQUEUED' "
          + "   AND concurrency_key > ? ORDER BY concurrency_key LIMIT 1) "
          + "  UNION ALL "
          + "  SELECT (SELECT j.concurrency_key FROM threadmill_jobs j "
          + "          WHERE j.queue = ? AND j.concurrency_key > keys.k "
          + "          AND j.state = 'ENQUEUED' ORDER BY j.concurrency_key LIMIT 1) "
          + "  FROM keys WHERE keys.k IS NOT NULL) "
          + "SELECT k FROM keys WHERE k IS NOT NULL LIMIT ?")) {
        ps.setString(1, "default");
        ps.setString(2, "plan:0000");
        ps.setString(3, "default");
        ps.setInt(4, PostgresJobStore.MAX_PENDING_KEYS_PER_PASS + 1);
        try (ResultSet rs = ps.executeQuery()) {
          var plan = new StringBuilder();
          while (rs.next()) plan.append(rs.getString(1)).append('\n');
          assertThat(plan.toString()).contains("threadmill_jobs_queue_pending_idx");
        }
      }
    }
  }

  @Test
  void orphanRecoveryUsesProcessingLivenessIndex() throws SQLException {
    JobStore store = store();
    var heartbeat = Instant.now().minusSeconds(120);
    for (int i = 0; i < 120; i++) {
      store.insert(Jobs.enqueued("com.example.H"));
    }
    store.claimReady(NodeId.newId(), "default", 120, heartbeat);

    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("SET enable_seqscan = off");
      try (PreparedStatement ps = conn.prepareStatement(
          "EXPLAIN (FORMAT TEXT) "
              + "SELECT body FROM threadmill_jobs WHERE state = 'PROCESSING' "
              + "AND GREATEST(owner_heartbeat_at, COALESCE(last_checkin_at, owner_heartbeat_at)) <= ? "
              + "ORDER BY GREATEST(owner_heartbeat_at, COALESCE(last_checkin_at, owner_heartbeat_at)) LIMIT ?")) {
        ps.setTimestamp(1, Timestamp.from(Instant.now()));
        ps.setInt(2, 10);
        try (ResultSet rs = ps.executeQuery()) {
          var plan = new StringBuilder();
          while (rs.next()) plan.append(rs.getString(1)).append('\n');
          assertThat(plan.toString()).contains("threadmill_jobs_processing_liveness_idx");
        }
      }
    }
  }

  @Test
  void workflowSuccessorLookupUsesParentIndex() throws SQLException {
    JobStore store = store();
    Job parent = Jobs.enqueued("com.example.Parent");
    Job otherParent = Jobs.enqueued("com.example.OtherParent");
    store.insert(parent);
    store.insert(otherParent);

    for (int i = 0; i < 80; i++) {
      store.insert(awaitingChildOf(i % 3 == 0 ? otherParent : parent, i));
    }

    assertThat(store.findAwaitingByParent(parent.id(), 10))
        .hasSize(10)
        .allSatisfy(job -> assertThat(job.relationship())
            .hasValueSatisfying(
                relationship -> assertThat(relationship.parentId()).isEqualTo(parent.id())));

    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("SET enable_seqscan = off");
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN (FORMAT TEXT) "
          + "SELECT body FROM threadmill_jobs WHERE state = 'AWAITING' AND parent_job_id = ? "
          + "ORDER BY current_state_at, id LIMIT ?")) {
        ps.setObject(1, parent.id().asUuid());
        ps.setInt(2, 10);
        try (ResultSet rs = ps.executeQuery()) {
          var plan = new StringBuilder();
          while (rs.next()) plan.append(rs.getString(1)).append('\n');
          assertThat(plan.toString()).contains("threadmill_jobs_awaiting_parent_idx");
        }
      }
    }
  }

  @Test
  void cronTaskDefinitionAndScheduleStateRoundTrip() {
    JobStore store = store();
    CronTask task = sampleCronTask("nightly-cleanup");
    var next = Instant.parse("2026-05-16T09:00:00Z");
    var last = Instant.parse("2026-05-15T09:00:00Z");

    store.upsertCronTask(task);
    store.upsertCronTaskState(new CronTaskScheduleState(
        task.name(),
        last,
        UUID.randomUUID(),
        next,
        UUID.randomUUID(),
        CronTaskScheduleState.timingFingerprintOf(task)));

    assertThat(store.findCronTask(task.name())).contains(task);
    assertThat(store.listCronTasks()).containsExactly(task);
    assertThat(store.findCronTaskState(task.name())).hasValueSatisfying(state -> {
      assertThat(state.lastRunAt()).isEqualTo(last);
      assertThat(state.nextRunAt()).isEqualTo(next);
      assertThat(state.inFlightJobId()).isNotNull();
    });

    store.deleteCronTask(task.name());
    assertThat(store.findCronTask(task.name())).isEmpty();
    assertThat(store.findCronTaskState(task.name())).isEmpty();
  }

  @Test
  void selfOwnedWritesCommitWhenConnectionsDefaultToNonAutoCommit() {
    var writer = new PostgresJobStore(new NonAutoCommitDataSource(dataSource));
    var observer = store();

    writer.pauseQueue("low-priority", "maintenance");
    assertThat(observer.listPausedQueues()).contains("low-priority");

    var task = sampleCronTask("non-auto-commit-task");
    writer.upsertCronTask(task);
    assertThat(observer.findCronTask(task.name())).contains(task);

    var nodeId = NodeId.newId();
    var heartbeat = Instant.parse("2026-08-11T12:00:00Z");
    writer.recordNodeHeartbeat(nodeId, heartbeat);
    assertThat(observer.readNodeHeartbeat(nodeId)).contains(heartbeat);

    assertThat(writer.tryAcquireMutex("non-auto-commit-mutex", "writer", Duration.ofMinutes(1)))
        .isTrue();
    assertThat(observer.tryAcquireMutex("non-auto-commit-mutex", "observer", Duration.ofMinutes(1)))
        .isFalse();

    writer.resumeQueue("low-priority");
  }

  @Test
  void mutexLeaseIsExclusiveReentrantAndExpires() throws InterruptedException {
    JobStore store = store();
    assertThat(store.tryAcquireMutex("billing-close", "node-a", Duration.ofMillis(75)))
        .isTrue();
    assertThat(store.tryAcquireMutex("billing-close", "node-b", Duration.ofSeconds(5)))
        .isFalse();
    assertThat(store.tryAcquireMutex("billing-close", "node-a", Duration.ofSeconds(5)))
        .isTrue();
    store.releaseMutex("billing-close", "wrong-holder");
    assertThat(store.tryAcquireMutex("billing-close", "node-b", Duration.ofMillis(75)))
        .isFalse();
    store.releaseMutex("billing-close", "node-a");
    assertThat(store.tryAcquireMutex("billing-close", "node-b", Duration.ofMillis(75)))
        .isTrue();
    Thread.sleep(220);
    assertThat(store.tryAcquireMutex("billing-close", "node-c", Duration.ofSeconds(5)))
        .isTrue();
  }

  private static CronTask sampleCronTask(String name) {
    return new CronTask(
        name,
        new CronTask.Trigger.Interval(Duration.ofMinutes(5)),
        "com.example.Cleanup",
        new JobArgument("java.lang.String", "\"payload\""),
        "system",
        7,
        CronTask.MissedRunPolicy.CATCH_UP,
        ZoneId.of("UTC"),
        true);
  }

  private static Job awaitingChildOf(Job parent, int index) {
    return Job.builder()
        .spec(JobSpec.of(
            "com.example.Child", new JobArgument("java.lang.String", "\"" + index + "\"")))
        .relationship(new JobRelationship(parent.id(), JobRelationship.Kind.WORKFLOW_STEP))
        .initialState(JobState.AWAITING)
        .createdAt(Instant.now().plusMillis(index))
        .build();
  }

  private static void dropSchemaObjects() throws SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("DROP TABLE IF EXISTS threadmill_mutexes CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_concurrency_workflow_holds CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_concurrency_groups CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_dedup_keys CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_cron_task_ownership CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_cron_task_state CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_cron_tasks CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_jobs CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_nodes CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_leases CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_metadata CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_job_counts CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_queue_counts CASCADE");
      st.execute("DROP FUNCTION IF EXISTS threadmill_maintain_queue_counts() CASCADE");
      st.execute("DROP FUNCTION IF EXISTS threadmill_adjust_queue_count(TEXT, BIGINT) CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_queue_pauses CASCADE");
      st.execute("DROP TABLE IF EXISTS threadmill_schema_history CASCADE");
    }
  }
}
