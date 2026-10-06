package com.hemju.threadmill.store.oracle;

import static com.hemju.threadmill.store.oracle.OracleJdbc.MAX_IN_LIST;
import static com.hemju.threadmill.store.oracle.OracleJdbc.PARENT_KEY_NOT_FOUND;
import static com.hemju.threadmill.store.oracle.OracleJdbc.UNIQUE_VIOLATION;
import static com.hemju.threadmill.store.oracle.OracleJdbc.bindStrings;
import static com.hemju.threadmill.store.oracle.OracleJdbc.bindUuids;
import static com.hemju.threadmill.store.oracle.OracleJdbc.bucket;
import static com.hemju.threadmill.store.oracle.OracleJdbc.chunks;
import static com.hemju.threadmill.store.oracle.OracleJdbc.getInstant;
import static com.hemju.threadmill.store.oracle.OracleJdbc.getUuid;
import static com.hemju.threadmill.store.oracle.OracleJdbc.hasErrorCode;
import static com.hemju.threadmill.store.oracle.OracleJdbc.placeholders;
import static com.hemju.threadmill.store.oracle.OracleJdbc.setInstant;
import static com.hemju.threadmill.store.oracle.OracleJdbc.setSeconds;
import static com.hemju.threadmill.store.oracle.OracleJdbc.setUuid;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobReplacement;
import com.hemju.threadmill.core.JobReplacements;
import com.hemju.threadmill.core.JobSnapshot;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.JobStateEntry;
import com.hemju.threadmill.core.Names;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.OversizedJobException;
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.engine.RemoteWakeChannel;
import com.hemju.threadmill.core.internal.ExecutionHeartbeats;
import com.hemju.threadmill.core.internal.RetentionPosition;
import com.hemju.threadmill.core.schedule.CronExpression;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.serialization.SerializationException;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.store.BulkInsertBudget;
import com.hemju.threadmill.core.store.JobSearch;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.core.store.JobStoreCapabilities;
import com.hemju.threadmill.core.store.Mutexes;
import com.hemju.threadmill.core.store.NodeHeartbeat;
import com.hemju.threadmill.core.store.RetentionCursor;
import com.hemju.threadmill.core.store.RetentionPage;

/**
 * Oracle Database (19c or later) implementation of {@link JobStore}.
 *
 * <p>Design highlights:
 * <ul>
 *   <li>The body column (JSON in a {@code CLOB}) is the <strong>source of
 *       truth</strong>; the indexed scalar columns are denormalized so hot
 *       queries hit indexes without parsing the body.</li>
 *   <li>Oracle has no partial indexes. Virtual columns of the form
 *       {@code CASE WHEN <predicate> THEN <column> END} (for example
 *       {@code keyed_queue}, {@code pending_key}) are indexed instead: rows
 *       that do not match the predicate have all-NULL keys, which a B-tree
 *       index does not store. Queries name these columns directly.</li>
 *   <li>{@link #claimReady} locks candidates with
 *       {@code FOR UPDATE SKIP LOCKED}, so contending workers never collide
 *       and never wait. Oracle forbids row limits on locking queries, so
 *       locking cursors are index-ordered and fetched only as far as the
 *       budget; Oracle locks {@code SKIP LOCKED} rows as they are fetched.</li>
 *   <li>Per-key candidate and admission lookups use plain index probes
 *       ({@code MIN}/{@code MAX}, {@code ROWNUM} top-n branches, and
 *       {@code EXISTS} range probes), never lateral joins: Oracle keeps a
 *       sort over each key's whole range inside a correlated top-n lateral
 *       view, so its cost would grow with each key's backlog.</li>
 *   <li>Every self-owned write uses an explicit transaction, independent of
 *       the {@link DataSource}'s default auto-commit mode, and is retried by
 *       {@link OracleDeadlockRetry} after {@code ORA-00060}.</li>
 *   <li>Per-state and per-queue counts come from sharded counter tables
 *       maintained by a trigger; a {@code COUNT(*)} would contend with claims.</li>
 *   <li>Only standard JDBC is used. The application supplies the driver.</li>
 * </ul>
 *
 * <p>There is no cross-node wake channel: Oracle has no lightweight
 * equivalent of PostgreSQL's {@code LISTEN}/{@code NOTIFY}, and wakes are
 * only latency hints. Same-JVM wakes still work; other nodes pick up new work
 * within their poll interval.
 */
public final class OracleJobStore implements JobStore {

  private static final Logger LOG = LoggerFactory.getLogger(OracleJobStore.class);
  private static final String MAINTENANCE_LEASE = "maintenance";

  /** Current server time as a UTC {@code TIMESTAMP}, matching the stored UTC wall time. */
  private static final String NOW_UTC = "SYS_EXTRACT_UTC(SYSTIMESTAMP)";

  /** States whose rows are finished and no longer hold claim-time concurrency. */
  private static final String TERMINAL_STATES = "('SUCCEEDED', 'FAILED', 'DELETED', 'QUARANTINED')";

  /** Keys enumerated per gathering pass; a per-queue cursor rotates later polls through the rest. */
  static final int MAX_PENDING_KEYS_PER_PASS = 128;

  /** Top-n branches per statement when gathering per-key or per-hold heads. */
  static final int MAX_BRANCHES_PER_STATEMENT = 64;

  /** Process-local fairness hints retained across dynamic queues. */
  private static final int MAX_TRACKED_PENDING_KEY_CURSORS = 1024;

  private static final String CLAIM_COLUMNS =
      "id, version, priority, concurrency_key, concurrency_mode, workflow_root_id, current_state_at";

  private static final String JOB_PROJECTION = "body, owner_heartbeat_at";

  private static final String JOB_INSERT = "INSERT INTO threadmill_jobs (id, state, queue, "
      + "priority, handler_signature, scheduled_at, owner_node_id, owner_heartbeat_at, "
      + "last_checkin_at, current_state_at, version, body, created_at, concurrency_key, "
      + "concurrency_mode, workflow_root_id, parent_job_id) "
      + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private static final String CRON_TASK_COLUMNS = "name, trigger_kind, trigger_value, "
      + "handler_signature, payload_type_tag, payload_serialized, queue, priority, timeout_seconds, "
      + "max_attempts, is_exclusive, missed_run_policy, time_zone, enabled";

  private final DataSource dataSource;
  private final OracleTransactionBoundary transactionBoundary;

  /** Boundary for self-owned writes that must never join an external transaction. */
  private final OracleTransactionBoundary owningBoundary;

  private final JobSerializer serializer;
  private final JobStoreCapabilities capabilities;
  private final OracleServer.Facts server;
  private final PendingKeyCursors pendingKeyCursors =
      new PendingKeyCursors(MAX_TRACKED_PENDING_KEY_CURSORS);

  private String idleGroupAfter;
  private String idleQueueAfter;

  /**
   * Create a store with the JSON serializer and default capabilities.
   *
   * @param dataSource host-owned connection source for a 19c+ AL32UTF8 database
   */
  public OracleJobStore(DataSource dataSource) {
    this(dataSource, new JsonJobSerializer(), JobStoreCapabilities.defaults());
  }

  /**
   * Create a store that owns its transactions.
   *
   * @param dataSource host-owned connection source for a 19c+ AL32UTF8 database
   * @param serializer the job wire-format serializer
   * @param capabilities size limits and feature flags
   */
  public OracleJobStore(
      DataSource dataSource, JobSerializer serializer, JobStoreCapabilities capabilities) {
    this(dataSource, serializer, capabilities, OracleTransactionBoundary.owning(dataSource));
  }

  /**
   * Create a store whose job writes run inside {@code transactionBoundary}.
   *
   * @param dataSource host-owned connection source for a 19c+ AL32UTF8 database
   * @param serializer the job wire-format serializer
   * @param capabilities size limits and feature flags
   * @param transactionBoundary boundary for enqueue writes, for example one that
   *     joins a framework-managed transaction
   * @throws com.hemju.threadmill.core.JobEngineFatalException if the server is
   *     older than 19c or its character set is not AL32UTF8
   */
  public OracleJobStore(
      DataSource dataSource,
      JobSerializer serializer,
      JobStoreCapabilities capabilities,
      OracleTransactionBoundary transactionBoundary) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    this.transactionBoundary = Objects.requireNonNull(transactionBoundary, "transactionBoundary");
    this.owningBoundary = OracleTransactionBoundary.owning(this.dataSource);
    this.serializer = Objects.requireNonNull(serializer, "serializer");
    this.capabilities = Objects.requireNonNull(capabilities, "capabilities");
    this.server = OracleServer.require(OracleServer.read(this.dataSource));
  }

  /**
   * Validate that {@code dataSource} points at a supported Oracle server.
   *
   * @param dataSource the connection source to check
   * @throws com.hemju.threadmill.core.JobEngineFatalException if the server is
   *     older than 19c or its character set is not AL32UTF8
   */
  public static void requireSupportedServer(DataSource dataSource) {
    OracleServer.require(OracleServer.read(dataSource));
  }

  // ---------------------------------------------------------------- capabilities

  @Override
  public JobStoreCapabilities capabilities() {
    return capabilities;
  }

  /**
   * Liveness probe for the dispatcher's store-outage circuit breaker: a real
   * round trip that fails while the database is unreachable.
   */
  @Override
  public void verifyWritable() {
    try (Connection conn = dataSource.getConnection();
        var st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT 1 FROM dual")) {
      rs.next();
    } catch (SQLException e) {
      throw new JdbcException("verifyWritable probe failed", e);
    }
  }

  @Override
  public String describe() {
    return server.describe();
  }

  @Override
  public JobStore delegate() {
    return this;
  }

  @Override
  public boolean supportsExternalTransactions() {
    return transactionBoundary.supportsExternalTransactions();
  }

  /** Oracle offers no lightweight cross-session notification; wakes fall back to polling. */
  @Override
  public Optional<RemoteWakeChannel> createRemoteWakeChannel(String channelName) {
    return Optional.empty();
  }

  private <T> T writeTransaction(OracleConnectionWork<T> work) throws SQLException {
    if (transactionBoundary.externallyManagedTransactionActive()) {
      return transactionBoundary.inTransaction(work);
    }
    return OracleDeadlockRetry.run(() -> transactionBoundary.inTransaction(work));
  }

  /**
   * Run one self-owned write in a transaction Threadmill commits, never the
   * caller's. The explicit boundary is required even for a single statement:
   * a pool may hand out {@code autoCommit=false} connections, and closing such
   * a connection does not commit it. Spring defers nudges to
   * {@code afterCommit}, where a joining boundary would hand back the caller's
   * already-committed connection and nobody would commit the nudge.
   */
  private <T> T ownedTransaction(OracleConnectionWork<T> work) throws SQLException {
    return OracleDeadlockRetry.run(() -> owningBoundary.inTransaction(work));
  }

  // ---------------------------------------------------------------- single-job

  @Override
  public void insert(Job job) {
    Objects.requireNonNull(job, "job");
    Names.requireName("queue", job.queue());
    long version = 1L;
    try {
      writeTransaction(conn -> {
        JobSnapshot snapshot = snapshotForInsert(conn, job, version);
        String body = serializer.serializeJob(snapshot, capabilities);
        insertSnapshot(
            conn, snapshot, body, lastTransitionTime(snapshot, snapshot.currentState()), version);
        noteInsertedWorkflowDescendant(conn, snapshot);
        return null;
      });
    } catch (SQLException e) {
      if (hasErrorCode(e, UNIQUE_VIOLATION)) {
        throw new IllegalStateException("Job already exists: " + job.id(), e);
      }
      throw new JdbcException("Insert failed", e);
    }
    job.adoptVersion(version);
  }

  @Override
  public List<JobId> insertAll(List<Job> jobsToInsert) {
    Objects.requireNonNull(jobsToInsert, "jobs");
    if (jobsToInsert.isEmpty()) return List.of();
    var budget = new BulkInsertBudget(jobsToInsert.size(), capabilities);
    // Pre-flight: serialize every snapshot before any write, so an oversized
    // job rejects the whole batch without mutating any input version.
    for (var job : jobsToInsert) {
      Objects.requireNonNull(job, "job");
      Names.requireName("queue", job.queue());
      budget.include(serializer.serializeJob(job.snapshot(), capabilities));
    }
    long version = 1L;
    try {
      writeTransaction(conn -> {
        // Re-snapshot inside the transaction so workflow_root_id resolves
        // against live store state.
        var finalBudget = new BulkInsertBudget(jobsToInsert.size(), capabilities);
        var snapshots = new ArrayList<JobSnapshot>(jobsToInsert.size());
        var bodies = new ArrayList<String>(jobsToInsert.size());
        for (var job : jobsToInsert) {
          JobSnapshot snapshot = snapshotForInsert(conn, job, version);
          String body = serializer.serializeJob(snapshot, capabilities);
          finalBudget.include(body);
          snapshots.add(snapshot);
          bodies.add(body);
        }
        try (var clobs = new OracleClobs();
            PreparedStatement ps = conn.prepareStatement(JOB_INSERT)) {
          for (int i = 0; i < snapshots.size(); i++) {
            JobSnapshot snapshot = snapshots.get(i);
            bindJobInsert(
                clobs,
                ps,
                snapshot,
                bodies.get(i),
                lastTransitionTime(snapshot, snapshot.currentState()),
                version);
            ps.addBatch();
          }
          ps.executeBatch();
        }
        // Lock the distinct concurrency groups once, in sorted order: locking
        // per job in batch order lets concurrent batches with reversed key
        // orders deadlock (fatal when joined to a caller's transaction).
        var keys = new TreeSet<String>();
        for (JobSnapshot snapshot : snapshots) {
          if (snapshot.concurrencyKey() != null) keys.add(snapshot.concurrencyKey());
        }
        if (!keys.isEmpty()) {
          lockConcurrencyGroups(conn, keys);
          for (JobSnapshot snapshot : snapshots) {
            incrementWorkflowHoldOutstanding(conn, snapshot);
          }
        }
        return null;
      });
    } catch (SQLException e) {
      if (hasErrorCode(e, UNIQUE_VIOLATION)) {
        throw new IllegalStateException("Duplicate job id in batch", e);
      }
      throw new JdbcException("insertAll failed", e);
    }
    var ids = new ArrayList<JobId>(jobsToInsert.size());
    for (var job : jobsToInsert) {
      job.adoptVersion(version);
      ids.add(job.id());
    }
    return List.copyOf(ids);
  }

  @Override
  public EnqueueResult enqueueIfAbsent(Job job, String dedupKey, Duration ttl, Instant now) {
    Objects.requireNonNull(job, "job");
    Objects.requireNonNull(ttl, "ttl");
    Objects.requireNonNull(now, "now");
    Names.requireName("queue", job.queue());
    if (dedupKey == null || dedupKey.isBlank()) {
      throw new IllegalArgumentException("dedupKey must not be blank");
    }
    long version = 1L;
    try {
      EnqueueResult result = writeTransaction(conn -> {
        JobSnapshot snapshot = snapshotForInsert(conn, job, version);
        String body = serializer.serializeJob(snapshot, capabilities);
        Instant currentStateAt = lastTransitionTime(snapshot, snapshot.currentState());
        Optional<JobId> existing = findActiveDedup(conn, job.queue(), dedupKey, now);
        if (existing.isPresent()) {
          return new EnqueueResult.Coalesced(existing.get());
        }
        // A concurrent producer may commit the same dedup key while this
        // insert waits on its unique index. Oracle rolls back only the failed
        // statement, so undo this job's writes to a savepoint and coalesce
        // onto the winner — inside a joined caller transaction as well.
        Savepoint beforeInsert = conn.setSavepoint();
        insertSnapshot(conn, snapshot, body, currentStateAt, version);
        noteInsertedWorkflowDescendant(conn, snapshot);
        try (PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO threadmill_dedup_keys (queue, dedup_key, job_id, expires_at) "
                + "VALUES (?, ?, ?, ?)")) {
          ps.setString(1, job.queue());
          ps.setString(2, dedupKey);
          setUuid(ps, 3, job.id().asUuid());
          setInstant(ps, 4, now.plus(ttl));
          ps.executeUpdate();
        } catch (SQLException e) {
          if (!hasErrorCode(e, UNIQUE_VIOLATION)) throw e;
          conn.rollback(beforeInsert);
          Optional<JobId> winner = findActiveDedup(conn, job.queue(), dedupKey, now);
          if (winner.isPresent()) return new EnqueueResult.Coalesced(winner.get());
          throw e;
        }
        return new EnqueueResult.Created(job.id());
      });
      if (result instanceof EnqueueResult.Created) {
        job.adoptVersion(version);
      }
      return result;
    } catch (SQLException e) {
      throw new JdbcException("enqueueIfAbsent failed", e);
    }
  }

  @Override
  public Optional<Job> findById(JobId id) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(
            "SELECT " + JOB_PROJECTION + " FROM threadmill_jobs WHERE id = ?")) {
      setUuid(ps, 1, id.asUuid());
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) return Optional.empty();
        return Optional.of(readJobWithHeartbeat(rs));
      }
    } catch (SQLException e) {
      throw new JdbcException("findById failed", e);
    }
  }

  @Override
  public void saveAtomic(Job job, long expectedVersion) {
    Objects.requireNonNull(job, "job");
    long nextVersion = expectedVersion + 1;
    JobSnapshot snapshot = withVersion(job, nextVersion);
    // Serialize before any database work so OversizedJobException cannot corrupt state.
    String body = serializer.serializeJob(snapshot, capabilities);
    Instant currentStateAt = lastTransitionTime(snapshot, snapshot.currentState());
    boolean saved;
    try {
      saved = ownedTransaction(conn -> {
        JobSnapshot oldSnapshot;
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT body, version FROM threadmill_jobs WHERE id = ? FOR UPDATE")) {
          setUuid(ps, 1, snapshot.id().asUuid());
          try (ResultSet rs = ps.executeQuery()) {
            if (!rs.next() || rs.getLong(2) != expectedVersion) {
              return false;
            }
            oldSnapshot = serializer.deserializeJob(rs.getString(1)).snapshot();
          }
        }
        if (oldSnapshot.concurrencyKey() != null) {
          lockConcurrencyGroup(conn, oldSnapshot.concurrencyKey());
        }
        adjustWorkflowHoldOnTransition(conn, oldSnapshot, snapshot.currentState());
        try (var clobs = new OracleClobs();
            PreparedStatement ps = conn.prepareStatement("UPDATE threadmill_jobs SET "
                + "state = ?, queue = ?, priority = ?, handler_signature = ?, "
                + "scheduled_at = ?, owner_node_id = ?, owner_heartbeat_at = ?, last_checkin_at = ?, "
                + "current_state_at = ?, version = ?, body = ?, "
                + "concurrency_key = ?, concurrency_mode = ?, workflow_root_id = ?, parent_job_id = ? "
                + "WHERE id = ? AND version = ?")) {
          ps.setString(1, snapshot.currentState().name());
          ps.setString(2, snapshot.queue());
          ps.setInt(3, snapshot.priority());
          ps.setString(4, snapshot.spec().handlerType());
          setInstant(ps, 5, snapshot.scheduledFor());
          setUuid(
              ps,
              6,
              snapshot.ownerNodeId() == null ? null : snapshot.ownerNodeId().asUuid());
          setInstant(ps, 7, snapshot.ownerHeartbeatAt());
          setInstant(ps, 8, snapshot.lastCheckinAt());
          setInstant(ps, 9, currentStateAt);
          ps.setLong(10, nextVersion);
          clobs.bind(ps, 11, body);
          setNullableConcurrency(ps, 12, snapshot.concurrencyKey(), snapshot.concurrencyMode());
          setUuid(ps, 14, snapshot.workflowRootId().asUuid());
          setUuid(ps, 15, parentJobId(snapshot));
          setUuid(ps, 16, snapshot.id().asUuid());
          ps.setLong(17, expectedVersion);
          return ps.executeUpdate() > 0;
        }
      });
    } catch (SQLException e) {
      throw new JdbcException("saveAtomic failed", e);
    }
    if (!saved) {
      throw new StaleJobException(job.id(), expectedVersion);
    }
    job.adoptVersion(nextVersion);
  }

  @Override
  public boolean softDelete(JobId id) {
    try {
      return ownedTransaction(conn -> {
        String body;
        long version;
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT body, version FROM threadmill_jobs WHERE id = ? FOR UPDATE")) {
          setUuid(ps, 1, id.asUuid());
          try (ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) return false;
            body = rs.getString(1);
            version = rs.getLong(2);
          }
        }
        Job job = serializer.deserializeJob(body);
        if (job.currentState() == JobState.DELETED) {
          return false;
        }
        JobSnapshot oldSnapshot = job.snapshot();
        if (oldSnapshot.concurrencyKey() != null) {
          lockConcurrencyGroup(conn, oldSnapshot.concurrencyKey());
        }
        job.transitionTo(JobState.DELETED, Instant.now(), "user.delete", null);
        long nextVersion = version + 1;
        JobSnapshot snapshot = withVersion(job, nextVersion);
        String newBody = serializer.serializeJob(snapshot, capabilities);
        adjustWorkflowHoldOnTransition(conn, oldSnapshot, JobState.DELETED);
        try (var clobs = new OracleClobs();
            PreparedStatement ps = conn.prepareStatement("UPDATE threadmill_jobs SET state = ?, "
                + "version = ?, body = ?, current_state_at = ? WHERE id = ?")) {
          ps.setString(1, JobState.DELETED.name());
          ps.setLong(2, nextVersion);
          clobs.bind(ps, 3, newBody);
          setInstant(ps, 4, lastTransitionTime(snapshot, JobState.DELETED));
          setUuid(ps, 5, id.asUuid());
          ps.executeUpdate();
        }
        return true;
      });
    } catch (SQLException e) {
      throw new JdbcException("softDelete failed", e);
    }
  }

  // ---------------------------------------------------------------- claim & heartbeat

  @Override
  public List<Job> claimReady(NodeId nodeId, String queue, int max, Instant heartbeatAt) {
    Objects.requireNonNull(nodeId, "nodeId");
    Names.requireName("queue", queue);
    Objects.requireNonNull(heartbeatAt, "heartbeatAt");
    if (max <= 0) return List.of();
    if (isQueuePaused(queue)) return List.of();
    int cap = Math.min(max, capabilities.maxClaimBatch());
    try {
      return ownedTransaction(conn -> {
        List<Job> result = new ArrayList<>();
        // Version-matched as defense in depth: correctness rests on the
        // SKIP LOCKED row locks taken while gathering, but if a refactor ever
        // fetched candidates without them, this turns a silent double claim
        // into a loud failure.
        try (var clobs = new OracleClobs();
            PreparedStatement ps = conn.prepareStatement("UPDATE threadmill_jobs SET "
                + "state = 'PROCESSING', owner_node_id = ?, owner_heartbeat_at = ?, "
                + "last_checkin_at = NULL, execution_revision = 0, current_state_at = ?, version = ?, "
                + "body = ? WHERE id = ? AND version = ?")) {
          var alreadyBatched = new HashSet<UUID>();
          while (result.size() < cap) {
            // Rows batched in an earlier pass are still ENQUEUED (the batch
            // executes after the loop) and locked by this transaction, so
            // SKIP LOCKED does not hide them from the re-gather.
            List<PendingClaim> pending =
                lockClaimCandidates(conn, queue, cap - result.size()).stream()
                    .filter(p -> !alreadyBatched.contains(p.id()))
                    .toList();
            if (pending.isEmpty()) {
              break;
            }
            List<PendingClaim> claimable = claimableCandidates(conn, pending, cap - result.size());
            Map<UUID, String> bodies = fetchBodies(conn, claimable);
            int quarantined = 0;
            int before = result.size();
            for (var candidate : claimable) {
              if (result.size() >= cap) break;
              String original = bodies.get(candidate.id());
              Job job;
              try {
                job = serializer.deserializeJob(original);
              } catch (RuntimeException corrupt) {
                // An unreadable body must not fail the whole claim and wedge
                // the queue: quarantine it and continue with the rest.
                quarantineUnreadable(conn, candidate, heartbeatAt, original);
                quarantined++;
                continue;
              }
              job.transitionTo(JobState.PROCESSING, heartbeatAt, "engine.claim", null);
              job.assignOwner(nodeId, heartbeatAt);
              job.incrementAttempts();
              long nextVersion = candidate.version() + 1;
              String newBody;
              try {
                newBody = serializer.serializeJob(withVersion(job, nextVersion), capabilities);
              } catch (OversizedJobException | SerializationException poison) {
                quarantineUnreadable(conn, candidate, heartbeatAt, original);
                quarantined++;
                continue;
              }
              acquireWorkflowHold(conn, job.snapshot());
              setUuid(ps, 1, nodeId.asUuid());
              setInstant(ps, 2, heartbeatAt);
              setInstant(ps, 3, heartbeatAt);
              ps.setLong(4, nextVersion);
              clobs.bind(ps, 5, newBody);
              setUuid(ps, 6, candidate.id());
              ps.setLong(7, candidate.version());
              ps.addBatch();
              alreadyBatched.add(candidate.id());
              result.add(serializer.deserializeJob(newBody));
            }
            // Every gathered head is concurrency-inadmissible right now. The
            // keyed cursor has already advanced, so the next poll sees the
            // next page; chasing pages here would grow the lock set with key
            // count. Quarantines are progress: the poison left ENQUEUED.
            if (result.size() == before && quarantined == 0) {
              break;
            }
          }
          if (!alreadyBatched.isEmpty()) {
            for (int count : ps.executeBatch()) {
              if (count != 1) {
                throw new IllegalStateException("Claim UPDATE matched " + count + " rows — the "
                    + "SKIP LOCKED row lock no longer guarantees claim exclusivity");
              }
            }
          }
        }
        return result;
      });
    } catch (SQLException e) {
      throw new JdbcException("claimReady failed", e);
    }
  }

  /**
   * Gather and row-lock claim candidates with cost bounded by claimable work,
   * never by backlog depth:
   *
   * <ul>
   *   <li><b>Unkeyed lane</b> — an index-ordered {@code FOR UPDATE SKIP
   *       LOCKED} cursor over {@code threadmill_jobs_unkeyed_idx}, fetched
   *       only as far as the page budget.</li>
   *   <li><b>Keyed lane</b> — distinct pending keys come from a recursive
   *       {@code MIN} loose scan (one index probe per key); each key
   *       contributes its earliest ENQUEUED heads through an index-ordered
   *       top-n probe in the engine's {@code (current_state_at, id)} in-key
   *       order.</li>
   *   <li><b>Hold lane</b> — members of an active workflow hold are
   *       admissible regardless of their position in the key's pending
   *       order, so each active hold's earliest members are gathered by
   *       {@code (concurrency_key, workflow_root_id)} as well.</li>
   * </ul>
   *
   * <p>The merged result is ordered {@code (priority DESC, id)} and capped at
   * the narrow page budget; keyed and hold candidates are then locked with
   * {@code SKIP LOCKED}, and rows another claimer holds are dropped.
   */
  private List<PendingClaim> lockClaimCandidates(Connection conn, String queue, int want)
      throws SQLException {
    int page = narrowClaimPageSize(want);
    var candidates = new LinkedHashMap<UUID, PendingClaim>();
    collectUnkeyedCandidates(conn, queue, page, candidates);
    var keyed = new LinkedHashMap<UUID, PendingClaim>();
    collectKeyedCandidates(conn, queue, want, keyed);
    keyed.keySet().removeAll(candidates.keySet());
    candidates.putAll(keyed);
    if (candidates.isEmpty()) {
      return List.of();
    }
    List<PendingClaim> ordered = candidates.values().stream()
        .sorted(Comparator.comparingInt(PendingClaim::priority)
            .reversed()
            .thenComparing(PendingClaim::id, JobId::compareCanonical))
        .limit(page)
        .toList();
    var unlocked =
        ordered.stream().map(PendingClaim::id).filter(keyed::containsKey).toList();
    if (unlocked.isEmpty()) {
      return ordered;
    }
    Map<UUID, PendingClaim> locked = lockByIds(conn, unlocked);
    var result = new ArrayList<PendingClaim>(ordered.size());
    for (var candidate : ordered) {
      if (!keyed.containsKey(candidate.id())) {
        result.add(candidate);
      } else if (locked.containsKey(candidate.id())) {
        result.add(locked.get(candidate.id()));
      }
    }
    return result;
  }

  private void collectUnkeyedCandidates(
      Connection conn, String queue, int page, Map<UUID, PendingClaim> into) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(UNKEYED_CLAIM_SQL)) {
      ps.setString(1, queue);
      // Oracle locks SKIP LOCKED rows as they are fetched: fetch exactly one
      // page so the cursor locks no more than the budget.
      ps.setFetchSize(page);
      ps.setMaxRows(page);
      readPendingClaims(ps, page, into);
    }
  }

  private void collectKeyedCandidates(
      Connection conn, String queue, int want, Map<UUID, PendingClaim> into) throws SQLException {
    List<String> keys = distinctEnqueuedKeys(conn, queue);
    if (keys.isEmpty()) {
      return;
    }
    int perKey = Math.max(1, want);
    for (var chunk : chunks(keys, MAX_BRANCHES_PER_STATEMENT)) {
      int size = branchBucket(chunk.size());
      try (PreparedStatement ps = conn.prepareStatement(keyedHeadsSql(size))) {
        int parameter = 1;
        for (int i = 0; i < size; i++) {
          ps.setString(parameter++, queue);
          if (i < chunk.size()) {
            ps.setString(parameter++, chunk.get(i));
          } else {
            ps.setNull(parameter++, Types.VARCHAR);
          }
          ps.setInt(parameter++, perKey);
        }
        ps.setFetchSize(Math.min(1000, chunk.size() * perKey));
        readPendingClaims(ps, Integer.MAX_VALUE, into);
      }
    }
    collectActiveHoldMemberCandidates(conn, queue, perKey, keys, into);
  }

  private void collectActiveHoldMemberCandidates(
      Connection conn, String queue, int perKey, List<String> keys, Map<UUID, PendingClaim> into)
      throws SQLException {
    var holds = new ArrayList<WorkflowKey>();
    int size = bucket(keys.size());
    try (PreparedStatement ps = conn.prepareStatement("SELECT concurrency_key, workflow_root_id "
        + "FROM threadmill_concurrency_workflow_holds WHERE concurrency_key IN ("
        + placeholders(size) + ")")) {
      bindStrings(ps, 1, keys, size);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          holds.add(new WorkflowKey(rs.getString(1), getUuid(rs, 2)));
        }
      }
    }
    for (var chunk : chunks(holds, MAX_BRANCHES_PER_STATEMENT)) {
      int branches = branchBucket(chunk.size());
      try (PreparedStatement ps = conn.prepareStatement(holdMembersSql(branches))) {
        int parameter = 1;
        for (int i = 0; i < branches; i++) {
          if (i < chunk.size()) {
            ps.setString(parameter++, chunk.get(i).concurrencyKey());
            setUuid(ps, parameter++, chunk.get(i).workflowRootId());
          } else {
            ps.setNull(parameter++, Types.VARCHAR);
            setUuid(ps, parameter++, null);
          }
          ps.setString(parameter++, queue);
          ps.setInt(parameter++, perKey);
        }
        readPendingClaims(ps, Integer.MAX_VALUE, into);
      }
    }
  }

  /** Branch counts are bucketed so variable key counts share a few cursors. */
  private static int branchBucket(int count) {
    int size = 4;
    while (size < count) {
      size <<= 1;
    }
    return Math.min(size, MAX_BRANCHES_PER_STATEMENT);
  }

  private Map<UUID, PendingClaim> lockByIds(Connection conn, List<UUID> ids) throws SQLException {
    var locked = new HashMap<UUID, PendingClaim>();
    for (var chunk : chunks(ids, MAX_IN_LIST)) {
      int size = bucket(chunk.size());
      try (PreparedStatement ps = conn.prepareStatement(lockByIdsSql(size))) {
        bindUuids(ps, 1, chunk, size);
        ps.setFetchSize(size);
        readPendingClaims(ps, Integer.MAX_VALUE, locked);
      }
    }
    return locked;
  }

  private static void readPendingClaims(
      PreparedStatement ps, int limit, Map<UUID, PendingClaim> into) throws SQLException {
    try (ResultSet rs = ps.executeQuery()) {
      int read = 0;
      while (read < limit && rs.next()) {
        read++;
        String mode = rs.getString(5);
        var claim = new PendingClaim(
            getUuid(rs, 1),
            rs.getLong(2),
            rs.getInt(3),
            rs.getString(4),
            mode == null ? null : ConcurrencyMode.valueOf(mode),
            getUuid(rs, 6),
            getInstant(rs, 7));
        into.putIfAbsent(claim.id(), claim);
      }
    }
  }

  /**
   * Distinct concurrency keys with ENQUEUED members in this queue, via a
   * recursive {@code MIN} loose scan over {@code threadmill_jobs_keyed_idx}:
   * one index probe per key, independent of how many rows each key holds.
   * Each store instance keeps a bounded, process-local keyset cursor per
   * active queue so a page of blocked early keys cannot permanently hide
   * later claimable keys; reaching the end wraps the next scan to the first
   * key. Cursor mutation is a disposable fairness hint, deliberately not
   * transactional: a retried transaction may skip or repeat a page without
   * affecting claim correctness.
   */
  private List<String> distinctEnqueuedKeys(Connection conn, String queue) throws SQLException {
    var observed = pendingKeyCursors.current(queue);
    var keys = distinctEnqueuedKeysAfter(conn, queue, observed == null ? null : observed.after());
    if (keys.size() > MAX_PENDING_KEYS_PER_PASS) {
      keys.removeLast();
      pendingKeyCursors.advance(queue, observed, keys.getLast());
      return keys;
    }
    pendingKeyCursors.clear(queue, observed);
    if (keys.isEmpty() && observed != null) {
      // The tail disappeared after the previous look-ahead (for example,
      // another node claimed it). Restart now instead of reporting a false
      // empty keyed lane for this poll.
      keys = distinctEnqueuedKeysAfter(conn, queue, null);
      if (keys.size() > MAX_PENDING_KEYS_PER_PASS) {
        keys.removeLast();
        pendingKeyCursors.advance(queue, null, keys.getLast());
      }
    }
    return keys;
  }

  private List<String> distinctEnqueuedKeysAfter(Connection conn, String queue, String after)
      throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(pendingKeysSql(after != null))) {
      int parameter = 1;
      ps.setString(parameter++, queue);
      if (after != null) ps.setString(parameter++, after);
      ps.setString(parameter++, queue);
      ps.setInt(parameter, MAX_PENDING_KEYS_PER_PASS);
      ps.setFetchSize(MAX_PENDING_KEYS_PER_PASS + 1);
      try (ResultSet rs = ps.executeQuery()) {
        var keys = new ArrayList<String>();
        while (rs.next()) {
          keys.add(rs.getString(1));
        }
        return keys;
      }
    }
  }

  private List<PendingClaim> claimableCandidates(
      Connection conn, List<PendingClaim> pending, int remaining) throws SQLException {
    var keys = new TreeSet<String>();
    var keyed = new ArrayList<PendingClaim>();
    for (var candidate : pending) {
      if (candidate.concurrencyKey() != null) {
        keys.add(candidate.concurrencyKey());
        keyed.add(candidate);
      }
    }
    if (keys.isEmpty()) {
      return pending.subList(0, Math.min(remaining, pending.size()));
    }
    lockConcurrencyGroups(conn, keys);
    Map<String, GroupState> groups = loadGroupStates(conn, keys);
    Set<WorkflowKey> activeHolds = loadActiveWorkflowHolds(conn, keyed, keys);
    Map<UUID, Admission> admissions = loadAdmissions(conn, keyed);
    var claimable = new ArrayList<PendingClaim>(Math.min(remaining, pending.size()));
    for (var candidate : pending) {
      if (claimable.size() >= remaining) {
        break;
      }
      if (canClaim(candidate, groups, activeHolds, admissions)) {
        claimable.add(candidate);
      }
    }
    return claimable;
  }

  /**
   * Whether an earlier pending job, and an earlier pending EXCLUSIVE job,
   * exists in each candidate's key, as {@code EXISTS} range probes bounded at
   * the candidate's own {@code current_state_at}: the probe reads the key's
   * first index entry and stops, independent of the key's backlog.
   */
  private Map<UUID, Admission> loadAdmissions(Connection conn, List<PendingClaim> keyed)
      throws SQLException {
    var admissions = new HashMap<UUID, Admission>();
    var ids = keyed.stream().map(PendingClaim::id).toList();
    for (var chunk : chunks(ids, MAX_IN_LIST)) {
      int size = bucket(chunk.size());
      try (PreparedStatement ps = conn.prepareStatement(admissionSql(size))) {
        bindUuids(ps, 1, chunk, size);
        ps.setFetchSize(size);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            admissions.put(getUuid(rs, 1), new Admission(rs.getInt(2) == 1, rs.getInt(3) == 1));
          }
        }
      }
    }
    return admissions;
  }

  private Map<UUID, String> fetchBodies(Connection conn, List<PendingClaim> claimable)
      throws SQLException {
    if (claimable.isEmpty()) {
      return Map.of();
    }
    var bodies = new HashMap<UUID, String>();
    var ids = claimable.stream().map(PendingClaim::id).toList();
    for (var chunk : chunks(ids, MAX_IN_LIST)) {
      int size = bucket(chunk.size());
      try (PreparedStatement ps = conn.prepareStatement(
          "SELECT id, body FROM threadmill_jobs WHERE id IN (" + placeholders(size) + ")")) {
        bindUuids(ps, 1, chunk, size);
        ps.setFetchSize(size);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            bodies.put(getUuid(rs, 1), rs.getString(2));
          }
        }
      }
    }
    return bodies;
  }

  private String quarantineBody(String original, long version, Instant now) {
    try {
      var rejected = serializer.deserializeJob(original);
      rejected.transitionTo(
          JobState.QUARANTINED, now, "engine.claim-poison", "Cannot prepare processing state");
      return serializer.serializeJob(withVersion(rejected, version), capabilities);
    } catch (RuntimeException unreadable) {
      // Preserve raw evidence when no valid bounded envelope can be written.
      return null;
    }
  }

  /**
   * Move an ENQUEUED job whose body cannot be prepared out of the claim path
   * with a scalar update; the counter trigger reconciles ENQUEUED to
   * QUARANTINED in the same transaction.
   */
  private void quarantineUnreadable(
      Connection conn, PendingClaim candidate, Instant now, String originalBody)
      throws SQLException {
    String rejectedBody = quarantineBody(originalBody, candidate.version() + 1, now);
    try (var clobs = new OracleClobs();
        PreparedStatement ps = conn.prepareStatement("UPDATE threadmill_jobs SET "
            + "state = 'QUARANTINED', current_state_at = ?, version = ?, body = NVL(?, body) "
            + "WHERE id = ? AND version = ? AND state = 'ENQUEUED'")) {
      setInstant(ps, 1, now);
      ps.setLong(2, candidate.version() + 1);
      clobs.bind(ps, 3, rejectedBody);
      setUuid(ps, 4, candidate.id());
      ps.setLong(5, candidate.version());
      if (ps.executeUpdate() == 0) {
        return; // raced away — nothing was quarantined
      }
    }
    // QUARANTINED is terminal: if the job's workflow root holds the key, this
    // member's share must be released, or the key stays held forever.
    if (candidate.concurrencyKey() != null) {
      releaseWorkflowHoldShare(
          conn,
          candidate.concurrencyKey(),
          candidate.concurrencyMode(),
          candidate.workflowRootId());
    }
    LOG.warn(
        "Quarantined job {} during claim: its persisted body could not be deserialized",
        candidate.id());
  }

  private static void lockConcurrencyGroups(Connection conn, Set<String> keys) throws SQLException {
    for (String key : new TreeSet<>(keys)) {
      lockConcurrencyGroup(conn, key);
    }
  }

  /**
   * Lock (creating if absent) the concurrency-group row for {@code key}. A
   * failed INSERT is a statement-level rollback in Oracle, so the loop is safe
   * inside a caller-owned transaction as well.
   */
  private static void lockConcurrencyGroup(Connection conn, String key) throws SQLException {
    try (var lock = conn.prepareStatement("SELECT concurrency_key FROM "
            + "threadmill_concurrency_groups WHERE concurrency_key = ? FOR UPDATE");
        var insert = conn.prepareStatement("INSERT INTO threadmill_concurrency_groups "
            + "(concurrency_key, exclusive_in_flight, shared_in_flight, last_modified) "
            + "VALUES (?, 0, 0, " + NOW_UTC + ")")) {
      lock.setString(1, key);
      insert.setString(1, key);
      for (int attempt = 0; attempt < 10; attempt++) {
        try (var row = lock.executeQuery()) {
          if (row.next()) return;
        }
        try {
          insert.executeUpdate();
          return; // the inserting transaction holds the new row's lock
        } catch (SQLException e) {
          if (!hasErrorCode(e, UNIQUE_VIOLATION)) throw e;
          // A concurrent transaction created it; lock it on the next attempt.
        }
      }
      throw new SQLException("Concurrency group kept disappearing while acquiring its lock");
    }
  }

  private static Map<String, GroupState> loadGroupStates(Connection conn, Set<String> keys)
      throws SQLException {
    var groups = new HashMap<String, GroupState>();
    for (var chunk : chunks(List.copyOf(keys), MAX_IN_LIST)) {
      int size = bucket(chunk.size());
      try (PreparedStatement ps = conn.prepareStatement("SELECT concurrency_key, "
          + "exclusive_in_flight, shared_in_flight FROM threadmill_concurrency_groups "
          + "WHERE concurrency_key IN (" + placeholders(size) + ")")) {
        bindStrings(ps, 1, chunk, size);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            groups.put(rs.getString(1), new GroupState(rs.getInt(2), rs.getInt(3)));
          }
        }
      }
    }
    return groups;
  }

  private static Set<WorkflowKey> loadActiveWorkflowHolds(
      Connection conn, List<PendingClaim> keyed, Set<String> keys) throws SQLException {
    var roots = keyed.stream().map(PendingClaim::workflowRootId).distinct().toList();
    var holds = new HashSet<WorkflowKey>();
    for (var keyChunk : chunks(List.copyOf(keys), 500)) {
      for (var rootChunk : chunks(roots, 500)) {
        int keySize = bucket(keyChunk.size());
        int rootSize = bucket(rootChunk.size());
        try (PreparedStatement ps = conn.prepareStatement("SELECT concurrency_key, "
            + "workflow_root_id FROM threadmill_concurrency_workflow_holds WHERE concurrency_key IN ("
            + placeholders(keySize) + ") AND workflow_root_id IN (" + placeholders(rootSize)
            + ")")) {
          int next = bindStrings(ps, 1, keyChunk, keySize);
          bindUuids(ps, next, rootChunk, rootSize);
          try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
              holds.add(new WorkflowKey(rs.getString(1), getUuid(rs, 2)));
            }
          }
        }
      }
    }
    return holds;
  }

  private static boolean canClaim(
      PendingClaim candidate,
      Map<String, GroupState> groups,
      Set<WorkflowKey> activeHolds,
      Map<UUID, Admission> admissions) {
    if (candidate.concurrencyKey() == null) {
      return true;
    }
    if (activeHolds.contains(
        new WorkflowKey(candidate.concurrencyKey(), candidate.workflowRootId()))) {
      return true;
    }
    Admission admission = admissions.get(candidate.id());
    if (admission == null) {
      return false; // left the table since it was gathered
    }
    GroupState group = groups.getOrDefault(candidate.concurrencyKey(), GroupState.IDLE);
    if (candidate.concurrencyMode() == ConcurrencyMode.EXCLUSIVE) {
      return group.isIdle() && !admission.earlierPending();
    }
    return group.exclusiveInFlight() == 0 && !admission.earlierExclusive();
  }

  private record PendingClaim(
      UUID id,
      long version,
      int priority,
      String concurrencyKey,
      ConcurrencyMode concurrencyMode,
      UUID workflowRootId,
      Instant currentStateAt) {}

  private record GroupState(int exclusiveInFlight, int sharedInFlight) {
    static final GroupState IDLE = new GroupState(0, 0);

    boolean isIdle() {
      return exclusiveInFlight == 0 && sharedInFlight == 0;
    }
  }

  private record Admission(boolean earlierPending, boolean earlierExclusive) {}

  private record WorkflowKey(String concurrencyKey, UUID workflowRootId) {}

  // ---------------------------------------------------------------- claim-path SQL
  // Package-private so the plan regression tests run exactly these statements.

  /**
   * Unkeyed lane: an index-ordered locking cursor. Oracle rejects a row limit
   * on a locking query, so the caller fetches only its page; Oracle locks
   * {@code SKIP LOCKED} rows as they are fetched.
   */
  static final String UNKEYED_CLAIM_SQL =
      "SELECT /*+ INDEX_RS_ASC(j threadmill_jobs_unkeyed_idx) */ "
          + CLAIM_COLUMNS + " FROM threadmill_jobs j WHERE unkeyed_queue = ? "
          + "ORDER BY unkeyed_rank, unkeyed_id FOR UPDATE SKIP LOCKED";

  /**
   * Distinct keys with ENQUEUED members in one queue: a recursive loose scan
   * whose every step is one {@code MIN} index probe, bounded to one page plus
   * one look-ahead key.
   */
  static String pendingKeysSql(boolean afterCursor) {
    return "WITH keys (k, n) AS ("
        + "SELECT /*+ INDEX(j threadmill_jobs_keyed_idx) */ MIN(keyed_key), 1 "
        + "FROM threadmill_jobs j WHERE keyed_queue = ?" + (afterCursor ? " AND keyed_key > ?" : "")
        + " UNION ALL "
        + "SELECT (SELECT /*+ INDEX(j threadmill_jobs_keyed_idx) */ MIN(keyed_key) "
        + "FROM threadmill_jobs j WHERE keyed_queue = ? AND keyed_key > keys.k), keys.n + 1 "
        + "FROM keys WHERE keys.k IS NOT NULL AND keys.n <= ?) "
        + "SELECT k FROM keys WHERE k IS NOT NULL ORDER BY n";
  }

  /**
   * Earliest ENQUEUED heads of {@code branches} keys: one {@code ROWNUM} top-n
   * branch per key that reads the keyed index in order and stops after n
   * entries. Deliberately not a lateral join: Oracle keeps a sort over the
   * key's whole range inside a correlated top-n lateral view, and
   * collection-driven lateral top-n views have returned wrong results.
   */
  static String keyedHeadsSql(int branches) {
    var branch = "SELECT * FROM (SELECT /*+ INDEX_RS_ASC(j threadmill_jobs_keyed_idx) */ "
        + CLAIM_COLUMNS + " FROM threadmill_jobs j WHERE keyed_queue = ? AND keyed_key = ? "
        + "ORDER BY keyed_at, keyed_id) WHERE ROWNUM <= ?";
    return String.join(" UNION ALL ", Collections.nCopies(branches, branch));
  }

  /**
   * Earliest ENQUEUED members of {@code branches} active workflow holds. One
   * root's members are bounded by its workflow's size, not by the backlog.
   */
  static String holdMembersSql(int branches) {
    var branch = "SELECT * FROM (SELECT /*+ INDEX(j threadmill_jobs_workflow_idx) */ "
        + CLAIM_COLUMNS + " FROM threadmill_jobs j WHERE outstanding_key = ? "
        + "AND outstanding_root = ? AND state = 'ENQUEUED' AND queue = ? "
        + "ORDER BY current_state_at, id) WHERE ROWNUM <= ?";
    return String.join(" UNION ALL ", Collections.nCopies(branches, branch));
  }

  /** Lock gathered keyed candidates by primary key, skipping rows another claimer holds. */
  static String lockByIdsSql(int size) {
    return "SELECT /*+ INDEX(j threadmill_jobs_pk) */ " + CLAIM_COLUMNS
        + " FROM threadmill_jobs j WHERE id IN (" + placeholders(size) + ") "
        + "AND state = 'ENQUEUED' FOR UPDATE SKIP LOCKED";
  }

  /**
   * Claim-time admission flags per candidate: whether an earlier pending job,
   * and an earlier pending EXCLUSIVE job, exists in its key. Each is an
   * {@code EXISTS} range probe bounded at the candidate's own
   * {@code current_state_at}, so it reads the key's first index entries and
   * stops, independent of the key's backlog.
   */
  static String admissionSql(int size) {
    return "SELECT /*+ INDEX(c threadmill_jobs_pk) */ c.id, "
        + "CASE WHEN EXISTS (SELECT 1 FROM threadmill_jobs p "
        + "WHERE p.pending_key = c.concurrency_key AND p.pending_at <= c.current_state_at "
        + "AND (p.pending_at < c.current_state_at OR p.pending_id < c.id)) THEN 1 ELSE 0 END, "
        + "CASE WHEN EXISTS (SELECT 1 FROM threadmill_jobs x "
        + "WHERE x.exclusive_key = c.concurrency_key AND x.exclusive_at <= c.current_state_at "
        + "AND (x.exclusive_at < c.current_state_at OR x.exclusive_id < c.id)) THEN 1 ELSE 0 END "
        + "FROM threadmill_jobs c WHERE c.id IN (" + placeholders(size) + ")";
  }

  // ---------------------------------------------------------------- queue pauses

  @Override
  public void pauseQueue(String queue, String reason) {
    Names.requireName("queue", queue);
    try {
      ownedTransaction(conn -> {
        try (var clobs = new OracleClobs()) {
          upsert(
              conn,
              "UPDATE threadmill_queue_pauses SET paused_at = ?, paused_by = ? WHERE queue = ?",
              ps -> {
                setInstant(ps, 1, Instant.now());
                clobs.bind(ps, 2, reason);
                ps.setString(3, queue);
              },
              "INSERT INTO threadmill_queue_pauses (queue, paused_at, paused_by) VALUES (?, ?, ?)",
              ps -> {
                ps.setString(1, queue);
                setInstant(ps, 2, Instant.now());
                clobs.bind(ps, 3, reason);
              });
        }
        return null;
      });
    } catch (SQLException e) {
      throw new JdbcException("pauseQueue failed", e);
    }
  }

  @Override
  public void resumeQueue(String queue) {
    Names.requireName("queue", queue);
    try {
      ownedTransaction(conn -> {
        try (PreparedStatement ps =
            conn.prepareStatement("DELETE FROM threadmill_queue_pauses WHERE queue = ?")) {
          ps.setString(1, queue);
          ps.executeUpdate();
        }
        return null;
      });
    } catch (SQLException e) {
      throw new JdbcException("resumeQueue failed", e);
    }
  }

  @Override
  public Set<String> listPausedQueues() {
    var out = new HashSet<String>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement("SELECT queue FROM threadmill_queue_pauses");
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) out.add(rs.getString(1));
    } catch (SQLException e) {
      throw new JdbcException("listPausedQueues failed", e);
    }
    return Set.copyOf(out);
  }

  private boolean isQueuePaused(String queue) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT 1 FROM threadmill_queue_pauses WHERE queue = ?")) {
      ps.setString(1, queue);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    } catch (SQLException e) {
      throw new JdbcException("isQueuePaused failed", e);
    }
  }

  // ---------------------------------------------------------------- heartbeats

  @Override
  public void touchExecutionHeartbeats(NodeId nodeId, Map<JobId, Long> activeClaims, Instant now) {
    Objects.requireNonNull(nodeId, "nodeId");
    Objects.requireNonNull(now, "now");
    var claims = ExecutionHeartbeats.snapshot(activeClaims);
    if (claims.isEmpty()) return;
    try {
      ownedTransaction(conn -> {
        try (var update = conn.prepareStatement("UPDATE threadmill_jobs SET "
            + "owner_heartbeat_at = GREATEST(NVL(owner_heartbeat_at, ?), ?) "
            + "WHERE id = ? AND version = ? AND owner_node_id = ? AND state = 'PROCESSING'")) {
          for (var claim : claims.entrySet()) {
            setInstant(update, 1, now);
            setInstant(update, 2, now);
            setUuid(update, 3, claim.getKey().asUuid());
            update.setLong(4, claim.getValue());
            setUuid(update, 5, nodeId.asUuid());
            update.addBatch();
          }
          update.executeBatch();
        }
        return null;
      });
    } catch (SQLException failure) {
      throw new JdbcException("touchExecutionHeartbeats failed", failure);
    }
  }

  @Override
  public void touchOwnerHeartbeat(NodeId nodeId, Instant now) {
    try {
      ownedTransaction(conn -> {
        try (PreparedStatement ps = conn.prepareStatement("UPDATE threadmill_jobs SET "
            + "owner_heartbeat_at = GREATEST(NVL(owner_heartbeat_at, ?), ?) "
            + "WHERE state = 'PROCESSING' AND owner_node_id = ?")) {
          setInstant(ps, 1, now);
          setInstant(ps, 2, now);
          setUuid(ps, 3, nodeId.asUuid());
          ps.executeUpdate();
        }
        return null;
      });
    } catch (SQLException e) {
      throw new JdbcException("touchOwnerHeartbeat failed", e);
    }
  }

  @Override
  public boolean saveExecutionUpdate(Job job, NodeId nodeId) {
    Objects.requireNonNull(job, "job");
    Objects.requireNonNull(nodeId, "nodeId");
    var incoming = job.snapshot();
    try {
      boolean saved = ownedTransaction(conn -> {
        Instant heartbeat = incoming.ownerHeartbeatAt();
        try (var select = conn.prepareStatement("SELECT owner_heartbeat_at, last_checkin_at "
            + "FROM threadmill_jobs WHERE id = ? AND state = 'PROCESSING' AND owner_node_id = ? "
            + "AND version = ? AND execution_revision = ? FOR UPDATE")) {
          setUuid(select, 1, incoming.id().asUuid());
          setUuid(select, 2, nodeId.asUuid());
          select.setLong(3, incoming.version());
          select.setLong(4, incoming.executionRevision());
          try (var rs = select.executeQuery()) {
            if (!rs.next()) return false;
            Instant persistedHeartbeat = getInstant(rs, 1);
            Instant persistedCheckIn = getInstant(rs, 2);
            if (persistedCheckIn != null
                && (incoming.lastCheckinAt() == null
                    || incoming.lastCheckinAt().isBefore(persistedCheckIn))) {
              return false;
            }
            if (persistedHeartbeat != null
                && (heartbeat == null || heartbeat.isBefore(persistedHeartbeat))) {
              heartbeat = persistedHeartbeat;
            }
          }
        }
        var updated = incoming.withExecutionUpdate(incoming.executionRevision() + 1, heartbeat);
        var body = serializer.serializeJob(updated, capabilities);
        try (var clobs = new OracleClobs();
            var update = conn.prepareStatement("UPDATE threadmill_jobs SET "
                + "owner_heartbeat_at = ?, last_checkin_at = ?, body = ?, execution_revision = ? "
                + "WHERE id = ?")) {
          setInstant(update, 1, heartbeat);
          setInstant(update, 2, updated.lastCheckinAt());
          clobs.bind(update, 3, body);
          update.setLong(4, updated.executionRevision());
          setUuid(update, 5, updated.id().asUuid());
          return update.executeUpdate() == 1;
        }
      });
      if (saved) job.adoptExecutionRevision(incoming.executionRevision() + 1);
      return saved;
    } catch (SQLException e) {
      throw new JdbcException("saveExecutionUpdate failed", e);
    }
  }

  @Override
  public void recordNodeHeartbeat(NodeId nodeId, Instant now) {
    try {
      ownedTransaction(conn -> {
        upsert(
            conn,
            "UPDATE threadmill_nodes SET last_heartbeat_at = ? WHERE id = ?",
            ps -> {
              setInstant(ps, 1, now);
              setUuid(ps, 2, nodeId.asUuid());
            },
            "INSERT INTO threadmill_nodes (id, last_heartbeat_at) VALUES (?, ?)",
            ps -> {
              setUuid(ps, 1, nodeId.asUuid());
              setInstant(ps, 2, now);
            });
        return null;
      });
    } catch (SQLException e) {
      throw new JdbcException("recordNodeHeartbeat failed", e);
    }
  }

  @Override
  public Optional<Instant> readNodeHeartbeat(NodeId nodeId) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT last_heartbeat_at FROM threadmill_nodes WHERE id = ?")) {
      setUuid(ps, 1, nodeId.asUuid());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(getInstant(rs, 1)) : Optional.empty();
      }
    } catch (SQLException e) {
      throw new JdbcException("readNodeHeartbeat failed", e);
    }
  }

  @Override
  public boolean acquireOrRenewMaintenanceLease(NodeId nodeId, Duration leaseDuration) {
    Objects.requireNonNull(nodeId, "nodeId");
    Mutexes.requirePositive(leaseDuration);
    try {
      // Expiry uses server time for both write and compare, so a node whose
      // clock runs ahead cannot steal an unexpired lease.
      return ownedTransaction(conn -> conditionalUpsert(
          conn,
          "UPDATE threadmill_leases SET holder = ?, expires_at = " + NOW_UTC
              + " + NUMTODSINTERVAL(?, 'SECOND') WHERE name = ? AND (holder = ? OR expires_at <= "
              + NOW_UTC + ")",
          ps -> {
            setUuid(ps, 1, nodeId.asUuid());
            setSeconds(ps, 2, leaseDuration);
            ps.setString(3, MAINTENANCE_LEASE);
            setUuid(ps, 4, nodeId.asUuid());
          },
          "INSERT INTO threadmill_leases (name, holder, expires_at) VALUES (?, ?, " + NOW_UTC
              + " + NUMTODSINTERVAL(?, 'SECOND'))",
          ps -> {
            ps.setString(1, MAINTENANCE_LEASE);
            setUuid(ps, 2, nodeId.asUuid());
            setSeconds(ps, 3, leaseDuration);
          }));
    } catch (SQLException e) {
      throw new JdbcException("acquireOrRenewMaintenanceLease failed", e);
    }
  }

  @Override
  public void releaseMaintenanceLease(NodeId nodeId) {
    Objects.requireNonNull(nodeId, "nodeId");
    try {
      ownedTransaction(conn -> {
        try (PreparedStatement ps =
            conn.prepareStatement("DELETE FROM threadmill_leases WHERE name = ? AND holder = ?")) {
          ps.setString(1, MAINTENANCE_LEASE);
          setUuid(ps, 2, nodeId.asUuid());
          ps.executeUpdate();
        }
        return null;
      });
    } catch (SQLException e) {
      throw new JdbcException("releaseMaintenanceLease failed", e);
    }
  }

  @Override
  public Optional<NodeId> readMaintenanceLeaseOwner() {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement("SELECT holder FROM threadmill_leases "
            + "WHERE name = ? AND expires_at > " + NOW_UTC)) {
      ps.setString(1, MAINTENANCE_LEASE);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(NodeId.of(getUuid(rs, 1))) : Optional.empty();
      }
    } catch (SQLException e) {
      throw new JdbcException("readMaintenanceLeaseOwner failed", e);
    }
  }

  // ---------------------------------------------------------------- housekeeping queries

  @Override
  public List<Job> findDueForPromotion(Instant now, int max) {
    int limit = Math.max(0, max);
    return queryJobs(PROMOTION_SQL, limit, ps -> {
      setInstant(ps, 1, now);
      ps.setInt(2, limit);
    });
  }

  @Override
  public List<Job> findOrphaned(Instant heartbeatExpiry, int max) {
    int limit = Math.max(0, max);
    return queryJobs(ORPHAN_SQL, limit, ps -> {
      setInstant(ps, 1, heartbeatExpiry);
      ps.setInt(2, limit);
    });
  }

  // Maintenance SQL is package-private so the plan regression tests explain exactly these.

  static final String PROMOTION_SQL =
      firstRows("SELECT /*+ INDEX(j threadmill_jobs_scheduled_idx) */ " + JOB_PROJECTION
          + " FROM threadmill_jobs j WHERE scheduled_due <= ? ORDER BY scheduled_due");

  static final String ORPHAN_SQL =
      firstRows("SELECT /*+ INDEX(j threadmill_jobs_liveness_idx) */ " + JOB_PROJECTION
          + " FROM threadmill_jobs j WHERE processing_liveness <= ? ORDER BY processing_liveness");

  static final String AWAITING_SQL =
      firstRows("SELECT /*+ INDEX(j threadmill_jobs_awaiting_idx) */ body FROM threadmill_jobs j "
          + "WHERE awaiting_parent = ? ORDER BY awaiting_at, awaiting_id");

  /** Oldest ENQUEUED transition in one queue: a single MIN/MAX index probe. */
  static final String QUEUE_AGE_SQL =
      "SELECT /*+ INDEX(j threadmill_jobs_queue_age_idx) */ MIN(enqueued_at) "
          + "FROM threadmill_jobs j WHERE enqueued_queue = ?";

  /** Earliest SCHEDULED due time: a single MIN/MAX index probe. */
  static final String OLDEST_SCHEDULED_SQL =
      "SELECT /*+ INDEX(j threadmill_jobs_scheduled_idx) */ MIN(scheduled_due) "
          + "FROM threadmill_jobs j";

  /** Earliest transition into one state: a single MIN/MAX probe of the retention index. */
  static final String OLDEST_IN_STATE_SQL =
      "SELECT /*+ INDEX(j threadmill_jobs_retention_idx) */ MIN(current_state_at) "
          + "FROM threadmill_jobs j WHERE state = ?";

  /** Oldest owner heartbeat among PROCESSING jobs: a single MIN/MAX index probe. */
  static final String OLDEST_HEARTBEAT_SQL =
      "SELECT /*+ INDEX(j threadmill_jobs_heartbeat_idx) */ MIN(processing_heartbeat) "
          + "FROM threadmill_jobs j";

  /**
   * Top-n over an ordered query, bound to a trailing {@code ?} row count. The
   * classic {@code ROWNUM} form runs as {@code COUNT STOPKEY} over the index
   * order on every supported release; with the 19c optimizer, {@code FETCH
   * FIRST} can become a {@code WINDOW SORT} that reads the whole index range.
   */
  private static String firstRows(String orderedQuery) {
    return "SELECT * FROM (" + orderedQuery + ") WHERE ROWNUM <= ?";
  }

  static final String COUNTS_SQL =
      "SELECT state, SUM(job_count) FROM threadmill_job_counts GROUP BY state";

  static final String QUEUE_DEPTHS_SQL = "SELECT queue, SUM(job_count) "
      + "FROM threadmill_queue_counts GROUP BY queue HAVING SUM(job_count) > 0";

  /** Cutoff-eligible retention candidates in {@code (current_state_at, id)} keyset order. */
  static String retentionCandidatesSql(boolean withBody, boolean afterCursor) {
    return "SELECT /*+ INDEX_RS_ASC(j threadmill_jobs_retention_idx) */ id, current_state_at"
        + (withBody ? ", body" : "")
        + " FROM threadmill_jobs j WHERE state = ? AND current_state_at <= ? "
        + (afterCursor ? "AND current_state_at >= ? AND (current_state_at > ? OR id > ?) " : "")
        + "ORDER BY current_state_at, id FOR UPDATE SKIP LOCKED";
  }

  /**
   * Reclaimable concurrency groups in binary key order, after an optional
   * keyset cursor, through the RAW {@code idle_sort} key so neither the index
   * use nor the order depends on the session's {@code NLS_SORT}.
   */
  static String idleGroupsSql(boolean afterCursor) {
    return "SELECT /*+ INDEX_RS_ASC(g threadmill_concurrency_idle_idx) */ concurrency_key "
        + "FROM threadmill_concurrency_groups g WHERE idle_sort "
        + (afterCursor ? "> NLSSORT(?, 'NLS_SORT = BINARY')" : ">= HEXTORAW('00')")
        + " AND last_modified <= " + NOW_UTC + " - INTERVAL '1' MINUTE "
        + "ORDER BY idle_sort FOR UPDATE SKIP LOCKED";
  }

  /**
   * Binary order for text keyset pages. Comparisons ({@code name > ?}) are
   * binary under the default {@code NLS_COMP}, but {@code ORDER BY} follows
   * {@code NLS_SORT}, which the driver derives from the client JVM locale; a
   * linguistic page order would skip rows between pages.
   */
  private static String binary(String column) {
    return "NLSSORT(" + column + ", 'NLS_SORT = BINARY')";
  }

  // ---------------------------------------------------------------- counts & search

  @Override
  public Map<JobState, Long> countsByState() {
    var counts = new EnumMap<JobState, Long>(JobState.class);
    for (JobState s : JobState.values()) counts.put(s, 0L);
    // Counts are sharded 16 ways per state; only the SUM is meaningful.
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(COUNTS_SQL);
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        try {
          counts.put(JobState.valueOf(rs.getString(1)), rs.getLong(2));
        } catch (IllegalArgumentException ignored) {
          // unknown state from another schema version — ignore
        }
      }
    } catch (SQLException e) {
      throw new JdbcException("countsByState failed", e);
    }
    return counts;
  }

  @Override
  public Map<String, Long> queueDepths() {
    var depths = new HashMap<String, Long>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(QUEUE_DEPTHS_SQL);
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        depths.put(rs.getString(1), rs.getLong(2));
      }
    } catch (SQLException e) {
      throw new JdbcException("queueDepths failed", e);
    }
    return depths;
  }

  @Override
  public List<String> listEnqueuedQueues() {
    return queueDepths().keySet().stream().sorted().toList();
  }

  @Override
  public List<Job> scanJobs(JobState state, JobId after, int max) {
    Objects.requireNonNull(state, "state");
    int limit = Math.clamp(max, 0, 500);
    return queryJobs(
        firstRows("SELECT /*+ INDEX(j threadmill_jobs_state_id_idx) */ " + JOB_PROJECTION
            + " FROM threadmill_jobs j WHERE state = ? " + (after == null ? "" : "AND id > ? ")
            + "ORDER BY id"),
        limit,
        ps -> {
          ps.setString(1, state.name());
          if (after != null) setUuid(ps, 2, after.asUuid());
          ps.setInt(after == null ? 2 : 3, limit);
        });
  }

  @Override
  public List<CronTask> scanCronTasks(String after, int max) {
    int limit = Math.clamp(max, 0, 500);
    var result = new ArrayList<CronTask>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(firstRows("SELECT " + CRON_TASK_COLUMNS
            + " FROM threadmill_cron_tasks " + (after == null ? "" : "WHERE name > ? ")
            + "ORDER BY " + binary("name")))) {
      if (after != null) ps.setString(1, after);
      ps.setInt(after == null ? 1 : 2, limit);
      ps.setFetchSize(Math.max(1, limit));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) result.add(readCronTask(rs));
      }
      return result;
    } catch (SQLException e) {
      throw new JdbcException("scanCronTasks failed", e);
    }
  }

  @Override
  public List<Job> searchJobs(JobSearch search) {
    Objects.requireNonNull(search, "search");
    var sql = new StringBuilder("SELECT " + JOB_PROJECTION + " FROM threadmill_jobs WHERE 1 = 1");
    var args = new ArrayList<Object>();
    if (search.state() != null) {
      sql.append(" AND state = ?");
      args.add(search.state().name());
    }
    if (search.queue() != null) {
      sql.append(" AND queue = ?");
      args.add(search.queue());
    }
    if (search.handlerType() != null) {
      sql.append(" AND handler_signature = ?");
      args.add(search.handlerType());
    }
    sql.append(" ORDER BY current_state_at DESC, id ASC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY");
    args.add(search.offset());
    args.add(search.limit());
    return queryJobs(sql.toString(), search.limit(), ps -> {
      for (int i = 0; i < args.size(); i++) {
        ps.setObject(i + 1, args.get(i));
      }
    });
  }

  @Override
  public Optional<Instant> oldestEnqueuedAt(String queue) {
    Names.requireName("queue", queue);
    return firstInstant(QUEUE_AGE_SQL, ps -> ps.setString(1, queue), "oldestEnqueuedAt");
  }

  @Override
  public Optional<Instant> oldestMaintenanceAt(JobState state) {
    Objects.requireNonNull(state, "state");
    if (state == JobState.SCHEDULED) {
      return firstInstant(OLDEST_SCHEDULED_SQL, ps -> {}, "oldestMaintenanceAt");
    }
    return firstInstant(
        OLDEST_IN_STATE_SQL, ps -> ps.setString(1, state.name()), "oldestMaintenanceAt");
  }

  @Override
  public Optional<Instant> oldestProcessingHeartbeat() {
    return firstInstant(OLDEST_HEARTBEAT_SQL, ps -> {}, "oldestProcessingHeartbeat");
  }

  @Override
  public List<NodeHeartbeat> listNodeHeartbeats() {
    var out = new ArrayList<NodeHeartbeat>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(
            "SELECT id, last_heartbeat_at FROM threadmill_nodes ORDER BY id");
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        out.add(new NodeHeartbeat(NodeId.of(getUuid(rs, 1)), getInstant(rs, 2)));
      }
      return out;
    } catch (SQLException e) {
      throw new JdbcException("listNodeHeartbeats failed", e);
    }
  }

  @Override
  public long deleteNodeHeartbeatsOlderThan(Instant cutoff) {
    Objects.requireNonNull(cutoff, "cutoff");
    try {
      return ownedTransaction(conn -> {
        try (PreparedStatement ps =
            conn.prepareStatement("DELETE FROM threadmill_nodes WHERE last_heartbeat_at <= ?")) {
          setInstant(ps, 1, cutoff);
          return (long) ps.executeUpdate();
        }
      });
    } catch (SQLException e) {
      throw new JdbcException("deleteNodeHeartbeatsOlderThan failed", e);
    }
  }

  private record KeyPage(List<String> keys, long removed) {}

  @Override
  public synchronized long deleteIdleConcurrencyGroups(int max) {
    int limit = Math.clamp(max, 0, 100);
    if (limit == 0) return 0;
    String after = idleGroupAfter;
    try {
      var page = writeTransaction(conn -> {
        var keys = new ArrayList<String>();
        try (var query = conn.prepareStatement(idleGroupsSql(after != null))) {
          if (after != null) query.setString(1, after);
          query.setFetchSize(limit);
          query.setMaxRows(limit);
          try (var rows = query.executeQuery()) {
            while (keys.size() < limit && rows.next()) keys.add(rows.getString(1));
          }
        }
        long removed = 0;
        try (var delete = conn.prepareStatement("DELETE FROM threadmill_concurrency_groups g "
            + "WHERE concurrency_key = ? AND exclusive_in_flight = 0 AND shared_in_flight = 0 "
            + "AND NOT EXISTS (SELECT 1 FROM threadmill_concurrency_workflow_holds h "
            + "WHERE h.concurrency_key = g.concurrency_key) "
            + "AND NOT EXISTS (SELECT 1 FROM threadmill_jobs j WHERE "
            + "j.outstanding_key = g.concurrency_key)")) {
          for (var key : keys) {
            delete.setString(1, key);
            removed += delete.executeUpdate();
          }
        }
        return new KeyPage(keys, removed);
      });
      idleGroupAfter = page.keys().size() < limit ? null : page.keys().getLast();
      return page.removed();
    } catch (SQLException failure) {
      throw new JdbcException("deleteIdleConcurrencyGroups failed", failure);
    }
  }

  @Override
  public synchronized long deleteIdleQueueMetadata(int max) {
    int limit = Math.clamp(max, 0, 100);
    if (limit == 0) return 0;
    String after = idleQueueAfter;
    try {
      var page = writeTransaction(conn -> {
        var queues = new ArrayList<String>();
        var idleQueues = new ArrayList<String>();
        var candidates =
            firstRows("SELECT queue, SUM(job_count) AS total FROM threadmill_queue_counts "
                + (after == null ? "" : "WHERE queue > ? ") + "GROUP BY queue ORDER BY "
                + binary("queue"));
        try (var query = conn.prepareStatement("SELECT c.queue, CASE WHEN c.total = 0 "
            + "AND NOT EXISTS (SELECT 1 FROM threadmill_jobs j WHERE j.enqueued_queue = c.queue) "
            + "THEN 1 ELSE 0 END FROM (" + candidates + ") c ORDER BY " + binary("c.queue"))) {
          if (after != null) query.setString(1, after);
          query.setInt(after == null ? 1 : 2, limit);
          try (var rows = query.executeQuery()) {
            while (rows.next()) {
              queues.add(rows.getString(1));
              if (rows.getInt(2) == 1) idleQueues.add(rows.getString(1));
            }
          }
        }
        long removed = 0;
        for (var queue : idleQueues) {
          removed += deleteBalancedQueueShards(conn, queue);
        }
        return new KeyPage(queues, removed);
      });
      idleQueueAfter = page.keys().size() < limit ? null : page.keys().getLast();
      return page.removed();
    } catch (SQLException failure) {
      throw new JdbcException("deleteIdleQueueMetadata failed", failure);
    }
  }

  /**
   * Delete only the locked, zero-sum subset of a queue's counter shards. This
   * keeps totals exact even with negative shards or concurrent trigger writes
   * to shards this transaction could not lock. The idle check is repeated
   * under the locks because a producer may have arrived since the prefilter.
   */
  private static long deleteBalancedQueueShards(Connection conn, String queue) throws SQLException {
    var shards = new ArrayList<Integer>();
    long total = 0;
    try (var lock = conn.prepareStatement("SELECT shard_no, job_count FROM threadmill_queue_counts "
        + "WHERE queue = ? FOR UPDATE SKIP LOCKED")) {
      lock.setString(1, queue);
      lock.setFetchSize(16);
      try (var rows = lock.executeQuery()) {
        while (rows.next()) {
          shards.add(rows.getInt(1));
          total += rows.getLong(2);
        }
      }
    }
    if (shards.isEmpty() || total != 0) {
      return 0;
    }
    int size = bucket(shards.size());
    try (var delete = conn.prepareStatement("DELETE FROM threadmill_queue_counts WHERE queue = ? "
        + "AND shard_no IN (" + placeholders(size) + ") AND NOT EXISTS (SELECT 1 FROM "
        + "threadmill_jobs j WHERE j.enqueued_queue = ?)")) {
      delete.setString(1, queue);
      int parameter = 2;
      for (int shard : shards) delete.setInt(parameter++, shard);
      while (parameter < 2 + size) delete.setNull(parameter++, Types.INTEGER);
      delete.setString(parameter, queue);
      return delete.executeUpdate();
    }
  }

  @Override
  public long deleteExpiredDedupKeys(Instant now, int max) {
    Objects.requireNonNull(now, "now");
    if (max <= 0) return 0L;
    try {
      return ownedTransaction(conn -> {
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM threadmill_dedup_keys "
            + "WHERE ROWID IN (SELECT d.ROWID FROM threadmill_dedup_keys d "
            + "WHERE d.expires_at <= ? AND NOT EXISTS (SELECT 1 FROM threadmill_jobs j "
            + "WHERE j.id = d.job_id AND j.state NOT IN " + TERMINAL_STATES + ") "
            + "AND ROWNUM <= ?)")) {
          setInstant(ps, 1, now);
          ps.setInt(2, max);
          return (long) ps.executeUpdate();
        }
      });
    } catch (SQLException e) {
      throw new JdbcException("deleteExpiredDedupKeys failed", e);
    }
  }

  @Override
  public List<Job> findByHandlerSignature(String handlerType, int max) {
    Objects.requireNonNull(handlerType, "handlerType");
    int limit = Math.max(0, max);
    return queryJobs(
        "SELECT " + JOB_PROJECTION + " FROM threadmill_jobs WHERE handler_signature = ? "
            + "AND ROWNUM <= ?",
        limit,
        ps -> {
          ps.setString(1, handlerType);
          ps.setInt(2, limit);
        });
  }

  // ---------------------------------------------------------------- retention

  @Override
  public RetentionPage deleteFinishedPage(
      Instant cutoff, JobState state, int max, RetentionCursor after) {
    Objects.requireNonNull(cutoff, "cutoff");
    if (state != JobState.SUCCEEDED
        && state != JobState.FAILED
        && state != JobState.DELETED
        && state != JobState.QUARANTINED) {
      throw new IllegalArgumentException("Retention requires a finished state");
    }
    int limit = Math.clamp(max, 0, 100);
    if (limit == 0) return new RetentionPage(0, null);
    var position = after == null ? null : RetentionPosition.from(after);
    try {
      return ownedTransaction(conn -> {
        long deleted = 0;
        int inspected = 0;
        RetentionPosition last = null;
        try (var candidates = conn.prepareStatement(
                retentionCandidatesSql(state == JobState.FAILED, position != null));
            var remove = conn.prepareStatement("DELETE FROM threadmill_jobs j WHERE id = ? "
                + "AND NOT EXISTS (SELECT 1 FROM threadmill_dedup_keys d WHERE d.job_id = j.id "
                + "AND d.expires_at > " + NOW_UTC + ") "
                + "AND NOT EXISTS (SELECT 1 FROM threadmill_jobs child WHERE "
                + "child.awaiting_parent = j.id)")) {
          candidates.setString(1, state.name());
          setInstant(candidates, 2, cutoff);
          if (position != null) {
            setInstant(candidates, 3, position.at());
            setInstant(candidates, 4, position.at());
            setUuid(candidates, 5, position.id().asUuid());
          }
          candidates.setFetchSize(limit);
          candidates.setMaxRows(limit);
          try (var rows = candidates.executeQuery()) {
            while (inspected < limit && rows.next()) {
              inspected++;
              last = new RetentionPosition(
                  getInstant(rows, "current_state_at"), JobId.of(getUuid(rows, "id")));
              if (state == JobState.FAILED) {
                try {
                  if (serializer
                      .deserializeJob(rows.getString("body"))
                      .failureDecision()
                      .map(decision -> decision.willRetry())
                      .orElse(true)) continue;
                } catch (SerializationException unreadable) {
                  continue; // Preserve unknown failure outcomes, but advance the scan.
                }
              }
              setUuid(remove, 1, last.id().asUuid());
              deleted += remove.executeUpdate();
            }
          }
        }
        return new RetentionPage(deleted, inspected == limit ? last.cursor() : null);
      });
    } catch (SQLException e) {
      throw new JdbcException("deleteFinishedPage failed", e);
    }
  }

  // ---------------------------------------------------------------- relationships & mutexes

  @Override
  public List<Job> findAwaitingByParent(JobId parentId, int max) {
    Objects.requireNonNull(parentId, "parentId");
    if (max <= 0) return List.of();
    var out = new ArrayList<Job>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(AWAITING_SQL)) {
      setUuid(ps, 1, parentId.asUuid());
      ps.setInt(2, max);
      ps.setFetchSize(Math.min(max, 500));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          out.add(serializer.deserializeJob(rs.getString(1)));
        }
      }
      return out;
    } catch (SQLException e) {
      throw new JdbcException("findAwaitingByParent failed", e);
    }
  }

  @Override
  public boolean tryAcquireMutex(String name, String holder, Duration leaseDuration) {
    Names.requireName("mutex", name);
    Objects.requireNonNull(holder, "holder");
    Mutexes.requirePositive(leaseDuration);
    try {
      // Server-side time for write and compare, like the maintenance lease.
      // DECODE treats two NULLs as equal, so an empty holder (stored as NULL
      // by Oracle) still matches itself.
      return ownedTransaction(conn -> conditionalUpsert(
          conn,
          "UPDATE threadmill_mutexes SET holder = ?, expires_at = " + NOW_UTC
              + " + NUMTODSINTERVAL(?, 'SECOND') WHERE name = ? AND (expires_at <= " + NOW_UTC
              + " OR DECODE(holder, ?, 1, 0) = 1)",
          ps -> {
            ps.setString(1, holder);
            setSeconds(ps, 2, leaseDuration);
            ps.setString(3, name);
            ps.setString(4, holder);
          },
          "INSERT INTO threadmill_mutexes (name, holder, expires_at) VALUES (?, ?, " + NOW_UTC
              + " + NUMTODSINTERVAL(?, 'SECOND'))",
          ps -> {
            ps.setString(1, name);
            ps.setString(2, holder);
            setSeconds(ps, 3, leaseDuration);
          }));
    } catch (SQLException e) {
      throw new JdbcException("tryAcquireMutex failed", e);
    }
  }

  @Override
  public void releaseMutex(String name, String holder) {
    Names.requireName("mutex", name);
    try {
      ownedTransaction(conn -> {
        try (PreparedStatement ps = conn.prepareStatement(
            "DELETE FROM threadmill_mutexes WHERE name = ? AND DECODE(holder, ?, 1, 0) = 1")) {
          ps.setString(1, name);
          ps.setString(2, holder);
          ps.executeUpdate();
        }
        return null;
      });
    } catch (SQLException e) {
      throw new JdbcException("releaseMutex failed", e);
    }
  }

  @Override
  public boolean replaceJob(JobId id, long expectedVersion, JobReplacement replacement) {
    Objects.requireNonNull(id, "id");
    Objects.requireNonNull(replacement, "replacement");
    try {
      return ownedTransaction(conn -> {
        String body;
        long version;
        String state;
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT body, version, state FROM threadmill_jobs WHERE id = ? FOR UPDATE")) {
          setUuid(ps, 1, id.asUuid());
          try (ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) return false;
            body = rs.getString(1);
            version = rs.getLong(2);
            state = rs.getString(3);
          }
        }
        if (version != expectedVersion) {
          throw new StaleJobException(id, expectedVersion);
        }
        if (!isReplaceableState(state)) {
          return false;
        }
        Job replaced = JobReplacements.apply(serializer.deserializeJob(body), replacement);
        long nextVersion = version + 1;
        JobSnapshot snapshot = withVersion(replaced, nextVersion);
        String newBody = serializer.serializeJob(snapshot, capabilities);
        try (var clobs = new OracleClobs();
            PreparedStatement ps = conn.prepareStatement("UPDATE threadmill_jobs SET "
                + "queue = ?, priority = ?, handler_signature = ?, scheduled_at = ?, "
                + "current_state_at = ?, version = ?, body = ?, concurrency_key = ?, "
                + "concurrency_mode = ?, workflow_root_id = ?, parent_job_id = ? "
                + "WHERE id = ? AND version = ?")) {
          ps.setString(1, snapshot.queue());
          ps.setInt(2, snapshot.priority());
          ps.setString(3, snapshot.spec().handlerType());
          setInstant(ps, 4, snapshot.scheduledFor());
          setInstant(ps, 5, lastTransitionTime(snapshot, snapshot.currentState()));
          ps.setLong(6, nextVersion);
          clobs.bind(ps, 7, newBody);
          setNullableConcurrency(ps, 8, snapshot.concurrencyKey(), snapshot.concurrencyMode());
          setUuid(ps, 10, snapshot.workflowRootId().asUuid());
          setUuid(ps, 11, parentJobId(snapshot));
          setUuid(ps, 12, id.asUuid());
          ps.setLong(13, expectedVersion);
          return ps.executeUpdate() > 0;
        }
      });
    } catch (SQLException e) {
      throw new JdbcException("replaceJob failed", e);
    }
  }

  private static boolean isReplaceableState(String state) {
    return "ENQUEUED".equals(state) || "SCHEDULED".equals(state) || "AWAITING".equals(state);
  }

  // ---------------------------------------------------------------- cron tasks

  @Override
  public void upsertCronTask(CronTask task) {
    Objects.requireNonNull(task, "task");
    Names.requireName("cronTask", task.name());
    Names.requireName("queue", task.queue());
    String kind;
    String value;
    if (task.trigger() instanceof CronTask.Trigger.CronExpr cron) {
      kind = "CRON";
      value = cron.expression().expression();
    } else if (task.trigger() instanceof CronTask.Trigger.Interval interval) {
      kind = "INTERVAL";
      value = interval.interval().toString();
    } else {
      throw new IllegalStateException("Unknown trigger kind: " + task.trigger());
    }
    try {
      ownedTransaction(conn -> {
        try (var clobs = new OracleClobs()) {
          upsert(
              conn,
              "UPDATE threadmill_cron_tasks SET trigger_kind = ?, trigger_value = ?, "
                  + "handler_signature = ?, payload_type_tag = ?, payload_serialized = ?, queue = ?, "
                  + "priority = ?, timeout_seconds = ?, max_attempts = ?, is_exclusive = ?, "
                  + "missed_run_policy = ?, time_zone = ?, enabled = ? WHERE name = ?",
              ps -> {
                bindCronTaskValues(clobs, ps, 1, task, kind, value);
                ps.setString(14, task.name());
              },
              "INSERT INTO threadmill_cron_tasks (trigger_kind, trigger_value, handler_signature, "
                  + "payload_type_tag, payload_serialized, queue, priority, timeout_seconds, "
                  + "max_attempts, is_exclusive, missed_run_policy, time_zone, enabled, name) "
                  + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
              ps -> {
                bindCronTaskValues(clobs, ps, 1, task, kind, value);
                ps.setString(14, task.name());
              });
        }
        return null;
      });
    } catch (SQLException e) {
      throw new JdbcException("upsertCronTask failed", e);
    }
  }

  private static void bindCronTaskValues(
      OracleClobs clobs, PreparedStatement ps, int start, CronTask task, String kind, String value)
      throws SQLException {
    ps.setString(start, kind);
    ps.setString(start + 1, value);
    ps.setString(start + 2, task.handlerType());
    ps.setString(start + 3, task.payloadArgument().typeTag());
    clobs.bind(ps, start + 4, task.payloadArgument().serialized());
    ps.setString(start + 5, task.queue());
    ps.setInt(start + 6, task.priority());
    if (task.timeout() == null) {
      ps.setNull(start + 7, Types.BIGINT);
    } else {
      ps.setLong(start + 7, task.timeout().toSeconds());
    }
    if (task.maxAttempts() == null) {
      ps.setNull(start + 8, Types.INTEGER);
    } else {
      ps.setInt(start + 8, task.maxAttempts());
    }
    ps.setInt(start + 9, task.exclusive() ? 1 : 0);
    ps.setString(start + 10, task.missedRunPolicy().name());
    ps.setString(start + 11, task.zone().getId());
    ps.setInt(start + 12, task.enabled() ? 1 : 0);
  }

  @Override
  public Optional<CronTask> findCronTask(String name) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(
            "SELECT " + CRON_TASK_COLUMNS + " FROM threadmill_cron_tasks WHERE name = ?")) {
      ps.setString(1, name);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(readCronTask(rs)) : Optional.empty();
      }
    } catch (SQLException e) {
      throw new JdbcException("findCronTask failed", e);
    }
  }

  @Override
  public List<CronTask> listCronTasks() {
    var out = new ArrayList<CronTask>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement("SELECT " + CRON_TASK_COLUMNS
            + " FROM threadmill_cron_tasks ORDER BY " + binary("name"))) {
      ps.setFetchSize(500);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) out.add(readCronTask(rs));
      }
    } catch (SQLException e) {
      throw new JdbcException("listCronTasks failed", e);
    }
    return out;
  }

  @Override
  public void deleteCronTask(String name) {
    try {
      ownedTransaction(conn -> {
        try (PreparedStatement ps =
            conn.prepareStatement("DELETE FROM threadmill_cron_tasks WHERE name = ?")) {
          ps.setString(1, name);
          ps.executeUpdate();
        }
        return null;
      });
    } catch (SQLException e) {
      throw new JdbcException("deleteCronTask failed", e);
    }
  }

  @Override
  public void recordCronTaskOwnership(String namespace, String taskName) {
    Names.requireName("cronTaskNamespace", namespace);
    Names.requireName("cronTask", taskName);
    try {
      ownedTransaction(conn -> {
        try (PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO threadmill_cron_task_ownership (namespace, task_name) VALUES (?, ?)")) {
          ps.setString(1, namespace);
          ps.setString(2, taskName);
          ps.executeUpdate();
        } catch (SQLException e) {
          if (!hasErrorCode(e, UNIQUE_VIOLATION)) throw e; // already recorded
        }
        return null;
      });
    } catch (SQLException e) {
      throw new JdbcException("recordCronTaskOwnership failed", e);
    }
  }

  @Override
  public Set<String> listCronTaskNamesOwnedBy(String namespace) {
    Names.requireName("cronTaskNamespace", namespace);
    var out = new HashSet<String>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(
            "SELECT task_name FROM threadmill_cron_task_ownership WHERE namespace = ?")) {
      ps.setString(1, namespace);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) out.add(rs.getString(1));
      }
      return Set.copyOf(out);
    } catch (SQLException e) {
      throw new JdbcException("listCronTaskNamesOwnedBy failed", e);
    }
  }

  @Override
  public void upsertCronTaskState(CronTaskScheduleState state) {
    Objects.requireNonNull(state, "state");
    try {
      ownedTransaction(conn -> {
        // The nudge columns are deliberately absent: only requestCronNudge and
        // clearCronNudge write them, so this blanket upsert cannot clobber a
        // concurrently accepted nudge.
        upsert(
            conn,
            "UPDATE threadmill_cron_task_state SET last_run_at = ?, last_run_job_id = ?, "
                + "next_run_at = ?, in_flight_job_id = ?, timing_fingerprint = ? WHERE task_name = ?",
            ps -> {
              bindCronStateValues(ps, state);
              ps.setString(6, state.taskName());
            },
            "INSERT INTO threadmill_cron_task_state (last_run_at, last_run_job_id, next_run_at, "
                + "in_flight_job_id, timing_fingerprint, task_name) VALUES (?, ?, ?, ?, ?, ?)",
            ps -> {
              bindCronStateValues(ps, state);
              ps.setString(6, state.taskName());
            });
        return null;
      });
    } catch (SQLException e) {
      throw new JdbcException("upsertCronTaskState failed", e);
    }
  }

  private static void bindCronStateValues(PreparedStatement ps, CronTaskScheduleState state)
      throws SQLException {
    setInstant(ps, 1, state.lastRunAt());
    setUuid(ps, 2, state.lastRunJobId());
    setInstant(ps, 3, state.nextRunAt());
    setUuid(ps, 4, state.inFlightJobId());
    ps.setString(5, state.timingFingerprint());
  }

  @Override
  public Optional<CronTaskScheduleState> findCronTaskState(String name) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement("SELECT task_name, last_run_at, "
            + "last_run_job_id, next_run_at, in_flight_job_id, timing_fingerprint, "
            + "nudge_requested_at, nudge_revision FROM threadmill_cron_task_state "
            + "WHERE task_name = ?")) {
      ps.setString(1, name);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) return Optional.empty();
        long revision = rs.getLong(8);
        boolean revisionNull = rs.wasNull();
        return Optional.of(new CronTaskScheduleState(
            rs.getString(1),
            getInstant(rs, 2),
            getUuid(rs, 3),
            getInstant(rs, 4),
            getUuid(rs, 5),
            rs.getString(6),
            getInstant(rs, 7),
            revisionNull ? null : revision));
      }
    } catch (SQLException e) {
      throw new JdbcException("findCronTaskState failed", e);
    }
  }

  @Override
  public NudgeOutcome requestCronNudge(String taskName, Instant requestedAt) {
    Names.requireName("cronTask", taskName);
    Objects.requireNonNull(requestedAt, "requestedAt");
    try {
      return ownedTransaction(conn -> {
        // Lock the task row so acceptance is atomic with the existence and
        // enabled checks: a concurrent delete or disable waits for this
        // transaction, and a nudge racing removal cannot resurrect state.
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT enabled FROM threadmill_cron_tasks WHERE name = ? FOR UPDATE")) {
          ps.setString(1, taskName);
          try (ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) return NudgeOutcome.UNKNOWN_TASK;
            if (rs.getInt(1) != 1) return NudgeOutcome.DISABLED;
          }
        }
        // The revision advances on every acceptance and never resets while
        // the schedule-state row exists, giving compare-and-clear a
        // collision-free identity within one task lifecycle.
        try {
          upsert(
              conn,
              "UPDATE threadmill_cron_task_state SET nudge_requested_at = ?, "
                  + "nudge_revision = NVL(nudge_revision, 0) + 1 WHERE task_name = ?",
              ps -> {
                setInstant(ps, 1, requestedAt);
                ps.setString(2, taskName);
              },
              "INSERT INTO threadmill_cron_task_state (task_name, nudge_requested_at, "
                  + "nudge_revision) VALUES (?, ?, 1)",
              ps -> {
                ps.setString(1, taskName);
                setInstant(ps, 2, requestedAt);
              });
        } catch (SQLException e) {
          if (hasErrorCode(e, PARENT_KEY_NOT_FOUND)) return NudgeOutcome.UNKNOWN_TASK;
          throw e;
        }
        return NudgeOutcome.ACCEPTED;
      });
    } catch (SQLException e) {
      throw new JdbcException("requestCronNudge failed", e);
    }
  }

  @Override
  public void clearCronNudge(String taskName, long observedRevision) {
    Names.requireName("cronTask", taskName);
    try {
      ownedTransaction(conn -> {
        // Clears the pending flag only; the revision is never reset, so
        // cleared identities cannot be reused by a later acceptance.
        try (PreparedStatement ps = conn.prepareStatement("UPDATE threadmill_cron_task_state "
            + "SET nudge_requested_at = NULL WHERE task_name = ? AND nudge_revision = ? "
            + "AND nudge_requested_at IS NOT NULL")) {
          ps.setString(1, taskName);
          ps.setLong(2, observedRevision);
          ps.executeUpdate();
        }
        return null;
      });
    } catch (SQLException e) {
      throw new JdbcException("clearCronNudge failed", e);
    }
  }

  private CronTask readCronTask(ResultSet rs) throws SQLException {
    String kind = rs.getString(2);
    String value = rs.getString(3);
    CronTask.Trigger trigger =
        switch (kind) {
          case "CRON" -> new CronTask.Trigger.CronExpr(CronExpression.parse(value));
          case "INTERVAL" -> new CronTask.Trigger.Interval(Duration.parse(value));
          default -> throw new SQLException("Unknown trigger_kind: " + kind);
        };
    long timeoutSeconds = rs.getLong(9);
    Duration timeout = rs.wasNull() ? null : Duration.ofSeconds(timeoutSeconds);
    int maxAttemptsValue = rs.getInt(10);
    Integer maxAttempts = rs.wasNull() ? null : maxAttemptsValue;
    String payload = rs.getString(6);
    return new CronTask(
        rs.getString(1),
        trigger,
        rs.getString(4),
        // Oracle stores an empty string as NULL.
        new JobArgument(rs.getString(5), payload == null ? "" : payload),
        rs.getString(7),
        rs.getInt(8),
        timeout,
        maxAttempts,
        rs.getInt(11) == 1,
        CronTask.MissedRunPolicy.valueOf(rs.getString(12)),
        ZoneId.of(rs.getString(13)),
        rs.getInt(14) == 1);
  }

  // ---------------------------------------------------------------- helpers

  @FunctionalInterface
  private interface StatementSetup {
    void apply(PreparedStatement ps) throws SQLException;
  }

  /**
   * Update-then-insert upsert. Two writers that both miss the UPDATE race on
   * the INSERT; the loser sees ORA-00001 after the winner commits and retries
   * its UPDATE. A failed statement does not abort an Oracle transaction.
   */
  private static void upsert(
      Connection conn,
      String updateSql,
      StatementSetup updateSetup,
      String insertSql,
      StatementSetup insertSetup)
      throws SQLException {
    for (int attempt = 0; ; attempt++) {
      try (PreparedStatement update = conn.prepareStatement(updateSql)) {
        updateSetup.apply(update);
        if (update.executeUpdate() > 0) return;
      }
      try (PreparedStatement insert = conn.prepareStatement(insertSql)) {
        insertSetup.apply(insert);
        insert.executeUpdate();
        return;
      } catch (SQLException e) {
        if (!hasErrorCode(e, UNIQUE_VIOLATION) || attempt >= 2) throw e;
      }
    }
  }

  /**
   * Conditional upsert for leases: the UPDATE applies only while its
   * condition holds. When it matches nothing, the INSERT either creates the
   * row (acquired) or hits ORA-00001 because another holder owns it.
   */
  private static boolean conditionalUpsert(
      Connection conn,
      String updateSql,
      StatementSetup updateSetup,
      String insertSql,
      StatementSetup insertSetup)
      throws SQLException {
    try (PreparedStatement update = conn.prepareStatement(updateSql)) {
      updateSetup.apply(update);
      if (update.executeUpdate() > 0) return true;
    }
    try (PreparedStatement insert = conn.prepareStatement(insertSql)) {
      insertSetup.apply(insert);
      insert.executeUpdate();
      return true;
    } catch (SQLException e) {
      if (hasErrorCode(e, UNIQUE_VIOLATION)) return false;
      throw e;
    }
  }

  private Job readJobWithHeartbeat(ResultSet rs) throws SQLException {
    var job = serializer.deserializeJob(rs.getString(1));
    Instant heartbeat = getInstant(rs, 2);
    if (heartbeat != null && job.ownerNodeId().isPresent()) job.updateHeartbeat(heartbeat);
    return job;
  }

  private List<Job> queryJobs(String sql, int expectedRows, StatementSetup setup) {
    var out = new ArrayList<Job>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      setup.apply(ps);
      ps.setFetchSize(Math.clamp(expectedRows, 1, 500));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          out.add(readJobWithHeartbeat(rs));
        }
      }
    } catch (SQLException e) {
      throw new JdbcException("query failed: " + sql, e);
    }
    return out;
  }

  private Optional<Instant> firstInstant(String sql, StatementSetup setup, String operation) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      setup.apply(ps);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? Optional.ofNullable(getInstant(rs, 1)) : Optional.empty();
      }
    } catch (SQLException e) {
      throw new JdbcException(operation + " failed", e);
    }
  }

  private Optional<JobId> findActiveDedup(
      Connection conn, String queue, String dedupKey, Instant now) throws SQLException {
    UUID jobId;
    Instant expiresAt;
    try (PreparedStatement ps = conn.prepareStatement("SELECT job_id, expires_at "
        + "FROM threadmill_dedup_keys WHERE queue = ? AND dedup_key = ? FOR UPDATE")) {
      ps.setString(1, queue);
      ps.setString(2, dedupKey);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) return Optional.empty();
        jobId = getUuid(rs, 1);
        expiresAt = getInstant(rs, 2);
      }
    }
    String state = null;
    try (PreparedStatement ps =
        conn.prepareStatement("SELECT state FROM threadmill_jobs WHERE id = ?")) {
      setUuid(ps, 1, jobId);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) state = rs.getString(1);
      }
    }
    // Expired keys still coalesce while the referenced job is active.
    if (state != null && (expiresAt.isAfter(now) || !isTerminal(JobState.valueOf(state)))) {
      return Optional.of(JobId.of(jobId));
    }
    try (PreparedStatement ps = conn.prepareStatement(
        "DELETE FROM threadmill_dedup_keys WHERE queue = ? AND dedup_key = ?")) {
      ps.setString(1, queue);
      ps.setString(2, dedupKey);
      ps.executeUpdate();
    }
    return Optional.empty();
  }

  private JobSnapshot snapshotForInsert(Connection conn, Job job, long version)
      throws SQLException {
    if (job.version() > version) {
      throw new IllegalStateException(
          "Insert requires a new job; persisted version cannot be reset to " + version);
    }
    JobSnapshot s = withVersion(job, version);
    if (s.relationship() == null) {
      return s;
    }
    try (PreparedStatement ps = conn.prepareStatement("SELECT workflow_root_id, "
        + "concurrency_key, concurrency_mode FROM threadmill_jobs WHERE id = ?")) {
      setUuid(ps, 1, s.relationship().parentId().asUuid());
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return s;
        }
        String modeValue = rs.getString(3);
        return new JobSnapshot(
            s.id(),
            s.spec(),
            s.queue(),
            s.priority(),
            s.createdAt(),
            s.cronTaskName(),
            s.relationship(),
            JobId.of(getUuid(rs, 1)),
            rs.getString(2),
            modeValue == null ? null : ConcurrencyMode.valueOf(modeValue),
            s.stateHistory(),
            new HashMap<>(s.metadata()),
            s.log(),
            s.progress(),
            version,
            s.ownerNodeId(),
            s.ownerHeartbeatAt(),
            s.lastCheckinAt(),
            s.scheduledFor(),
            s.result(),
            s.attempts(),
            s.failureDecision(),
            s.executionRevision());
      }
    }
  }

  /** First claim page: locks at most twice the budget in rows. */
  static int narrowClaimPageSize(int cap) {
    return Math.max(cap, cap * 2);
  }

  private static boolean hasActiveWorkflowHoldForRoot(Connection conn, JobSnapshot candidate)
      throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(
        "SELECT 1 FROM "
            + "threadmill_concurrency_workflow_holds WHERE concurrency_key = ? AND workflow_root_id = ?")) {
      ps.setString(1, candidate.concurrencyKey());
      setUuid(ps, 2, candidate.workflowRootId().asUuid());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  private static void noteInsertedWorkflowDescendant(Connection conn, JobSnapshot snapshot)
      throws SQLException {
    if (snapshot.concurrencyKey() == null) {
      return;
    }
    lockConcurrencyGroup(conn, snapshot.concurrencyKey());
    incrementWorkflowHoldOutstanding(conn, snapshot);
  }

  /** Caller must already hold the concurrency-group row lock for the snapshot's key. */
  private static void incrementWorkflowHoldOutstanding(Connection conn, JobSnapshot snapshot)
      throws SQLException {
    if (snapshot.concurrencyKey() == null) {
      return;
    }
    try (PreparedStatement ps = conn.prepareStatement(
        "UPDATE threadmill_concurrency_workflow_holds "
            + "SET outstanding = outstanding + 1 WHERE concurrency_key = ? AND workflow_root_id = ?")) {
      ps.setString(1, snapshot.concurrencyKey());
      setUuid(ps, 2, snapshot.workflowRootId().asUuid());
      ps.executeUpdate();
    }
  }

  /** Caller must already hold the concurrency-group row lock for the snapshot's key. */
  private static void acquireWorkflowHold(Connection conn, JobSnapshot snapshot)
      throws SQLException {
    if (snapshot.concurrencyKey() == null || hasActiveWorkflowHoldForRoot(conn, snapshot)) {
      return;
    }
    int outstanding = Math.max(1, countOutstandingWorkflowJobs(conn, snapshot));
    try (PreparedStatement ps = conn.prepareStatement("INSERT INTO "
        + "threadmill_concurrency_workflow_holds (concurrency_key, workflow_root_id, outstanding) "
        + "VALUES (?, ?, ?)")) {
      ps.setString(1, snapshot.concurrencyKey());
      setUuid(ps, 2, snapshot.workflowRootId().asUuid());
      ps.setInt(3, outstanding);
      ps.executeUpdate();
    }
    String column = snapshot.concurrencyMode() == ConcurrencyMode.EXCLUSIVE
        ? "exclusive_in_flight"
        : "shared_in_flight";
    try (PreparedStatement ps = conn.prepareStatement("UPDATE threadmill_concurrency_groups SET "
        + column + " = " + column + " + 1, last_modified = " + NOW_UTC
        + " WHERE concurrency_key = ?")) {
      ps.setString(1, snapshot.concurrencyKey());
      ps.executeUpdate();
    }
  }

  private static int countOutstandingWorkflowJobs(Connection conn, JobSnapshot snapshot)
      throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("SELECT /*+ INDEX(j "
        + "threadmill_jobs_workflow_idx) */ COUNT(*) FROM threadmill_jobs j WHERE "
        + "outstanding_key = ? AND outstanding_root = ?")) {
      ps.setString(1, snapshot.concurrencyKey());
      setUuid(ps, 2, snapshot.workflowRootId().asUuid());
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  private static void adjustWorkflowHoldOnTransition(
      Connection conn, JobSnapshot oldSnapshot, JobState newState) throws SQLException {
    if (oldSnapshot.concurrencyKey() == null) {
      return;
    }
    if (isTerminal(oldSnapshot.currentState()) && !isTerminal(newState)) {
      // Mirror of the release for the terminal -> non-terminal resurrect (the
      // FAILED -> SCHEDULED retry path). Without it the job is decremented
      // twice and an EXCLUSIVE key can be released while a descendant still
      // runs. Zero rows matched is fine: a standalone job whose hold was fully
      // released re-registers at its next claim.
      try (PreparedStatement ps = conn.prepareStatement("UPDATE "
          + "threadmill_concurrency_workflow_holds SET outstanding = outstanding + 1 "
          + "WHERE concurrency_key = ? AND workflow_root_id = ?")) {
        ps.setString(1, oldSnapshot.concurrencyKey());
        setUuid(ps, 2, oldSnapshot.workflowRootId().asUuid());
        ps.executeUpdate();
      }
      return;
    }
    if (isTerminal(oldSnapshot.currentState()) || !isTerminal(newState)) {
      return;
    }
    releaseWorkflowHoldShare(
        conn,
        oldSnapshot.concurrencyKey(),
        oldSnapshot.concurrencyMode(),
        oldSnapshot.workflowRootId().asUuid());
  }

  /**
   * One member of the root's workflow left the non-terminal population:
   * decrement the hold's outstanding count and, when it reaches zero, delete
   * the hold and free its in-flight slot. A missing hold row is a no-op —
   * never-claimed standalone jobs hold nothing. Caller holds the group lock.
   */
  private static void releaseWorkflowHoldShare(
      Connection conn, String concurrencyKey, ConcurrencyMode mode, UUID root) throws SQLException {
    int outstanding;
    try (PreparedStatement ps = conn.prepareStatement("SELECT outstanding FROM "
        + "threadmill_concurrency_workflow_holds WHERE concurrency_key = ? AND workflow_root_id = ? "
        + "FOR UPDATE")) {
      ps.setString(1, concurrencyKey);
      setUuid(ps, 2, root);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return;
        }
        outstanding = rs.getInt(1) - 1;
      }
    }
    if (outstanding > 0) {
      try (PreparedStatement ps = conn.prepareStatement("UPDATE "
          + "threadmill_concurrency_workflow_holds SET outstanding = ? "
          + "WHERE concurrency_key = ? AND workflow_root_id = ?")) {
        ps.setInt(1, outstanding);
        ps.setString(2, concurrencyKey);
        setUuid(ps, 3, root);
        ps.executeUpdate();
      }
      return;
    }
    try (PreparedStatement ps = conn.prepareStatement(
        "DELETE FROM "
            + "threadmill_concurrency_workflow_holds WHERE concurrency_key = ? AND workflow_root_id = ?")) {
      ps.setString(1, concurrencyKey);
      setUuid(ps, 2, root);
      ps.executeUpdate();
    }
    String column = mode == ConcurrencyMode.EXCLUSIVE ? "exclusive_in_flight" : "shared_in_flight";
    try (PreparedStatement ps = conn.prepareStatement("UPDATE threadmill_concurrency_groups SET "
        + column + " = GREATEST(" + column + " - 1, 0), last_modified = " + NOW_UTC
        + " WHERE concurrency_key = ?")) {
      ps.setString(1, concurrencyKey);
      ps.executeUpdate();
    }
  }

  private static void insertSnapshot(
      Connection conn, JobSnapshot snapshot, String body, Instant currentStateAt, long version)
      throws SQLException {
    try (var clobs = new OracleClobs();
        PreparedStatement ps = conn.prepareStatement(JOB_INSERT)) {
      bindJobInsert(clobs, ps, snapshot, body, currentStateAt, version);
      ps.executeUpdate();
    }
  }

  private static void bindJobInsert(
      OracleClobs clobs,
      PreparedStatement ps,
      JobSnapshot snapshot,
      String body,
      Instant currentStateAt,
      long version)
      throws SQLException {
    setUuid(ps, 1, snapshot.id().asUuid());
    ps.setString(2, snapshot.currentState().name());
    ps.setString(3, snapshot.queue());
    ps.setInt(4, snapshot.priority());
    ps.setString(5, snapshot.spec().handlerType());
    setInstant(ps, 6, snapshot.scheduledFor());
    setUuid(
        ps, 7, snapshot.ownerNodeId() == null ? null : snapshot.ownerNodeId().asUuid());
    setInstant(ps, 8, snapshot.ownerHeartbeatAt());
    setInstant(ps, 9, snapshot.lastCheckinAt());
    setInstant(ps, 10, currentStateAt);
    ps.setLong(11, version);
    clobs.bind(ps, 12, body);
    setInstant(ps, 13, snapshot.createdAt());
    setNullableConcurrency(ps, 14, snapshot.concurrencyKey(), snapshot.concurrencyMode());
    setUuid(ps, 16, snapshot.workflowRootId().asUuid());
    setUuid(ps, 17, parentJobId(snapshot));
  }

  private static UUID parentJobId(JobSnapshot snapshot) {
    return snapshot.relationship() == null
        ? null
        : snapshot.relationship().parentId().asUuid();
  }

  private static void setNullableConcurrency(
      PreparedStatement ps, int startIndex, String key, ConcurrencyMode mode) throws SQLException {
    if (key == null) {
      ps.setNull(startIndex, Types.VARCHAR);
      ps.setNull(startIndex + 1, Types.VARCHAR);
    } else {
      ps.setString(startIndex, key);
      ps.setString(startIndex + 1, mode.name());
    }
  }

  private static Instant lastTransitionTime(JobSnapshot snapshot, JobState state) {
    List<JobStateEntry> history = snapshot.stateHistory();
    for (int i = history.size() - 1; i >= 0; i--) {
      if (history.get(i).state() == state) return history.get(i).at();
    }
    return snapshot.createdAt();
  }

  private static JobSnapshot withVersion(Job job, long version) {
    JobSnapshot s = job.snapshot();
    return new JobSnapshot(
        s.id(),
        s.spec(),
        s.queue(),
        s.priority(),
        s.createdAt(),
        s.cronTaskName(),
        s.relationship(),
        s.workflowRootId(),
        s.concurrencyKey(),
        s.concurrencyMode(),
        s.stateHistory(),
        new HashMap<>(s.metadata()),
        s.log(),
        s.progress(),
        version,
        s.ownerNodeId(),
        s.ownerHeartbeatAt(),
        s.lastCheckinAt(),
        s.scheduledFor(),
        s.result(),
        s.attempts(),
        s.failureDecision(),
        s.executionRevision());
  }

  private static boolean isTerminal(JobState state) {
    return switch (state) {
      case SUCCEEDED, FAILED, DELETED, QUARANTINED -> true;
      case AWAITING, SCHEDULED, ENQUEUED, PROCESSING, PROCESSED -> false;
    };
  }

  /** Translates {@link SQLException}s from a {@link JobStore} method into an unchecked form. */
  public static class JdbcException extends RuntimeException {
    /**
     * Wrap a JDBC failure.
     *
     * @param message the failed operation
     * @param cause the driver's exception
     */
    public JdbcException(String message, SQLException cause) {
      super(message, cause);
    }
  }
}
