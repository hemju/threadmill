package com.hemju.threadmill.store.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import javax.sql.DataSource;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobEngineFatalException;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobReplacement;
import com.hemju.threadmill.core.JobReplacements;
import com.hemju.threadmill.core.JobSnapshot;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.JobStateEntry;
import com.hemju.threadmill.core.Names;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.engine.RemoteWakeChannel;
import com.hemju.threadmill.core.schedule.CronExpression;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.store.JobSearch;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.core.store.JobStoreCapabilities;
import com.hemju.threadmill.core.store.Mutexes;
import com.hemju.threadmill.core.store.NodeHeartbeat;

/**
 * PostgreSQL implementation of {@link JobStore}.
 *
 * <p>Design highlights:
 * <ul>
 *   <li>The body column (JSON text) is the <strong>source of truth</strong>;
 *       the indexed scalar columns ({@code state}, {@code queue},
 *       {@code priority}, {@code scheduled_at}, {@code handler_signature},
 *       {@code owner_heartbeat_at}, {@code current_state_at}) are
 *       denormalized so hot queries hit indexes without parsing the body.</li>
 *   <li>{@link #claimReady} uses {@code SELECT … FOR UPDATE SKIP LOCKED} so
 *       contending workers never collide and never wait.</li>
 *   <li>Every write is wrapped in {@link DeadlockRetry} — deadlocks on a
 *       busy queue table are normal and the right response is bounded retry
 *       with jittered backoff.</li>
 *   <li>Per-state counts come from {@code threadmill_job_counts}, maintained
 *       row-by-row by a trigger; a naive {@code COUNT(*)} would contend
 *       with the claim path.</li>
 *   <li>Oversized jobs are rejected by the serializer before any SQL runs.
 *       The in-memory version is never corrupted on a failed save.</li>
 * </ul>
 */
public final class PostgresJobStore implements JobStore {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(PostgresJobStore.class);
    private static final String MAINTENANCE_LEASE = "maintenance";

    /** Threadmill requires PostgreSQL 18 or later. See {@link #requirePostgresEighteen}. */
    static final int MINIMUM_SERVER_VERSION_NUM = 180000;

    private final DataSource dataSource;
    private final PostgresTransactionBoundary transactionBoundary;
    private final JobSerializer serializer;
    private final JobStoreCapabilities capabilities;
    private final String serverVersion;
    private final String databaseName;

    public PostgresJobStore(DataSource dataSource) {
        this(dataSource, new JsonJobSerializer(), JobStoreCapabilities.defaults());
    }

    public PostgresJobStore(DataSource dataSource, JobSerializer serializer, JobStoreCapabilities capabilities) {
        this(dataSource, serializer, capabilities, PostgresTransactionBoundary.owning(dataSource));
    }

    public PostgresJobStore(
            DataSource dataSource,
            JobSerializer serializer,
            JobStoreCapabilities capabilities,
            PostgresTransactionBoundary transactionBoundary) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.transactionBoundary = Objects.requireNonNull(transactionBoundary, "transactionBoundary");
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.capabilities = Objects.requireNonNull(capabilities, "capabilities");
        var identity = requirePostgresEighteen(this.dataSource);
        this.serverVersion = identity.serverVersion();
        this.databaseName = identity.database();
    }

    /** Validate that the supplied {@link DataSource} points at supported PostgreSQL. */
    public static void requireSupportedServer(DataSource dataSource) {
        requirePostgresEighteen(dataSource);
    }

    /**
     * Refuse to start against pre-PostgreSQL-18 servers. Threadmill's schema, queries, and
     * migration runner target PostgreSQL 18+ only; older majors are not tested and are not
     * supported. The check runs once at construction so a misconfigured host fails fast
     * with an actionable message rather than at the first query.
     *
     * <p>Also captures the server-version string and database name so {@link #describe()}
     * can read them later in constant time without re-querying the server.
     *
     * @throws JobEngineFatalException if the server reports a {@code server_version_num}
     *     below {@value #MINIMUM_SERVER_VERSION_NUM}, or if the version cannot be read.
     */
    private static ServerIdentity requirePostgresEighteen(DataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            int versionNum;
            try (ResultSet rs = st.executeQuery("SHOW server_version_num")) {
                if (!rs.next()) {
                    throw new JobEngineFatalException(
                            "Could not read PostgreSQL server_version_num — refusing to start");
                }
                versionNum = Integer.parseInt(rs.getString(1).trim());
            }
            if (versionNum < MINIMUM_SERVER_VERSION_NUM) {
                int major = versionNum / 10000;
                throw new JobEngineFatalException(
                        "Threadmill requires PostgreSQL 18 or later — found server major " + major
                                + " (server_version_num=" + versionNum + "). Upgrade the server or use a"
                                + " different backend (Redis, in-memory).");
            }
            String version;
            try (ResultSet rs = st.executeQuery("SHOW server_version")) {
                version = rs.next() ? rs.getString(1).trim() : Integer.toString(versionNum / 10000);
            }
            String database = conn.getCatalog();
            return new ServerIdentity(version, database == null ? "unknown" : database);
        } catch (SQLException e) {
            throw new JobEngineFatalException("Failed to verify PostgreSQL server version", e);
        }
    }

    private record ServerIdentity(String serverVersion, String database) {}

    // ---------------------------------------------------------------- capabilities

    @Override
    public JobStoreCapabilities capabilities() {
        return capabilities;
    }

    /**
     * Liveness probe for the dispatcher's store-outage circuit breaker. The SPI
     * default returns a cached capabilities object and can never fail, so on
     * Postgres the documented "pause and probe the store until it returns"
     * behaviour would otherwise be dead code — the breaker would resume on every
     * probe and thrash. A real {@code SELECT 1} round trip fails while the
     * database is unreachable and succeeds once it returns.
     */
    @Override
    public void verifyWritable() {
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT 1")) {
            rs.next();
        } catch (SQLException e) {
            throw new JdbcException("verifyWritable probe failed", e);
        }
    }

    @Override
    public String describe() {
        return "PostgreSQL " + serverVersion + " @ " + databaseName;
    }

    @Override
    public boolean supportsExternalTransactions() {
        return transactionBoundary.supportsExternalTransactions();
    }

    @Override
    public Optional<RemoteWakeChannel> createRemoteWakeChannel(String channelName) {
        return Optional.of(new PostgresRemoteWakeChannel(dataSource, channelName));
    }

    private <T> T writeTransaction(PostgresConnectionWork<T> work) throws SQLException {
        if (transactionBoundary.externallyManagedTransactionActive()) {
            return transactionBoundary.inTransaction(work);
        }
        return DeadlockRetry.run(() -> transactionBoundary.inTransaction(work));
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
                Instant currentStateAt = lastTransitionTime(snapshot, snapshot.currentState());
                insertSnapshot(conn, snapshot, body, currentStateAt, version);
                noteInsertedWorkflowDescendant(conn, snapshot);
                return null;
            });
        } catch (SQLException e) {
            if (DeadlockRetry.hasSqlState(e, "23505")) {
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

        // Pre-flight: serialize every snapshot up front. OversizedJobException
        // here rejects the whole batch before any DB write — no Job in the
        // input has its version mutated, matching insert()'s invariant.
        record Prepared(Job job, JobSnapshot snapshot, String body, Instant currentStateAt) {}
        var prepared = new ArrayList<Prepared>(jobsToInsert.size());
        for (var j : jobsToInsert) {
            Objects.requireNonNull(j, "job");
            Names.requireName("queue", j.queue());
            // snapshotForInsert(conn, ...) needs a connection for workflow-root resolution;
            // we re-snapshot inside the transaction below. Here we only validate size.
            JobSnapshot probe = j.snapshot();
            String body = serializer.serializeJob(probe, capabilities);
            prepared.add(new Prepared(j, probe, body, lastTransitionTime(probe, probe.currentState())));
        }

        long version = 1L;
        try {
            writeTransaction(conn -> {
                // Re-snapshot inside the txn so workflow_root_id is resolved
                // against the live store state; re-serialize matches.
                var finalSnapshots = new ArrayList<JobSnapshot>(prepared.size());
                var finalBodies = new ArrayList<String>(prepared.size());
                var finalStateAt = new ArrayList<Instant>(prepared.size());
                for (var p : prepared) {
                    JobSnapshot snap = snapshotForInsert(conn, p.job, version);
                    String body = serializer.serializeJob(snap, capabilities);
                    finalSnapshots.add(snap);
                    finalBodies.add(body);
                    finalStateAt.add(lastTransitionTime(snap, snap.currentState()));
                }
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO threadmill_jobs (id, state, queue, priority, handler_signature, "
                                + "scheduled_at, owner_node_id, owner_heartbeat_at, last_checkin_at, current_state_at, "
                                + "version, body, created_at, concurrency_key, concurrency_mode, workflow_root_id, "
                                + "parent_job_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
                    for (int i = 0; i < finalSnapshots.size(); i++) {
                        JobSnapshot snap = finalSnapshots.get(i);
                        ps.setObject(1, snap.id().asUuid());
                        ps.setString(2, snap.currentState().name());
                        ps.setString(3, snap.queue());
                        ps.setInt(4, snap.priority());
                        ps.setString(5, snap.spec().handlerType());
                        setNullableTimestamp(ps, 6, snap.scheduledFor());
                        setNullableUuid(
                                ps,
                                7,
                                snap.ownerNodeId() == null
                                        ? null
                                        : snap.ownerNodeId().asUuid());
                        setNullableTimestamp(ps, 8, snap.ownerHeartbeatAt());
                        setNullableTimestamp(ps, 9, snap.lastCheckinAt());
                        ps.setTimestamp(10, Timestamp.from(finalStateAt.get(i)));
                        ps.setLong(11, version);
                        ps.setString(12, finalBodies.get(i));
                        ps.setTimestamp(13, Timestamp.from(snap.createdAt()));
                        setNullableConcurrency(ps, 14, snap.concurrencyKey(), snap.concurrencyMode());
                        ps.setObject(16, snap.workflowRootId().asUuid());
                        setNullableParentJobId(ps, 17, snap);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                // Lock the distinct concurrency-group rows once, sorted —
                // per-snapshot locking in batch order manufactured deadlocks
                // between concurrent batches with reversed key orders (fatal
                // in join_transaction mode, where retry is disabled).
                var workflowKeys = new TreeSet<String>();
                for (JobSnapshot snap : finalSnapshots) {
                    if (snap.concurrencyKey() != null) {
                        workflowKeys.add(snap.concurrencyKey());
                    }
                }
                if (!workflowKeys.isEmpty()) {
                    lockConcurrencyGroups(conn, workflowKeys);
                    for (JobSnapshot snap : finalSnapshots) {
                        incrementWorkflowHoldOutstanding(conn, snap);
                    }
                }
                return null;
            });
        } catch (SQLException e) {
            if (DeadlockRetry.hasSqlState(e, "23505")) {
                throw new IllegalStateException("Duplicate job id in batch", e);
            }
            throw new JdbcException("insertAll failed", e);
        }
        var ids = new ArrayList<JobId>(prepared.size());
        for (var p : prepared) {
            p.job.adoptVersion(version);
            ids.add(p.job.id());
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
                insertSnapshot(conn, snapshot, body, currentStateAt, version);
                noteInsertedWorkflowDescendant(conn, snapshot);
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO threadmill_dedup_keys (queue, dedup_key, job_id, expires_at) VALUES (?, ?, ?, ?)")) {
                    ps.setString(1, job.queue());
                    ps.setString(2, dedupKey);
                    ps.setObject(3, job.id().asUuid());
                    ps.setTimestamp(4, Timestamp.from(now.plus(ttl)));
                    ps.executeUpdate();
                }
                return new EnqueueResult.Created(job.id());
            });
            if (result instanceof EnqueueResult.Created) {
                job.adoptVersion(version);
            }
            return result;
        } catch (SQLException e) {
            if (DeadlockRetry.hasSqlState(e, "23505") && !transactionBoundary.externallyManagedTransactionActive()) {
                try {
                    Optional<JobId> existing = findActiveDedup(job.queue(), dedupKey, now);
                    if (existing.isPresent()) return new EnqueueResult.Coalesced(existing.get());
                } catch (SQLException lookupFailure) {
                    e.addSuppressed(lookupFailure);
                }
            }
            throw new JdbcException("enqueueIfAbsent failed", e);
        }
    }

    @Override
    public Optional<Job> findById(JobId id) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT body FROM threadmill_jobs WHERE id = ?")) {
            ps.setObject(1, id.asUuid());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(serializer.deserializeJob(rs.getString(1)));
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
        // Serialize BEFORE any DB work so OversizedJobException can't corrupt state.
        String body = serializer.serializeJob(snapshot, capabilities);
        Instant currentStateAt = lastTransitionTime(snapshot, snapshot.currentState());

        boolean saved;
        try {
            saved = DeadlockRetry.run(() -> {
                try (Connection conn = dataSource.getConnection()) {
                    conn.setAutoCommit(false);
                    try {
                        JobSnapshot oldSnapshot;
                        try (PreparedStatement ps = conn.prepareStatement(
                                "SELECT body, version FROM threadmill_jobs WHERE id = ? FOR UPDATE")) {
                            ps.setObject(1, snapshot.id().asUuid());
                            try (ResultSet rs = ps.executeQuery()) {
                                if (!rs.next()) {
                                    conn.commit();
                                    return false;
                                }
                                if (rs.getLong(2) != expectedVersion) {
                                    conn.commit();
                                    return false;
                                }
                                oldSnapshot = serializer
                                        .deserializeJob(rs.getString(1))
                                        .snapshot();
                            }
                        }
                        if (oldSnapshot.concurrencyKey() != null) {
                            lockConcurrencyGroup(conn, oldSnapshot.concurrencyKey());
                        }
                        adjustWorkflowHoldOnTransition(conn, oldSnapshot, snapshot.currentState());
                        try (PreparedStatement ps = conn.prepareStatement("UPDATE threadmill_jobs SET "
                                + "state = ?, queue = ?, priority = ?, handler_signature = ?, "
                                + "scheduled_at = ?, owner_node_id = ?, owner_heartbeat_at = ?, last_checkin_at = ?, "
                                + "current_state_at = ?, version = ?, body = ?, "
                                + "concurrency_key = ?, concurrency_mode = ?, workflow_root_id = ?, parent_job_id = ? "
                                + "WHERE id = ? AND version = ?")) {
                            ps.setString(1, snapshot.currentState().name());
                            ps.setString(2, snapshot.queue());
                            ps.setInt(3, snapshot.priority());
                            ps.setString(4, snapshot.spec().handlerType());
                            setNullableTimestamp(ps, 5, snapshot.scheduledFor());
                            setNullableUuid(
                                    ps,
                                    6,
                                    snapshot.ownerNodeId() == null
                                            ? null
                                            : snapshot.ownerNodeId().asUuid());
                            setNullableTimestamp(ps, 7, snapshot.ownerHeartbeatAt());
                            setNullableTimestamp(ps, 8, snapshot.lastCheckinAt());
                            ps.setTimestamp(9, Timestamp.from(currentStateAt));
                            ps.setLong(10, nextVersion);
                            ps.setString(11, body);
                            setNullableConcurrency(ps, 12, snapshot.concurrencyKey(), snapshot.concurrencyMode());
                            ps.setObject(14, snapshot.workflowRootId().asUuid());
                            setNullableParentJobId(ps, 15, snapshot);
                            ps.setObject(16, snapshot.id().asUuid());
                            ps.setLong(17, expectedVersion);
                            int rows = ps.executeUpdate();
                            conn.commit();
                            return rows > 0;
                        }
                    } catch (RuntimeException | SQLException e) {
                        conn.rollback();
                        throw e;
                    } finally {
                        conn.setAutoCommit(true);
                    }
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
            return DeadlockRetry.run(() -> {
                try (Connection conn = dataSource.getConnection()) {
                    conn.setAutoCommit(false);
                    try {
                        String body;
                        long version;
                        try (PreparedStatement ps = conn.prepareStatement(
                                "SELECT body, version FROM threadmill_jobs WHERE id = ? FOR UPDATE")) {
                            ps.setObject(1, id.asUuid());
                            try (ResultSet rs = ps.executeQuery()) {
                                if (!rs.next()) {
                                    conn.commit();
                                    return false;
                                }
                                body = rs.getString(1);
                                version = rs.getLong(2);
                            }
                        }
                        Job j = serializer.deserializeJob(body);
                        if (j.currentState() == JobState.DELETED) {
                            conn.commit();
                            return false;
                        }
                        JobSnapshot oldSnapshot = j.snapshot();
                        if (oldSnapshot.concurrencyKey() != null) {
                            lockConcurrencyGroup(conn, oldSnapshot.concurrencyKey());
                        }
                        j.transitionTo(JobState.DELETED, Instant.now(), "user.delete", null);
                        long nextVersion = version + 1;
                        JobSnapshot snapshot = withVersion(j, nextVersion);
                        String newBody = serializer.serializeJob(snapshot, capabilities);
                        Instant currentStateAt = lastTransitionTime(snapshot, JobState.DELETED);
                        adjustWorkflowHoldOnTransition(conn, oldSnapshot, JobState.DELETED);
                        try (PreparedStatement ps = conn.prepareStatement(
                                "UPDATE threadmill_jobs SET state = ?, version = ?, body = ?, current_state_at = ? "
                                        + "WHERE id = ?")) {
                            ps.setString(1, JobState.DELETED.name());
                            ps.setLong(2, nextVersion);
                            ps.setString(3, newBody);
                            ps.setTimestamp(4, Timestamp.from(currentStateAt));
                            ps.setObject(5, id.asUuid());
                            ps.executeUpdate();
                        }
                        conn.commit();
                        return true;
                    } catch (RuntimeException | SQLException e) {
                        conn.rollback();
                        throw e;
                    } finally {
                        conn.setAutoCommit(true);
                    }
                }
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
            return DeadlockRetry.run(() -> {
                List<Job> result = new ArrayList<>();
                try (Connection conn = dataSource.getConnection()) {
                    conn.setAutoCommit(false);
                    try {
                        // 1. Atomically page through ready ids, skipping any locked by another worker.
                        // The first page is narrow — FOR UPDATE locks every row it reads and
                        // holds it until commit, so a wide first page would pin 64x the claim
                        // budget and starve concurrent claimers' SKIP LOCKED scans. Only when
                        // a page yields zero claimable candidates (a concurrency-blocked hot
                        // key hiding claimable work deeper in the queue) does the scan
                        // escalate to the wide page.
                        int pageSize = narrowClaimPageSize(cap);
                        Integer cursorPriority = null;
                        UUID cursorId = null;

                        // 2. For each, deserialize, transition to PROCESSING, re-serialize, and UPDATE the row.
                        // Version-matched as defense-in-depth: correctness rests on the
                        // FOR UPDATE SKIP LOCKED row lock from readClaimPage, but if a
                        // future refactor ever fetches candidates without it, this turns a
                        // silent double-claim into a loud failure.
                        try (PreparedStatement ps = conn.prepareStatement(
                                "UPDATE threadmill_jobs SET state = 'PROCESSING', owner_node_id = ?, "
                                        + "owner_heartbeat_at = ?, last_checkin_at = NULL, current_state_at = ?, version = ?, body = ? "
                                        + "WHERE id = ? AND version = ?")) {
                            while (result.size() < cap) {
                                List<PendingClaim> pending =
                                        readClaimPage(conn, queue, pageSize, cursorPriority, cursorId);
                                if (pending.isEmpty()) {
                                    break;
                                }
                                List<PendingClaim> claimable = claimableCandidates(conn, pending, cap - result.size());
                                Map<UUID, String> bodies = fetchBodies(conn, claimable);
                                for (var p : claimable) {
                                    if (result.size() >= cap) break;
                                    Job j;
                                    try {
                                        j = serializer.deserializeJob(bodies.get(p.id));
                                    } catch (RuntimeException corrupt) {
                                        // An undeserializable body (e.g. a wire form a rollback
                                        // can't read, or external corruption) must not fail the
                                        // whole claim and wedge the queue. Quarantine it via a
                                        // body-independent scalar update so it leaves the
                                        // ENQUEUED claim path, and continue with the rest.
                                        quarantineUnreadable(conn, p.id, p.version, heartbeatAt);
                                        continue;
                                    }
                                    acquireWorkflowHold(conn, j.snapshot());
                                    j.transitionTo(JobState.PROCESSING, heartbeatAt, "engine.claim", null);
                                    j.assignOwner(nodeId, heartbeatAt);
                                    j.incrementAttempts();
                                    long nextVersion = p.version + 1;
                                    JobSnapshot snap = withVersion(j, nextVersion);
                                    String newBody = serializer.serializeJob(snap, capabilities);
                                    ps.setObject(1, nodeId.asUuid());
                                    ps.setTimestamp(2, Timestamp.from(heartbeatAt));
                                    ps.setTimestamp(3, Timestamp.from(heartbeatAt));
                                    ps.setLong(4, nextVersion);
                                    ps.setString(5, newBody);
                                    ps.setObject(6, p.id);
                                    ps.setLong(7, p.version);
                                    ps.addBatch();
                                    result.add(serializer.deserializeJob(newBody));
                                }
                                PendingClaim last = pending.get(pending.size() - 1);
                                cursorPriority = last.priority;
                                cursorId = last.id;
                                if (pending.size() < pageSize) {
                                    break;
                                }
                                if (claimable.isEmpty()) {
                                    pageSize = wideClaimPageSize(cap);
                                }
                            }
                            int[] updated = ps.executeBatch();
                            for (int count : updated) {
                                if (count != 1) {
                                    throw new IllegalStateException(
                                            "Claim UPDATE matched " + count + " rows — the row lock taken by "
                                                    + "readClaimPage no longer guarantees claim exclusivity");
                                }
                            }
                        }
                        conn.commit();
                        return result;
                    } catch (RuntimeException | SQLException e) {
                        conn.rollback();
                        throw e;
                    } finally {
                        conn.setAutoCommit(true);
                    }
                }
            });
        } catch (SQLException e) {
            throw new JdbcException("claimReady failed", e);
        }
    }

    private List<PendingClaim> readClaimPage(
            Connection conn, String queue, int limit, Integer cursorPriority, UUID cursorId) throws SQLException {
        String cursorPredicate = cursorPriority == null ? "" : "AND (priority < ? OR (priority = ? AND id > ?)) ";
        try (PreparedStatement ps = conn.prepareStatement("SELECT id, version, priority, concurrency_key, "
                + "concurrency_mode, workflow_root_id, current_state_at FROM threadmill_jobs "
                + "WHERE state = 'ENQUEUED' AND queue = ? "
                + cursorPredicate
                + "ORDER BY priority DESC, id "
                + "LIMIT ? FOR UPDATE SKIP LOCKED")) {
            ps.setString(1, queue);
            int index = 2;
            if (cursorPriority != null) {
                ps.setInt(index++, cursorPriority);
                ps.setInt(index++, cursorPriority);
                ps.setObject(index++, cursorId);
            }
            ps.setInt(index, limit);
            try (ResultSet rs = ps.executeQuery()) {
                var page = new ArrayList<PendingClaim>();
                while (rs.next()) {
                    String mode = rs.getString(5);
                    page.add(new PendingClaim(
                            (UUID) rs.getObject(1),
                            rs.getLong(2),
                            rs.getInt(3),
                            rs.getString(4),
                            mode == null ? null : ConcurrencyMode.valueOf(mode),
                            (UUID) rs.getObject(6),
                            rs.getTimestamp(7).toInstant()));
                }
                return page;
            }
        }
    }

    private List<PendingClaim> claimableCandidates(Connection conn, List<PendingClaim> pending, int remaining)
            throws SQLException {
        Set<String> keys = new HashSet<>();
        for (var candidate : pending) {
            if (candidate.concurrencyKey != null) {
                keys.add(candidate.concurrencyKey);
            }
        }
        if (keys.isEmpty()) {
            return pending.subList(0, Math.min(remaining, pending.size()));
        }
        lockConcurrencyGroups(conn, keys);
        Map<String, GroupState> groups = loadGroupStates(conn, keys);
        Set<WorkflowKey> activeHolds = loadActiveWorkflowHolds(conn, pending, keys);
        Map<String, PendingOrder> firstPending = loadFirstPendingByKey(conn, keys, false);
        Map<String, PendingOrder> firstExclusivePending = loadFirstPendingByKey(conn, keys, true);
        var claimable = new ArrayList<PendingClaim>(Math.min(remaining, pending.size()));
        for (var candidate : pending) {
            if (claimable.size() >= remaining) {
                break;
            }
            if (canClaim(candidate, groups, activeHolds, firstPending, firstExclusivePending)) {
                claimable.add(candidate);
            }
        }
        return claimable;
    }

    private Map<UUID, String> fetchBodies(Connection conn, List<PendingClaim> claimable) throws SQLException {
        if (claimable.isEmpty()) {
            return Map.of();
        }
        UUID[] ids = claimable.stream().map(PendingClaim::id).toArray(UUID[]::new);
        var idArray = conn.createArrayOf("uuid", ids);
        try (PreparedStatement ps = conn.prepareStatement("SELECT id, body FROM threadmill_jobs WHERE id = ANY (?)")) {
            ps.setArray(1, idArray);
            try (ResultSet rs = ps.executeQuery()) {
                var bodies = new HashMap<UUID, String>();
                while (rs.next()) {
                    bodies.put((UUID) rs.getObject(1), rs.getString(2));
                }
                return bodies;
            }
        } finally {
            idArray.free();
        }
    }

    /**
     * Move an ENQUEUED job with an unreadable body out of the claim path via a
     * scalar update — no body deserialize needed. Runs in the claim transaction;
     * the counts trigger reconciles ENQUEUED → QUARANTINED.
     */
    private void quarantineUnreadable(Connection conn, UUID id, long version, Instant now) throws SQLException {
        String concurrencyKey;
        String concurrencyMode;
        UUID workflowRoot;
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE threadmill_jobs SET state = 'QUARANTINED', current_state_at = ?, version = ? "
                        + "WHERE id = ? AND version = ? AND state = 'ENQUEUED' "
                        + "RETURNING concurrency_key, concurrency_mode, workflow_root_id")) {
            ps.setTimestamp(1, Timestamp.from(now));
            ps.setLong(2, version + 1);
            ps.setObject(3, id);
            ps.setLong(4, version);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return; // raced away — nothing was quarantined
                }
                concurrencyKey = rs.getString(1);
                concurrencyMode = rs.getString(2);
                workflowRoot = (UUID) rs.getObject(3);
            }
        }
        // QUARANTINED is terminal: if the job's workflow root holds the key,
        // this member's share must be released, or the key stays held forever
        // (the queue-level wedge would just move to the concurrency key).
        if (concurrencyKey != null && workflowRoot != null) {
            releaseWorkflowHoldShare(
                    conn,
                    concurrencyKey,
                    concurrencyMode == null ? null : ConcurrencyMode.valueOf(concurrencyMode),
                    workflowRoot);
        }
        LOG.warn("Quarantined job {} during claim: its persisted body could not be deserialized", id);
    }

    private void lockConcurrencyGroups(Connection conn, Set<String> keys) throws SQLException {
        var sorted = keys.stream().sorted().toList();
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO threadmill_concurrency_groups "
                + "(concurrency_key, exclusive_in_flight, shared_in_flight, last_modified) "
                + "VALUES (?, 0, 0, clock_timestamp()) "
                + "ON CONFLICT (concurrency_key) DO NOTHING")) {
            for (String key : sorted) {
                ps.setString(1, key);
                ps.addBatch();
            }
            ps.executeBatch();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT concurrency_key FROM threadmill_concurrency_groups WHERE concurrency_key = ? FOR UPDATE")) {
            for (String key : sorted) {
                ps.setString(1, key);
                ps.execute();
            }
        }
    }

    private Map<String, GroupState> loadGroupStates(Connection conn, Set<String> keys) throws SQLException {
        var keyArray = conn.createArrayOf("text", keys.toArray(String[]::new));
        try (PreparedStatement ps =
                conn.prepareStatement("SELECT concurrency_key, exclusive_in_flight, shared_in_flight "
                        + "FROM threadmill_concurrency_groups WHERE concurrency_key = ANY (?)")) {
            ps.setArray(1, keyArray);
            try (ResultSet rs = ps.executeQuery()) {
                var groups = new HashMap<String, GroupState>();
                while (rs.next()) {
                    groups.put(rs.getString(1), new GroupState(rs.getInt(2), rs.getInt(3)));
                }
                return groups;
            }
        } finally {
            keyArray.free();
        }
    }

    private Set<WorkflowKey> loadActiveWorkflowHolds(Connection conn, List<PendingClaim> pending, Set<String> keys)
            throws SQLException {
        var roots = pending.stream()
                .filter(candidate -> candidate.concurrencyKey != null)
                .map(PendingClaim::workflowRootId)
                .distinct()
                .toArray(UUID[]::new);
        if (roots.length == 0) {
            return Set.of();
        }
        var keyArray = conn.createArrayOf("text", keys.toArray(String[]::new));
        var rootArray = conn.createArrayOf("uuid", roots);
        try (PreparedStatement ps = conn.prepareStatement("SELECT concurrency_key, workflow_root_id "
                + "FROM threadmill_concurrency_workflow_holds "
                + "WHERE concurrency_key = ANY (?) AND workflow_root_id = ANY (?)")) {
            ps.setArray(1, keyArray);
            ps.setArray(2, rootArray);
            try (ResultSet rs = ps.executeQuery()) {
                var holds = new HashSet<WorkflowKey>();
                while (rs.next()) {
                    holds.add(new WorkflowKey(rs.getString(1), (UUID) rs.getObject(2)));
                }
                return holds;
            }
        } finally {
            keyArray.free();
            rootArray.free();
        }
    }

    private Map<String, PendingOrder> loadFirstPendingByKey(Connection conn, Set<String> keys, boolean exclusiveOnly)
            throws SQLException {
        var keyArray = conn.createArrayOf("text", keys.toArray(String[]::new));
        String modePredicate = exclusiveOnly ? "AND concurrency_mode = 'EXCLUSIVE' " : "";
        try (PreparedStatement ps = conn.prepareStatement("SELECT DISTINCT ON (concurrency_key) "
                + "concurrency_key, current_state_at, id FROM threadmill_jobs "
                + "WHERE concurrency_key = ANY (?) "
                + modePredicate
                + "AND state IN ('ENQUEUED','SCHEDULED','AWAITING') "
                + "ORDER BY concurrency_key, current_state_at, id")) {
            ps.setArray(1, keyArray);
            try (ResultSet rs = ps.executeQuery()) {
                var first = new HashMap<String, PendingOrder>();
                while (rs.next()) {
                    first.put(
                            rs.getString(1), new PendingOrder(rs.getTimestamp(2).toInstant(), (UUID) rs.getObject(3)));
                }
                return first;
            }
        } finally {
            keyArray.free();
        }
    }

    private static boolean canClaim(
            PendingClaim candidate,
            Map<String, GroupState> groups,
            Set<WorkflowKey> activeHolds,
            Map<String, PendingOrder> firstPending,
            Map<String, PendingOrder> firstExclusivePending) {
        if (candidate.concurrencyKey == null) {
            return true;
        }
        if (activeHolds.contains(new WorkflowKey(candidate.concurrencyKey, candidate.workflowRootId))) {
            return true;
        }
        GroupState group = groups.getOrDefault(candidate.concurrencyKey, GroupState.IDLE);
        if (candidate.concurrencyMode == ConcurrencyMode.EXCLUSIVE) {
            return group.isIdle() && !isBefore(firstPending.get(candidate.concurrencyKey), candidate);
        }
        return group.exclusiveInFlight == 0
                && !isBefore(firstExclusivePending.get(candidate.concurrencyKey), candidate);
    }

    private static boolean isBefore(PendingOrder possibleEarlier, PendingClaim candidate) {
        if (possibleEarlier == null || possibleEarlier.id.equals(candidate.id)) {
            return false;
        }
        int timeOrder = possibleEarlier.currentStateAt.compareTo(candidate.currentStateAt);
        if (timeOrder != 0) {
            return timeOrder < 0;
        }
        return possibleEarlier.id.compareTo(candidate.id) < 0;
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

    private record PendingOrder(Instant currentStateAt, UUID id) {}

    private record WorkflowKey(String concurrencyKey, UUID workflowRootId) {}

    // ---------------------------------------------------------------- queue pauses

    @Override
    public void pauseQueue(String queue, String reason) {
        Names.requireName("queue", queue);
        try {
            DeadlockRetry.run(() -> {
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement ps = conn.prepareStatement("INSERT INTO threadmill_queue_pauses "
                                + "(queue, paused_at, paused_by) VALUES (?, ?, ?) "
                                + "ON CONFLICT (queue) DO UPDATE SET paused_at = EXCLUDED.paused_at, "
                                + "paused_by = EXCLUDED.paused_by")) {
                    ps.setString(1, queue);
                    ps.setTimestamp(2, Timestamp.from(Instant.now()));
                    ps.setString(3, reason);
                    ps.executeUpdate();
                    return null;
                }
            });
        } catch (SQLException e) {
            throw new JdbcException("pauseQueue failed", e);
        }
    }

    @Override
    public void resumeQueue(String queue) {
        Names.requireName("queue", queue);
        try {
            DeadlockRetry.run(() -> {
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement ps =
                                conn.prepareStatement("DELETE FROM threadmill_queue_pauses WHERE queue = ?")) {
                    ps.setString(1, queue);
                    ps.executeUpdate();
                    return null;
                }
            });
        } catch (SQLException e) {
            throw new JdbcException("resumeQueue failed", e);
        }
    }

    @Override
    public Set<String> listPausedQueues() {
        try {
            return DeadlockRetry.run(() -> {
                var out = new HashSet<String>();
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement ps = conn.prepareStatement("SELECT queue FROM threadmill_queue_pauses");
                        ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) out.add(rs.getString(1));
                }
                return Set.copyOf(out);
            });
        } catch (SQLException e) {
            throw new JdbcException("listPausedQueues failed", e);
        }
    }

    private boolean isQueuePaused(String queue) {
        try {
            return DeadlockRetry.run(() -> {
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement ps =
                                conn.prepareStatement("SELECT 1 FROM threadmill_queue_pauses WHERE queue = ?")) {
                    ps.setString(1, queue);
                    try (ResultSet rs = ps.executeQuery()) {
                        return rs.next();
                    }
                }
            });
        } catch (SQLException e) {
            throw new JdbcException("isQueuePaused failed", e);
        }
    }

    @Override
    public void touchOwnerHeartbeat(NodeId nodeId, Instant now) {
        try {
            DeadlockRetry.run(() -> {
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement ps =
                                conn.prepareStatement("UPDATE threadmill_jobs SET owner_heartbeat_at = ? "
                                        + "WHERE state = 'PROCESSING' AND owner_node_id = ?")) {
                    ps.setTimestamp(1, Timestamp.from(now));
                    ps.setObject(2, nodeId.asUuid());
                    ps.executeUpdate();
                    return null;
                }
            });
        } catch (SQLException e) {
            throw new JdbcException("touchOwnerHeartbeat failed", e);
        }
    }

    @Override
    public boolean saveExecutionUpdate(Job job, NodeId nodeId) {
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(nodeId, "nodeId");
        JobSnapshot snapshot = withVersion(job, job.version());
        String body = serializer.serializeJob(snapshot, capabilities);
        try {
            return DeadlockRetry.run(() -> {
                try (Connection conn = dataSource.getConnection();
                        // The version guard rejects a zombie flush from a previous
                        // attempt: a claim bumps version while a check-in does not,
                        // so an attempt-N flush whose job was orphan-reclaimed,
                        // retried, and re-claimed (as attempt N+1) by the same node
                        // no longer matches the live row's version and is dropped.
                        PreparedStatement ps = conn.prepareStatement("UPDATE threadmill_jobs SET "
                                + "owner_heartbeat_at = ?, last_checkin_at = ?, body = ? "
                                + "WHERE id = ? AND state = 'PROCESSING' AND owner_node_id = ? AND version = ?")) {
                    Instant heartbeat =
                            snapshot.lastCheckinAt() == null ? snapshot.ownerHeartbeatAt() : snapshot.lastCheckinAt();
                    setNullableTimestamp(ps, 1, heartbeat);
                    setNullableTimestamp(ps, 2, snapshot.lastCheckinAt());
                    ps.setString(3, body);
                    ps.setObject(4, snapshot.id().asUuid());
                    ps.setObject(5, nodeId.asUuid());
                    ps.setLong(6, snapshot.version());
                    return ps.executeUpdate() > 0;
                }
            });
        } catch (SQLException e) {
            throw new JdbcException("saveExecutionUpdate failed", e);
        }
    }

    @Override
    public void recordNodeHeartbeat(NodeId nodeId, Instant now) {
        try {
            DeadlockRetry.run(() -> {
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement ps = conn.prepareStatement(
                                "INSERT INTO threadmill_nodes (id, last_heartbeat_at) VALUES (?, ?) "
                                        + "ON CONFLICT (id) DO UPDATE SET last_heartbeat_at = EXCLUDED.last_heartbeat_at")) {
                    ps.setObject(1, nodeId.asUuid());
                    ps.setTimestamp(2, Timestamp.from(now));
                    ps.executeUpdate();
                    return null;
                }
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
            ps.setObject(1, nodeId.asUuid());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(rs.getTimestamp(1).toInstant());
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
            return DeadlockRetry.run(() -> {
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement ps =
                                conn.prepareStatement("INSERT INTO threadmill_leases (name, holder, expires_at) "
                                        + "VALUES (?, ?, clock_timestamp() + (? * interval '1 millisecond')) "
                                        + "ON CONFLICT (name) DO UPDATE "
                                        + "SET holder = EXCLUDED.holder, expires_at = EXCLUDED.expires_at "
                                        + "WHERE threadmill_leases.holder = EXCLUDED.holder "
                                        + "OR threadmill_leases.expires_at <= clock_timestamp()")) {
                    ps.setString(1, MAINTENANCE_LEASE);
                    ps.setObject(2, nodeId.asUuid());
                    ps.setLong(3, leaseDuration.toMillis());
                    return ps.executeUpdate() > 0;
                }
            });
        } catch (SQLException e) {
            throw new JdbcException("acquireOrRenewMaintenanceLease failed", e);
        }
    }

    @Override
    public void releaseMaintenanceLease(NodeId nodeId) {
        Objects.requireNonNull(nodeId, "nodeId");
        try {
            DeadlockRetry.run(() -> {
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement ps =
                                conn.prepareStatement("DELETE FROM threadmill_leases WHERE name = ? AND holder = ?")) {
                    ps.setString(1, MAINTENANCE_LEASE);
                    ps.setObject(2, nodeId.asUuid());
                    ps.executeUpdate();
                    return null;
                }
            });
        } catch (SQLException e) {
            throw new JdbcException("releaseMaintenanceLease failed", e);
        }
    }

    @Override
    public Optional<NodeId> readMaintenanceLeaseOwner() {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT holder FROM threadmill_leases WHERE name = ? AND expires_at > clock_timestamp()")) {
            ps.setString(1, MAINTENANCE_LEASE);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(NodeId.of((UUID) rs.getObject(1)));
            }
        } catch (SQLException e) {
            throw new JdbcException("readMaintenanceLeaseOwner failed", e);
        }
    }

    // ---------------------------------------------------------------- housekeeping queries

    @Override
    public List<Job> findDueForPromotion(Instant now, int max) {
        return queryJobs(
                "SELECT body FROM threadmill_jobs WHERE state = 'SCHEDULED' AND scheduled_at <= ? "
                        + "ORDER BY scheduled_at LIMIT ?",
                ps -> {
                    ps.setTimestamp(1, Timestamp.from(now));
                    ps.setInt(2, Math.max(0, max));
                });
    }

    @Override
    public List<Job> findOrphaned(Instant heartbeatExpiry, int max) {
        return queryJobs(
                "SELECT body FROM threadmill_jobs WHERE state = 'PROCESSING' "
                        + "AND GREATEST(owner_heartbeat_at, COALESCE(last_checkin_at, owner_heartbeat_at)) <= ? "
                        + "ORDER BY GREATEST(owner_heartbeat_at, COALESCE(last_checkin_at, owner_heartbeat_at)) LIMIT ?",
                ps -> {
                    ps.setTimestamp(1, Timestamp.from(heartbeatExpiry));
                    ps.setInt(2, Math.max(0, max));
                });
    }

    // ---------------------------------------------------------------- counts & search

    @Override
    public Map<JobState, Long> countsByState() {
        var counts = new EnumMap<JobState, Long>(JobState.class);
        for (JobState s : JobState.values()) counts.put(s, 0L);
        // Counts are sharded across 16 rows per state (V2) so concurrent
        // writers touch disjoint rows; only the SUM is meaningful.
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps =
                        conn.prepareStatement("SELECT state, SUM(count) FROM threadmill_job_counts GROUP BY state");
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                try {
                    counts.put(JobState.valueOf(rs.getString(1)), rs.getLong(2));
                } catch (IllegalArgumentException ignored) {
                    // unknown state from an older schema — ignore
                }
            }
        } catch (SQLException e) {
            throw new JdbcException("countsByState failed", e);
        }
        return counts;
    }

    @Override
    public Map<String, Long> queueDepths() {
        Map<String, Long> depths = new HashMap<>();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT queue, count(*) FROM threadmill_jobs WHERE state = 'ENQUEUED' GROUP BY queue");
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
        List<String> queues = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT DISTINCT queue FROM threadmill_jobs WHERE state = 'ENQUEUED' ORDER BY queue");
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                queues.add(rs.getString(1));
            }
            return queues;
        } catch (SQLException e) {
            throw new JdbcException("listEnqueuedQueues failed", e);
        }
    }

    @Override
    public List<Job> searchJobs(JobSearch search) {
        Objects.requireNonNull(search, "search");
        var sql = new StringBuilder("SELECT body FROM threadmill_jobs WHERE 1=1");
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
        sql.append(" ORDER BY current_state_at DESC, id ASC LIMIT ? OFFSET ?");
        args.add(search.limit());
        args.add(search.offset());
        return queryJobs(sql.toString(), ps -> {
            for (int i = 0; i < args.size(); i++) {
                ps.setObject(i + 1, args.get(i));
            }
        });
    }

    @Override
    public Optional<Instant> oldestEnqueuedAt(String queue) {
        Names.requireName("queue", queue);
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT current_state_at FROM threadmill_jobs "
                        + "WHERE state = 'ENQUEUED' AND queue = ? ORDER BY current_state_at LIMIT 1")) {
            ps.setString(1, queue);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(rs.getTimestamp(1).toInstant());
            }
        } catch (SQLException e) {
            throw new JdbcException("oldestEnqueuedAt failed", e);
        }
    }

    @Override
    public Optional<Instant> oldestProcessingHeartbeat() {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT owner_heartbeat_at FROM threadmill_jobs "
                        + "WHERE state = 'PROCESSING' ORDER BY owner_heartbeat_at LIMIT 1");
                ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) return Optional.empty();
            return Optional.of(rs.getTimestamp(1).toInstant());
        } catch (SQLException e) {
            throw new JdbcException("oldestProcessingHeartbeat failed", e);
        }
    }

    @Override
    public List<NodeHeartbeat> listNodeHeartbeats() {
        List<NodeHeartbeat> out = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps =
                        conn.prepareStatement("SELECT id, last_heartbeat_at FROM threadmill_nodes ORDER BY id");
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                out.add(new NodeHeartbeat(
                        NodeId.of((UUID) rs.getObject(1)), rs.getTimestamp(2).toInstant()));
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
            return DeadlockRetry.run(() -> {
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement ps =
                                conn.prepareStatement("DELETE FROM threadmill_nodes WHERE last_heartbeat_at <= ?")) {
                    ps.setTimestamp(1, Timestamp.from(cutoff));
                    return (long) ps.executeUpdate();
                }
            });
        } catch (SQLException e) {
            throw new JdbcException("deleteNodeHeartbeatsOlderThan failed", e);
        }
    }

    @Override
    public long deleteExpiredDedupKeys(Instant now, int max) {
        Objects.requireNonNull(now, "now");
        if (max <= 0) return 0L;
        try {
            return DeadlockRetry.run(() -> {
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement ps =
                                conn.prepareStatement("DELETE FROM threadmill_dedup_keys d WHERE d.ctid IN ("
                                        + "SELECT d2.ctid FROM threadmill_dedup_keys d2 "
                                        + "LEFT JOIN threadmill_jobs j ON j.id = d2.job_id "
                                        + "WHERE d2.expires_at <= ? AND (j.id IS NULL OR j.state IN ('SUCCEEDED','FAILED','DELETED','QUARANTINED')) "
                                        + "LIMIT ?)")) {
                    ps.setTimestamp(1, Timestamp.from(now));
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
        return queryJobs("SELECT body FROM threadmill_jobs WHERE handler_signature = ? LIMIT ?", ps -> {
            ps.setString(1, handlerType);
            ps.setInt(2, Math.max(0, max));
        });
    }

    // ---------------------------------------------------------------- retention

    @Override
    public long deleteFinishedOlderThan(Instant cutoff, JobState state, int max) {
        try {
            return DeadlockRetry.run(() -> {
                try (Connection conn = dataSource.getConnection();
                        // Skip a terminal job that still has an unexpired dedup
                        // row: the FK is ON DELETE CASCADE, so deleting it here
                        // would drop a live dedup key and silently cap the dedup
                        // TTL at the retention age. Keep the job until its dedup
                        // expires; the next sweep then removes both.
                        PreparedStatement ps = conn.prepareStatement(
                                "DELETE FROM threadmill_jobs WHERE id IN (" + "SELECT j.id FROM threadmill_jobs j "
                                        + "WHERE j.state = ? AND j.current_state_at <= ? "
                                        + "AND NOT EXISTS (SELECT 1 FROM threadmill_dedup_keys d "
                                        + "WHERE d.job_id = j.id AND d.expires_at > clock_timestamp()) "
                                        + "LIMIT ?)")) {
                    ps.setString(1, state.name());
                    ps.setTimestamp(2, Timestamp.from(cutoff));
                    ps.setInt(3, Math.max(0, max));
                    return (long) ps.executeUpdate();
                }
            });
        } catch (SQLException e) {
            throw new JdbcException("deleteFinishedOlderThan failed", e);
        }
    }

    // ---------------------------------------------------------------- relationships & mutexes

    @Override
    public List<Job> findAwaitingByParent(JobId parentId, int max) {
        Objects.requireNonNull(parentId, "parentId");
        if (max <= 0) return List.of();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT body FROM threadmill_jobs "
                        + "WHERE state = 'AWAITING' AND parent_job_id = ? "
                        + "ORDER BY current_state_at, id LIMIT ?")) {
            ps.setObject(1, parentId.asUuid());
            ps.setInt(2, max);
            List<Job> out = new ArrayList<>();
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
            return DeadlockRetry.run(() -> {
                // Lease expiry uses server-side time (clock_timestamp()) for
                // both write and compare, like the maintenance lease: a node
                // whose clock runs ahead must not be able to steal a mutex
                // whose lease is unexpired by server time.
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement ps = conn.prepareStatement("INSERT INTO threadmill_mutexes "
                                + "(name, holder, expires_at) "
                                + "VALUES (?, ?, clock_timestamp() + (? * interval '1 millisecond')) "
                                + "ON CONFLICT (name) DO UPDATE SET holder = EXCLUDED.holder, expires_at = EXCLUDED.expires_at "
                                + "WHERE threadmill_mutexes.expires_at <= clock_timestamp() "
                                + "OR threadmill_mutexes.holder = EXCLUDED.holder")) {
                    ps.setString(1, name);
                    ps.setString(2, holder);
                    ps.setLong(3, leaseDuration.toMillis());
                    return ps.executeUpdate() > 0;
                }
            });
        } catch (SQLException e) {
            throw new JdbcException("tryAcquireMutex failed", e);
        }
    }

    @Override
    public boolean replaceJob(JobId id, long expectedVersion, JobReplacement replacement) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(replacement, "replacement");
        try {
            return DeadlockRetry.run(() -> {
                try (Connection conn = dataSource.getConnection()) {
                    conn.setAutoCommit(false);
                    try {
                        String body;
                        long version;
                        String state;
                        try (PreparedStatement ps = conn.prepareStatement(
                                "SELECT body, version, state FROM threadmill_jobs WHERE id = ? FOR UPDATE")) {
                            ps.setObject(1, id.asUuid());
                            try (ResultSet rs = ps.executeQuery()) {
                                if (!rs.next()) {
                                    conn.commit();
                                    return false;
                                }
                                body = rs.getString(1);
                                version = rs.getLong(2);
                                state = rs.getString(3);
                            }
                        }
                        if (version != expectedVersion) {
                            conn.commit();
                            throw new StaleJobException(id, expectedVersion);
                        }
                        if (!isReplaceableState(state)) {
                            conn.commit();
                            return false;
                        }
                        Job current = serializer.deserializeJob(body);
                        Job replaced = JobReplacements.apply(current, replacement);
                        long nextVersion = version + 1;
                        JobSnapshot snap = withVersion(replaced, nextVersion);
                        String newBody = serializer.serializeJob(snap, capabilities);
                        Instant currentStateAt = lastTransitionTime(snap, snap.currentState());
                        try (PreparedStatement ps = conn.prepareStatement("UPDATE threadmill_jobs SET "
                                + "queue = ?, priority = ?, handler_signature = ?, scheduled_at = ?, "
                                + "current_state_at = ?, version = ?, body = ?, "
                                + "concurrency_key = ?, concurrency_mode = ?, workflow_root_id = ?, parent_job_id = ? "
                                + "WHERE id = ? AND version = ?")) {
                            ps.setString(1, snap.queue());
                            ps.setInt(2, snap.priority());
                            ps.setString(3, snap.spec().handlerType());
                            setNullableTimestamp(ps, 4, snap.scheduledFor());
                            ps.setTimestamp(5, Timestamp.from(currentStateAt));
                            ps.setLong(6, nextVersion);
                            ps.setString(7, newBody);
                            setNullableConcurrency(ps, 8, snap.concurrencyKey(), snap.concurrencyMode());
                            ps.setObject(10, snap.workflowRootId().asUuid());
                            setNullableParentJobId(ps, 11, snap);
                            ps.setObject(12, id.asUuid());
                            ps.setLong(13, expectedVersion);
                            int rows = ps.executeUpdate();
                            conn.commit();
                            return rows > 0;
                        }
                    } catch (RuntimeException | SQLException e) {
                        conn.rollback();
                        throw e;
                    } finally {
                        conn.setAutoCommit(true);
                    }
                }
            });
        } catch (SQLException e) {
            throw new JdbcException("replaceJob failed", e);
        }
    }

    private static boolean isReplaceableState(String state) {
        return "ENQUEUED".equals(state) || "SCHEDULED".equals(state) || "AWAITING".equals(state);
    }

    @Override
    public void releaseMutex(String name, String holder) {
        Names.requireName("mutex", name);
        try {
            DeadlockRetry.run(() -> {
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement ps =
                                conn.prepareStatement("DELETE FROM threadmill_mutexes WHERE name = ? AND holder = ?")) {
                    ps.setString(1, name);
                    ps.setString(2, holder);
                    ps.executeUpdate();
                    return null;
                }
            });
        } catch (SQLException e) {
            throw new JdbcException("releaseMutex failed", e);
        }
    }

    // ---------------------------------------------------------------- cron tasks

    @Override
    public void upsertCronTask(CronTask task) {
        Objects.requireNonNull(task, "task");
        Names.requireName("cronTask", task.name());
        Names.requireName("queue", task.queue());
        String kind;
        String value;
        CronTask.Trigger trigger = task.trigger();
        if (trigger instanceof CronTask.Trigger.CronExpr ce) {
            kind = "CRON";
            value = ce.expression().expression();
        } else if (trigger instanceof CronTask.Trigger.Interval iv) {
            kind = "INTERVAL";
            value = iv.interval().toString();
        } else {
            throw new IllegalStateException("Unknown trigger kind: " + trigger);
        }
        try {
            DeadlockRetry.run(() -> {
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement ps = conn.prepareStatement(
                                "INSERT INTO threadmill_cron_tasks (name, trigger_kind, trigger_value, handler_signature, "
                                        + "payload_type_tag, payload_serialized, queue, priority, missed_run_policy, time_zone, enabled) "
                                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                                        + "ON CONFLICT (name) DO UPDATE SET "
                                        + "trigger_kind = EXCLUDED.trigger_kind, trigger_value = EXCLUDED.trigger_value, "
                                        + "handler_signature = EXCLUDED.handler_signature, "
                                        + "payload_type_tag = EXCLUDED.payload_type_tag, "
                                        + "payload_serialized = EXCLUDED.payload_serialized, "
                                        + "queue = EXCLUDED.queue, priority = EXCLUDED.priority, "
                                        + "missed_run_policy = EXCLUDED.missed_run_policy, "
                                        + "time_zone = EXCLUDED.time_zone, enabled = EXCLUDED.enabled")) {
                    ps.setString(1, task.name());
                    ps.setString(2, kind);
                    ps.setString(3, value);
                    ps.setString(4, task.handlerType());
                    ps.setString(5, task.payloadArgument().typeTag());
                    ps.setString(6, task.payloadArgument().serialized());
                    ps.setString(7, task.queue());
                    ps.setInt(8, task.priority());
                    ps.setString(9, task.missedRunPolicy().name());
                    ps.setString(10, task.zone().getId());
                    ps.setBoolean(11, task.enabled());
                    ps.executeUpdate();
                    return null;
                }
            });
        } catch (SQLException e) {
            throw new JdbcException("upsertCronTask failed", e);
        }
    }

    @Override
    public Optional<CronTask> findCronTask(String name) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT name, trigger_kind, trigger_value, handler_signature, payload_type_tag, "
                                + "payload_serialized, queue, priority, missed_run_policy, time_zone, enabled "
                                + "FROM threadmill_cron_tasks WHERE name = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(readCronTask(rs));
            }
        } catch (SQLException e) {
            throw new JdbcException("findCronTask failed", e);
        }
    }

    @Override
    public List<CronTask> listCronTasks() {
        List<CronTask> out = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT name, trigger_kind, trigger_value, handler_signature, payload_type_tag, "
                                + "payload_serialized, queue, priority, missed_run_policy, time_zone, enabled "
                                + "FROM threadmill_cron_tasks ORDER BY name");
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(readCronTask(rs));
        } catch (SQLException e) {
            throw new JdbcException("listCronTasks failed", e);
        }
        return out;
    }

    @Override
    public void deleteCronTask(String name) {
        try {
            DeadlockRetry.run(() -> {
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement ps =
                                conn.prepareStatement("DELETE FROM threadmill_cron_tasks WHERE name = ?")) {
                    ps.setString(1, name);
                    ps.executeUpdate();
                    return null;
                }
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
            DeadlockRetry.run(() -> {
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement ps = conn.prepareStatement(
                                "INSERT INTO threadmill_cron_task_ownership (namespace, task_name) VALUES (?, ?) "
                                        + "ON CONFLICT (namespace, task_name) DO NOTHING")) {
                    ps.setString(1, namespace);
                    ps.setString(2, taskName);
                    ps.executeUpdate();
                    return null;
                }
            });
        } catch (SQLException e) {
            throw new JdbcException("recordCronTaskOwnership failed", e);
        }
    }

    @Override
    public Set<String> listCronTaskNamesOwnedBy(String namespace) {
        Names.requireName("cronTaskNamespace", namespace);
        Set<String> out = new HashSet<>();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT task_name FROM threadmill_cron_task_ownership WHERE namespace = ?")) {
            ps.setString(1, namespace);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(rs.getString(1));
                }
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
            DeadlockRetry.run(() -> {
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement ps = conn.prepareStatement(
                                "INSERT INTO threadmill_cron_task_state (task_name, last_run_at, last_run_job_id, "
                                        + "next_run_at, in_flight_job_id) VALUES (?, ?, ?, ?, ?) "
                                        + "ON CONFLICT (task_name) DO UPDATE SET "
                                        + "last_run_at = EXCLUDED.last_run_at, last_run_job_id = EXCLUDED.last_run_job_id, "
                                        + "next_run_at = EXCLUDED.next_run_at, in_flight_job_id = EXCLUDED.in_flight_job_id")) {
                    ps.setString(1, state.taskName());
                    setNullableTimestamp(ps, 2, state.lastRunAt());
                    setNullableUuid(ps, 3, state.lastRunJobId());
                    setNullableTimestamp(ps, 4, state.nextRunAt());
                    setNullableUuid(ps, 5, state.inFlightJobId());
                    ps.executeUpdate();
                    return null;
                }
            });
        } catch (SQLException e) {
            throw new JdbcException("upsertCronTaskState failed", e);
        }
    }

    @Override
    public Optional<CronTaskScheduleState> findCronTaskState(String name) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT task_name, last_run_at, last_run_job_id, next_run_at, in_flight_job_id "
                                + "FROM threadmill_cron_task_state WHERE task_name = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new CronTaskScheduleState(
                        rs.getString(1),
                        rs.getTimestamp(2) == null ? null : rs.getTimestamp(2).toInstant(),
                        (UUID) rs.getObject(3),
                        rs.getTimestamp(4) == null ? null : rs.getTimestamp(4).toInstant(),
                        (UUID) rs.getObject(5)));
            }
        } catch (SQLException e) {
            throw new JdbcException("findCronTaskState failed", e);
        }
    }

    private CronTask readCronTask(ResultSet rs) throws SQLException {
        String name = rs.getString(1);
        String kind = rs.getString(2);
        String value = rs.getString(3);
        CronTask.Trigger trigger;
        if ("CRON".equals(kind)) {
            trigger = new CronTask.Trigger.CronExpr(CronExpression.parse(value));
        } else if ("INTERVAL".equals(kind)) {
            trigger = new CronTask.Trigger.Interval(Duration.parse(value));
        } else {
            throw new SQLException("Unknown trigger_kind: " + kind);
        }
        return new CronTask(
                name,
                trigger,
                rs.getString(4),
                new JobArgument(rs.getString(5), rs.getString(6)),
                rs.getString(7),
                rs.getInt(8),
                CronTask.MissedRunPolicy.valueOf(rs.getString(9)),
                ZoneId.of(rs.getString(10)),
                rs.getBoolean(11));
    }

    // ---------------------------------------------------------------- helpers

    @FunctionalInterface
    private interface StatementSetup {
        void apply(PreparedStatement ps) throws SQLException;
    }

    private List<Job> queryJobs(String sql, StatementSetup setup) {
        List<Job> out = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            setup.apply(ps);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(serializer.deserializeJob(rs.getString(1)));
                }
            }
        } catch (SQLException e) {
            throw new JdbcException("query failed: " + sql, e);
        }
        return out;
    }

    private Optional<JobId> findActiveDedup(Connection conn, String queue, String dedupKey, Instant now)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT d.job_id, d.expires_at, j.state "
                + "FROM threadmill_dedup_keys d LEFT JOIN threadmill_jobs j ON j.id = d.job_id "
                + "WHERE d.queue = ? AND d.dedup_key = ? FOR UPDATE OF d")) {
            ps.setString(1, queue);
            ps.setString(2, dedupKey);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                var id = JobId.of((UUID) rs.getObject(1));
                var expiresAt = rs.getTimestamp(2).toInstant();
                String stateValue = rs.getString(3);
                if (stateValue != null && (expiresAt.isAfter(now) || !isTerminal(JobState.valueOf(stateValue)))) {
                    return Optional.of(id);
                }
            }
        }
        try (PreparedStatement ps =
                conn.prepareStatement("DELETE FROM threadmill_dedup_keys WHERE queue = ? AND dedup_key = ?")) {
            ps.setString(1, queue);
            ps.setString(2, dedupKey);
            ps.executeUpdate();
        }
        return Optional.empty();
    }

    private Optional<JobId> findActiveDedup(String queue, String dedupKey, Instant now) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return findActiveDedup(conn, queue, dedupKey, now);
        }
    }

    private JobSnapshot snapshotForInsert(Connection conn, Job job, long version) throws SQLException {
        JobSnapshot s = withVersion(job, version);
        if (s.relationship() == null) {
            return s;
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT workflow_root_id, concurrency_key, concurrency_mode FROM threadmill_jobs WHERE id = ?")) {
            ps.setObject(1, s.relationship().parentId().asUuid());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return s;
                }
                var rootId = JobId.of((UUID) rs.getObject(1));
                String key = rs.getString(2);
                String modeValue = rs.getString(3);
                ConcurrencyMode mode = modeValue == null ? null : ConcurrencyMode.valueOf(modeValue);
                return new JobSnapshot(
                        s.id(),
                        s.spec(),
                        s.queue(),
                        s.priority(),
                        s.createdAt(),
                        s.cronTaskName(),
                        s.relationship(),
                        rootId,
                        key,
                        mode,
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
                        s.attempts());
            }
        }
    }

    /** First claim page: locks at most 2x the budget in rows. */
    static int narrowClaimPageSize(int cap) {
        return Math.max(cap, cap * 2);
    }

    /** Escalated page for scanning past concurrency-blocked hot keys. */
    static int wideClaimPageSize(int cap) {
        return Math.max(cap, cap * 64);
    }

    private static void lockConcurrencyGroup(Connection conn, String key) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO threadmill_concurrency_groups "
                + "(concurrency_key, exclusive_in_flight, shared_in_flight, last_modified) "
                + "VALUES (?, 0, 0, clock_timestamp()) "
                + "ON CONFLICT (concurrency_key) DO NOTHING")) {
            ps.setString(1, key);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT concurrency_key FROM threadmill_concurrency_groups WHERE concurrency_key = ? FOR UPDATE")) {
            ps.setString(1, key);
            ps.execute();
        }
    }

    private boolean hasActiveWorkflowHoldForRoot(Connection conn, JobSnapshot candidate) throws SQLException {
        try (PreparedStatement ps =
                conn.prepareStatement("SELECT EXISTS (SELECT 1 FROM threadmill_concurrency_workflow_holds "
                        + "WHERE concurrency_key = ? AND workflow_root_id = ?)")) {
            ps.setString(1, candidate.concurrencyKey());
            ps.setObject(2, candidate.workflowRootId().asUuid());
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    private void noteInsertedWorkflowDescendant(Connection conn, JobSnapshot snapshot) throws SQLException {
        if (snapshot.concurrencyKey() == null) {
            return;
        }
        lockConcurrencyGroup(conn, snapshot.concurrencyKey());
        incrementWorkflowHoldOutstanding(conn, snapshot);
    }

    /** Caller must already hold the concurrency-group row lock for the snapshot's key. */
    private void incrementWorkflowHoldOutstanding(Connection conn, JobSnapshot snapshot) throws SQLException {
        if (snapshot.concurrencyKey() == null) {
            return;
        }
        try (PreparedStatement ps = conn.prepareStatement("UPDATE threadmill_concurrency_workflow_holds "
                + "SET outstanding = outstanding + 1 "
                + "WHERE concurrency_key = ? AND workflow_root_id = ?")) {
            ps.setString(1, snapshot.concurrencyKey());
            ps.setObject(2, snapshot.workflowRootId().asUuid());
            ps.executeUpdate();
        }
    }

    private void acquireWorkflowHold(Connection conn, JobSnapshot snapshot) throws SQLException {
        if (snapshot.concurrencyKey() == null || hasActiveWorkflowHoldForRoot(conn, snapshot)) {
            return;
        }
        int outstanding = countOutstandingWorkflowJobs(conn, snapshot);
        if (outstanding <= 0) {
            outstanding = 1;
        }
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO threadmill_concurrency_workflow_holds "
                + "(concurrency_key, workflow_root_id, outstanding) VALUES (?, ?, ?) "
                + "ON CONFLICT (concurrency_key, workflow_root_id) DO NOTHING")) {
            ps.setString(1, snapshot.concurrencyKey());
            ps.setObject(2, snapshot.workflowRootId().asUuid());
            ps.setInt(3, outstanding);
            ps.executeUpdate();
        }
        String column =
                snapshot.concurrencyMode() == ConcurrencyMode.EXCLUSIVE ? "exclusive_in_flight" : "shared_in_flight";
        try (PreparedStatement ps = conn.prepareStatement("UPDATE threadmill_concurrency_groups SET "
                + column + " = " + column + " + 1, last_modified = clock_timestamp() "
                + "WHERE concurrency_key = ?")) {
            ps.setString(1, snapshot.concurrencyKey());
            ps.executeUpdate();
        }
    }

    private int countOutstandingWorkflowJobs(Connection conn, JobSnapshot snapshot) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM threadmill_jobs "
                + "WHERE concurrency_key = ? AND workflow_root_id = ? "
                + "AND state NOT IN ('SUCCEEDED','FAILED','DELETED','QUARANTINED')")) {
            ps.setString(1, snapshot.concurrencyKey());
            ps.setObject(2, snapshot.workflowRootId().asUuid());
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    private void adjustWorkflowHoldOnTransition(Connection conn, JobSnapshot oldSnapshot, JobState newState)
            throws SQLException {
        if (oldSnapshot.concurrencyKey() == null) {
            return;
        }
        if (isTerminal(oldSnapshot.currentState()) && !isTerminal(newState)) {
            // Mirror branch for the terminal -> non-terminal resurrect — the
            // standard retry path (FAILED -> SCHEDULED). Without it the job
            // is decremented twice (once at FAILED, once at its eventual
            // terminal state) and an EXCLUSIVE key can be released while a
            // descendant still runs. Zero rows matched is fine: a standalone
            // job whose hold was already fully released is re-registered at
            // the next claim by acquireWorkflowHold.
            try (PreparedStatement ps = conn.prepareStatement("UPDATE threadmill_concurrency_workflow_holds "
                    + "SET outstanding = outstanding + 1 "
                    + "WHERE concurrency_key = ? AND workflow_root_id = ?")) {
                ps.setString(1, oldSnapshot.concurrencyKey());
                ps.setObject(2, oldSnapshot.workflowRootId().asUuid());
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
     * decrement the hold's outstanding count and, when it reaches zero,
     * delete the hold and free its in-flight slot. A missing hold row is a
     * no-op — never-claimed standalone jobs hold nothing.
     */
    private void releaseWorkflowHoldShare(Connection conn, String concurrencyKey, ConcurrencyMode mode, UUID root)
            throws SQLException {
        int outstanding;
        try (PreparedStatement ps = conn.prepareStatement("UPDATE threadmill_concurrency_workflow_holds "
                + "SET outstanding = outstanding - 1 "
                + "WHERE concurrency_key = ? AND workflow_root_id = ? "
                + "RETURNING outstanding")) {
            ps.setString(1, concurrencyKey);
            ps.setObject(2, root);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return;
                }
                outstanding = rs.getInt(1);
            }
        }
        if (outstanding > 0) {
            return;
        }
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM threadmill_concurrency_workflow_holds "
                + "WHERE concurrency_key = ? AND workflow_root_id = ?")) {
            ps.setString(1, concurrencyKey);
            ps.setObject(2, root);
            ps.executeUpdate();
        }
        String column = mode == ConcurrencyMode.EXCLUSIVE ? "exclusive_in_flight" : "shared_in_flight";
        try (PreparedStatement ps = conn.prepareStatement("UPDATE threadmill_concurrency_groups SET "
                + column + " = GREATEST(" + column + " - 1, 0), last_modified = clock_timestamp() "
                + "WHERE concurrency_key = ?")) {
            ps.setString(1, concurrencyKey);
            ps.executeUpdate();
        }
    }

    private void insertSnapshot(
            Connection conn, JobSnapshot snapshot, String body, Instant currentStateAt, long version)
            throws SQLException {
        try (PreparedStatement ps =
                conn.prepareStatement("INSERT INTO threadmill_jobs (id, state, queue, priority, handler_signature, "
                        + "scheduled_at, owner_node_id, owner_heartbeat_at, last_checkin_at, current_state_at, "
                        + "version, body, created_at, concurrency_key, concurrency_mode, workflow_root_id, "
                        + "parent_job_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            ps.setObject(1, snapshot.id().asUuid());
            ps.setString(2, snapshot.currentState().name());
            ps.setString(3, snapshot.queue());
            ps.setInt(4, snapshot.priority());
            ps.setString(5, snapshot.spec().handlerType());
            setNullableTimestamp(ps, 6, snapshot.scheduledFor());
            setNullableUuid(
                    ps,
                    7,
                    snapshot.ownerNodeId() == null
                            ? null
                            : snapshot.ownerNodeId().asUuid());
            setNullableTimestamp(ps, 8, snapshot.ownerHeartbeatAt());
            setNullableTimestamp(ps, 9, snapshot.lastCheckinAt());
            ps.setTimestamp(10, Timestamp.from(currentStateAt));
            ps.setLong(11, version);
            ps.setString(12, body);
            ps.setTimestamp(13, Timestamp.from(snapshot.createdAt()));
            setNullableConcurrency(ps, 14, snapshot.concurrencyKey(), snapshot.concurrencyMode());
            ps.setObject(16, snapshot.workflowRootId().asUuid());
            setNullableParentJobId(ps, 17, snapshot);
            ps.executeUpdate();
        }
    }

    private static void setNullableConcurrency(PreparedStatement ps, int startIndex, String key, ConcurrencyMode mode)
            throws SQLException {
        if (key == null) {
            ps.setNull(startIndex, Types.VARCHAR);
            ps.setNull(startIndex + 1, Types.VARCHAR);
        } else {
            ps.setString(startIndex, key);
            ps.setString(startIndex + 1, mode.name());
        }
    }

    private static void setNullableTimestamp(PreparedStatement ps, int index, Instant v) throws SQLException {
        if (v == null) ps.setNull(index, Types.TIMESTAMP_WITH_TIMEZONE);
        else ps.setTimestamp(index, Timestamp.from(v));
    }

    private static void setNullableUuid(PreparedStatement ps, int index, UUID v) throws SQLException {
        if (v == null) ps.setNull(index, Types.OTHER);
        else ps.setObject(index, v);
    }

    private static void setNullableParentJobId(PreparedStatement ps, int index, JobSnapshot snapshot)
            throws SQLException {
        if (snapshot.relationship() == null) {
            ps.setNull(index, Types.OTHER);
        } else {
            ps.setObject(index, snapshot.relationship().parentId().asUuid());
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
                s.attempts());
    }

    private static boolean isTerminal(JobState state) {
        return switch (state) {
            case SUCCEEDED, FAILED, DELETED, QUARANTINED -> true;
            case AWAITING, SCHEDULED, ENQUEUED, PROCESSING, PROCESSED -> false;
        };
    }

    /** Translates {@link SQLException}s from a {@link JobStore} method into an unchecked form. */
    public static class JdbcException extends RuntimeException {
        public JdbcException(String message, SQLException cause) {
            super(message, cause);
        }
    }
}
