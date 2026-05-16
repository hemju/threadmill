package com.hemju.threadmill.store.redis;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;

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
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.core.store.JobStoreCapabilities;
import com.hemju.threadmill.core.store.Mutexes;
import com.hemju.threadmill.core.store.NodeHeartbeat;

/**
 * Redis-backed {@link JobStore}.
 *
 * <h2>Design</h2>
 * <ul>
 *   <li>Per-job state lives in a HASH; auxiliary structures (per-queue
 *       ENQUEUED ZSET, global SCHEDULED / AWAITING / PROCESSING ZSETs, per-node
 *       PROCESSING ZSET, by-handler SET, by-state-time ZSETs, per-state counts
 *       HASH) are kept in sync inside the same atomic Lua call as the state
 *       change that affected them.</li>
 *   <li>The claim path prepares the new serialized body in Java first, then
 *       commits body, scalars, counts, and indexes together in one Lua script.
 *       A crash before the script leaves the job ENQUEUED; a crash after the
 *       script leaves a complete PROCESSING record for orphan recovery.</li>
 *   <li>Node heartbeats are TTL'd strings: key existence is the heartbeat.</li>
 * </ul>
 *
 * <h2>Durability</h2>
 * <p>Threadmill on Redis is durable to the level Redis is configured for.
 * For job-store use cases, run Redis with AOF persistence enabled
 * ({@code appendonly yes}, {@code appendfsync everysec} is a reasonable
 * default). Out of the box, Redis is less durable than PostgreSQL.
 *
 * <h2>Capability differences</h2>
 * <ul>
 *   <li>Per-state counts are exact (engine-maintained).</li>
 *   <li>{@code findByHandlerSignature} is supported via a per-handler set.</li>
 *   <li>Deep ad-hoc search across arbitrary metadata is NOT supported —
 *       use the Postgres backend if you need that. This is surfaced through
 *       {@link JobStoreCapabilities#supportsRichSearch()}.</li>
 * </ul>
 */
public final class RedisJobStore implements JobStore {

    private static final String DELETE_NODE_HEARTBEAT_IF_STALE = """
            local heartbeat = redis.call('GET', KEYS[1])
            if not heartbeat then
              return redis.call('SREM', KEYS[2], ARGV[2])
            end
            if heartbeat == ARGV[1] then
              redis.call('DEL', KEYS[1])
              return redis.call('SREM', KEYS[2], ARGV[2])
            end
            return 0
            """;

    private final AbstractRedisClient client;
    private final AutoCloseable connection;
    private final RedisClusterCommands<String, String> commands;
    private final JobSerializer serializer;
    private final JobStoreCapabilities capabilities;
    private final boolean ownsClient;

    public RedisJobStore(RedisURI uri) {
        this(connectStandalone(RedisClient.create(uri)), new JsonJobSerializer(), defaultCapabilities(), true);
    }

    public RedisJobStore(RedisStoreConfig config) {
        this(connect(config), new JsonJobSerializer(), defaultCapabilities(), true);
    }

    public RedisJobStore(RedisClient client, JobSerializer serializer, JobStoreCapabilities capabilities) {
        this(connectStandalone(Objects.requireNonNull(client, "client")), serializer, capabilities, false);
    }

    private record ConnectionHandle(
            AbstractRedisClient client, AutoCloseable connection, RedisClusterCommands<String, String> commands) {}

    private record ClaimLock(String key, String token) {}

    private static ConnectionHandle connect(RedisStoreConfig config) {
        Objects.requireNonNull(config, "config");
        return switch (config) {
            case RedisStoreConfig.Standalone standalone -> connectStandalone(RedisClient.create(standalone.uri()));
            case RedisStoreConfig.Sentinel sentinel -> connectSentinel(sentinel);
            case RedisStoreConfig.Cluster cluster -> connectCluster(cluster);
        };
    }

    private static ConnectionHandle connectStandalone(RedisClient client) {
        StatefulRedisConnection<String, String> connection = client.connect();
        return new ConnectionHandle(client, connection, connection.sync());
    }

    private static ConnectionHandle connectSentinel(RedisStoreConfig.Sentinel config) {
        var first = config.nodes().getFirst();
        var builder = RedisURI.Builder.sentinel(first.host(), first.port(), config.master());
        for (int i = 1; i < config.nodes().size(); i++) {
            var node = config.nodes().get(i);
            builder.withSentinel(node.host(), node.port());
        }
        if (config.password() != null && !config.password().isBlank()) {
            builder.withPassword(config.password().toCharArray());
        }
        return connectStandalone(RedisClient.create(builder.build()));
    }

    private static ConnectionHandle connectCluster(RedisStoreConfig.Cluster config) {
        var uris = config.nodes().stream()
                .map(node -> RedisURI.Builder.redis(node.host(), node.port()).build())
                .toList();
        RedisClusterClient client = RedisClusterClient.create(uris);
        StatefulRedisClusterConnection<String, String> connection = client.connect();
        return new ConnectionHandle(client, connection, connection.sync());
    }

    private RedisJobStore(
            ConnectionHandle handle, JobSerializer serializer, JobStoreCapabilities capabilities, boolean ownsClient) {
        this.client = handle.client();
        this.connection = handle.connection();
        this.commands = handle.commands();
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.capabilities = Objects.requireNonNull(capabilities, "capabilities");
        this.ownsClient = ownsClient;
    }

    /** Closes the underlying connection (and the client, if this instance owns it). */
    public void close() {
        try {
            connection.close();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to close Redis connection", e);
        }
        if (ownsClient) client.shutdown();
    }

    @Override
    public JobStoreCapabilities capabilities() {
        return capabilities;
    }

    // ---------------------------------------------------------------- insert

    @Override
    public void insert(Job job) {
        Objects.requireNonNull(job, "job");
        Names.requireName("queue", job.queue());
        long version = 1L;
        RedisClusterCommands<String, String> r = sync();
        JobSnapshot snapshot = snapshotForInsert(r, job, version);
        List<ClaimLock> locks = List.of();
        while (true) {
            locks = acquireClaimLocks(r, concurrencyClaimLockKeys(List.of(snapshot)));
            JobSnapshot lockedSnapshot = snapshotForInsert(r, job, version);
            if (concurrencyClaimLockKeys(List.of(lockedSnapshot)).equals(claimLockKeys(locks))) {
                snapshot = lockedSnapshot;
                break;
            }
            releaseClaimLocks(r, locks);
            locks = List.of();
            snapshot = lockedSnapshot;
        }
        try {
            insertSnapshot(r, snapshot);
        } finally {
            releaseClaimLocks(r, locks);
        }
        job.adoptVersion(version);
    }

    private void insertSnapshot(RedisClusterCommands<String, String> r, JobSnapshot snapshot) {
        String body = serializer.serializeJob(snapshot, capabilities);
        Instant stateAt = lastTransitionTime(snapshot, snapshot.currentState());

        String activeKey = activeKeyFor(snapshot.currentState(), snapshot.queue(), snapshot.ownerNodeId());
        double activeScore = activeScoreFor(snapshot, stateAt);
        var keys = new String[] {
            RedisKeys.job(snapshot.id()),
            activeKey == null ? "" : activeKey,
            RedisKeys.byStateTime(snapshot.currentState()),
            RedisKeys.byHandler(snapshot.spec().handlerType()),
            RedisKeys.COUNTS,
            concurrencyPendingKey(snapshot),
            concurrencyWorkflowsKey(snapshot.concurrencyKey())
        };
        var args = new String[] {
            snapshot.id().toString(),
            body,
            snapshot.currentState().name(),
            snapshot.queue(),
            snapshot.spec().handlerType(),
            Integer.toString(snapshot.priority()),
            snapshot.scheduledFor() == null
                    ? ""
                    : Long.toString(snapshot.scheduledFor().toEpochMilli()),
            snapshot.ownerNodeId() == null ? "" : snapshot.ownerNodeId().toString(),
            snapshot.ownerHeartbeatAt() == null
                    ? ""
                    : Long.toString(snapshot.ownerHeartbeatAt().toEpochMilli()),
            snapshot.lastCheckinAt() == null
                    ? ""
                    : Long.toString(snapshot.lastCheckinAt().toEpochMilli()),
            Long.toString(stateAt.toEpochMilli()),
            Long.toString(snapshot.createdAt().toEpochMilli()),
            Double.toString(activeScore),
            snapshot.workflowRootId().toString(),
            snapshot.concurrencyKey() == null ? "" : snapshot.concurrencyKey(),
            snapshot.concurrencyMode() == null ? "" : snapshot.concurrencyMode().name(),
            concurrencyPendingMember(snapshot),
            Long.toString(toEpochMicros(stateAt))
        };

        String result = r.eval(LuaScripts.insert(), ScriptOutputType.VALUE, keys, args);
        if ("EXISTS".equals(result)) {
            throw new IllegalStateException("Job already exists: " + snapshot.id());
        }
        if (snapshot.currentState() == JobState.ENQUEUED) {
            r.sadd(RedisKeys.QUEUES, snapshot.queue());
        }
    }

    @Override
    public List<JobId> insertAll(List<Job> jobsToInsert) {
        Objects.requireNonNull(jobsToInsert, "jobs");
        if (jobsToInsert.isEmpty()) return List.of();

        long version = 1L;
        RedisClusterCommands<String, String> r = sync();

        // Pre-serialize everything so OversizedJobException rejects the whole
        // batch before any Redis write.
        var snapshots = new ArrayList<JobSnapshot>(jobsToInsert.size());
        var bodies = new ArrayList<String>(jobsToInsert.size());
        var stateAts = new ArrayList<Instant>(jobsToInsert.size());
        for (var j : jobsToInsert) {
            Objects.requireNonNull(j, "job");
            Names.requireName("queue", j.queue());
            JobSnapshot snap = snapshotForInsert(r, j, version);
            String body = serializer.serializeJob(snap, capabilities);
            snapshots.add(snap);
            bodies.add(body);
            stateAts.add(lastTransitionTime(snap, snap.currentState()));
        }

        List<ClaimLock> locks = List.of();
        while (true) {
            locks = acquireClaimLocks(r, concurrencyClaimLockKeys(snapshots));
            var lockedSnapshots = new ArrayList<JobSnapshot>(jobsToInsert.size());
            var lockedBodies = new ArrayList<String>(jobsToInsert.size());
            var lockedStateAts = new ArrayList<Instant>(jobsToInsert.size());
            for (var j : jobsToInsert) {
                JobSnapshot snap = snapshotForInsert(r, j, version);
                lockedSnapshots.add(snap);
                lockedBodies.add(serializer.serializeJob(snap, capabilities));
                lockedStateAts.add(lastTransitionTime(snap, snap.currentState()));
            }
            if (concurrencyClaimLockKeys(lockedSnapshots).equals(claimLockKeys(locks))) {
                snapshots = lockedSnapshots;
                bodies = lockedBodies;
                stateAts = lockedStateAts;
                break;
            }
            releaseClaimLocks(r, locks);
            locks = List.of();
            snapshots = lockedSnapshots;
            bodies = lockedBodies;
            stateAts = lockedStateAts;
        }

        try {
            // Pack 7 keys + 18 args per job for the batched Lua script.
            int n = snapshots.size();
            var keyList = new ArrayList<String>(n * 7);
            var argList = new ArrayList<String>(1 + n * 18);
            argList.add(Integer.toString(n));
            for (int i = 0; i < n; i++) {
                JobSnapshot snap = snapshots.get(i);
                Instant stateAt = stateAts.get(i);
                String activeKey = activeKeyFor(snap.currentState(), snap.queue(), snap.ownerNodeId());
                double activeScore = activeScoreFor(snap, stateAt);
                keyList.add(RedisKeys.job(snap.id()));
                keyList.add(activeKey == null ? "" : activeKey);
                keyList.add(RedisKeys.byStateTime(snap.currentState()));
                keyList.add(RedisKeys.byHandler(snap.spec().handlerType()));
                keyList.add(RedisKeys.COUNTS);
                keyList.add(concurrencyPendingKey(snap));
                keyList.add(concurrencyWorkflowsKey(snap.concurrencyKey()));

                argList.add(snap.id().toString());
                argList.add(bodies.get(i));
                argList.add(snap.currentState().name());
                argList.add(snap.queue());
                argList.add(snap.spec().handlerType());
                argList.add(Integer.toString(snap.priority()));
                argList.add(
                        snap.scheduledFor() == null
                                ? ""
                                : Long.toString(snap.scheduledFor().toEpochMilli()));
                argList.add(snap.ownerNodeId() == null ? "" : snap.ownerNodeId().toString());
                argList.add(
                        snap.ownerHeartbeatAt() == null
                                ? ""
                                : Long.toString(snap.ownerHeartbeatAt().toEpochMilli()));
                argList.add(
                        snap.lastCheckinAt() == null
                                ? ""
                                : Long.toString(snap.lastCheckinAt().toEpochMilli()));
                argList.add(Long.toString(stateAt.toEpochMilli()));
                argList.add(Long.toString(snap.createdAt().toEpochMilli()));
                argList.add(Double.toString(activeScore));
                argList.add(snap.workflowRootId().toString());
                argList.add(snap.concurrencyKey() == null ? "" : snap.concurrencyKey());
                argList.add(
                        snap.concurrencyMode() == null
                                ? ""
                                : snap.concurrencyMode().name());
                argList.add(concurrencyPendingMember(snap));
                argList.add(Long.toString(toEpochMicros(stateAt)));
            }
            String result = r.eval(
                    LuaScripts.insertAll(),
                    ScriptOutputType.VALUE,
                    keyList.toArray(new String[0]),
                    argList.toArray(new String[0]));
            if (result != null && result.startsWith("EXISTS:")) {
                int idx = Integer.parseInt(result.substring("EXISTS:".length()));
                throw new IllegalStateException("Duplicate job id in batch at index " + idx + ": "
                        + snapshots.get(idx).id());
            }

            // Add queue names to the QUEUES set for any ENQUEUED jobs.
            for (JobSnapshot snap : snapshots) {
                if (snap.currentState() == JobState.ENQUEUED) {
                    r.sadd(RedisKeys.QUEUES, snap.queue());
                }
            }
        } finally {
            releaseClaimLocks(r, locks);
        }
        var ids = new ArrayList<JobId>(jobsToInsert.size());
        for (var j : jobsToInsert) {
            j.adoptVersion(version);
            ids.add(j.id());
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
        RedisClusterCommands<String, String> r = sync();
        JobSnapshot snapshot = snapshotForInsert(r, job, version);
        List<ClaimLock> locks = List.of();
        while (true) {
            locks = acquireClaimLocks(r, concurrencyClaimLockKeys(List.of(snapshot)));
            JobSnapshot lockedSnapshot = snapshotForInsert(r, job, version);
            if (concurrencyClaimLockKeys(List.of(lockedSnapshot)).equals(claimLockKeys(locks))) {
                snapshot = lockedSnapshot;
                break;
            }
            releaseClaimLocks(r, locks);
            locks = List.of();
            snapshot = lockedSnapshot;
        }
        try {
            return enqueueIfAbsentSnapshot(r, job, dedupKey, ttl, now, version, snapshot);
        } finally {
            releaseClaimLocks(r, locks);
        }
    }

    private EnqueueResult enqueueIfAbsentSnapshot(
            RedisClusterCommands<String, String> r,
            Job job,
            String dedupKey,
            Duration ttl,
            Instant now,
            long version,
            JobSnapshot snapshot) {
        String body = serializer.serializeJob(snapshot, capabilities);
        Instant stateAt = lastTransitionTime(snapshot, snapshot.currentState());
        String activeKey = activeKeyFor(snapshot.currentState(), snapshot.queue(), snapshot.ownerNodeId());
        double activeScore = activeScoreFor(snapshot, stateAt);
        String reply = r.eval(
                LuaScripts.enqueueIfAbsent(),
                ScriptOutputType.VALUE,
                new String[] {
                    RedisKeys.dedup(snapshot.queue(), dedupKey),
                    RedisKeys.job(snapshot.id()),
                    activeKey == null ? "" : activeKey,
                    RedisKeys.byStateTime(snapshot.currentState()),
                    RedisKeys.byHandler(snapshot.spec().handlerType()),
                    RedisKeys.COUNTS,
                    RedisKeys.dedupExpiry(),
                    concurrencyPendingKey(snapshot),
                    concurrencyWorkflowsKey(snapshot.concurrencyKey())
                },
                snapshot.id().toString(),
                body,
                snapshot.currentState().name(),
                snapshot.queue(),
                snapshot.spec().handlerType(),
                Integer.toString(snapshot.priority()),
                snapshot.scheduledFor() == null
                        ? ""
                        : Long.toString(snapshot.scheduledFor().toEpochMilli()),
                snapshot.ownerNodeId() == null ? "" : snapshot.ownerNodeId().toString(),
                snapshot.ownerHeartbeatAt() == null
                        ? ""
                        : Long.toString(snapshot.ownerHeartbeatAt().toEpochMilli()),
                snapshot.lastCheckinAt() == null
                        ? ""
                        : Long.toString(snapshot.lastCheckinAt().toEpochMilli()),
                Long.toString(stateAt.toEpochMilli()),
                Long.toString(snapshot.createdAt().toEpochMilli()),
                Double.toString(activeScore),
                Long.toString(now.plus(ttl).toEpochMilli()),
                Long.toString(now.toEpochMilli()),
                RedisKeys.PREFIX + "job:",
                snapshot.workflowRootId().toString(),
                snapshot.concurrencyKey() == null ? "" : snapshot.concurrencyKey(),
                snapshot.concurrencyMode() == null
                        ? ""
                        : snapshot.concurrencyMode().name(),
                concurrencyPendingMember(snapshot),
                Long.toString(toEpochMicros(stateAt)));
        if (reply != null && reply.startsWith("COALESCED:")) {
            return new EnqueueResult.Coalesced(JobId.parse(reply.substring("COALESCED:".length())));
        }
        if ("EXISTS".equals(reply)) {
            throw new IllegalStateException("Job already exists: " + job.id());
        }
        if (snapshot.currentState() == JobState.ENQUEUED) {
            r.sadd(RedisKeys.QUEUES, snapshot.queue());
        }
        job.adoptVersion(version);
        return new EnqueueResult.Created(job.id());
    }

    // ---------------------------------------------------------------- findById

    @Override
    public Optional<Job> findById(JobId id) {
        Objects.requireNonNull(id, "id");
        String body = sync().hget(RedisKeys.job(id), "body");
        return Optional.ofNullable(body).map(serializer::deserializeJob);
    }

    // ---------------------------------------------------------------- saveAtomic

    @Override
    public void saveAtomic(Job job, long expectedVersion) {
        Objects.requireNonNull(job, "job");
        long nextVersion = expectedVersion + 1;
        JobSnapshot snapshot = withVersion(job, nextVersion);
        // Serialize BEFORE any Redis call so OversizedJobException can't corrupt state.
        String body = serializer.serializeJob(snapshot, capabilities);

        // Find the prior persisted state from the hash; needed to compute the old structures to remove.
        RedisClusterCommands<String, String> r = sync();
        Map<String, String> hash = r.hgetall(RedisKeys.job(job.id()));
        if (hash == null || hash.isEmpty()) {
            throw new StaleJobException(job.id(), expectedVersion);
        }
        var oldState = JobState.valueOf(hash.get("state"));
        String oldQueue = hash.get("queue");
        String oldOwnerNode = hash.get("owner_node_id");
        NodeId oldOwnerNodeId = (oldOwnerNode == null || oldOwnerNode.isEmpty()) ? null : NodeId.parse(oldOwnerNode);
        String oldConcurrencyKey = blankToNull(hash.get("concurrency_key"));
        String oldConcurrencyMode = blankToNull(hash.get("concurrency_mode"));
        String oldWorkflowRoot = hash.get("workflow_root_id");

        String newActive = activeKeyFor(snapshot.currentState(), snapshot.queue(), snapshot.ownerNodeId());
        String newPerNode = (snapshot.currentState() == JobState.PROCESSING && snapshot.ownerNodeId() != null)
                ? RedisKeys.processingFor(snapshot.ownerNodeId())
                : "";
        String oldActive = activeKeyFor(oldState, oldQueue, oldOwnerNodeId);
        String oldPerNode = (oldState == JobState.PROCESSING && oldOwnerNodeId != null)
                ? RedisKeys.processingFor(oldOwnerNodeId)
                : "";

        Instant stateAt = lastTransitionTime(snapshot, snapshot.currentState());
        double newScore = activeScoreFor(snapshot, stateAt);

        var keys = new String[] {
            RedisKeys.job(snapshot.id()),
            newActive == null ? "" : newActive,
            newPerNode,
            RedisKeys.byStateTime(snapshot.currentState()),
            oldActive == null ? "" : oldActive,
            oldPerNode,
            RedisKeys.byStateTime(oldState),
            RedisKeys.COUNTS,
            concurrencyPendingKey(oldConcurrencyKey),
            concurrencyPendingKey(snapshot),
            concurrencyCountersKey(oldConcurrencyKey),
            concurrencyWorkflowsKey(oldConcurrencyKey)
        };
        var args = new String[] {
            snapshot.id().toString(),
            Long.toString(expectedVersion),
            Long.toString(nextVersion),
            body,
            snapshot.currentState().name(),
            snapshot.queue(),
            Integer.toString(snapshot.priority()),
            snapshot.scheduledFor() == null
                    ? ""
                    : Long.toString(snapshot.scheduledFor().toEpochMilli()),
            snapshot.ownerNodeId() == null ? "" : snapshot.ownerNodeId().toString(),
            snapshot.ownerHeartbeatAt() == null
                    ? ""
                    : Long.toString(snapshot.ownerHeartbeatAt().toEpochMilli()),
            snapshot.lastCheckinAt() == null
                    ? ""
                    : Long.toString(snapshot.lastCheckinAt().toEpochMilli()),
            Long.toString(stateAt.toEpochMilli()),
            Double.toString(newScore),
            oldState.name(),
            snapshot.workflowRootId().toString(),
            snapshot.concurrencyKey() == null ? "" : snapshot.concurrencyKey(),
            snapshot.concurrencyMode() == null ? "" : snapshot.concurrencyMode().name(),
            oldConcurrencyKey == null ? "" : oldConcurrencyKey,
            oldConcurrencyMode == null ? "" : oldConcurrencyMode,
            oldWorkflowRoot == null ? "" : oldWorkflowRoot,
            concurrencyPendingMember(oldConcurrencyMode, snapshot.id()),
            concurrencyPendingMember(snapshot),
            Long.toString(toEpochMicros(stateAt))
        };

        String result = r.eval(LuaScripts.saveAtomic(), ScriptOutputType.VALUE, keys, args);
        if ("VANISHED".equals(result) || "STALE".equals(result)) {
            throw new StaleJobException(job.id(), expectedVersion);
        }
        if (snapshot.currentState() == JobState.ENQUEUED) {
            r.sadd(RedisKeys.QUEUES, snapshot.queue());
        }
        job.adoptVersion(nextVersion);
    }

    // ---------------------------------------------------------------- softDelete

    @Override
    public boolean softDelete(JobId id) {
        RedisClusterCommands<String, String> r = sync();
        Map<String, String> hash = r.hgetall(RedisKeys.job(id));
        if (hash == null || hash.isEmpty()) return false;

        String body = hash.get("body");
        Job j = serializer.deserializeJob(body);
        if (j.currentState() == JobState.DELETED) return false;
        var now = Instant.now();
        j.transitionTo(JobState.DELETED, now, "user.delete", null);
        long nextVersion = j.version() + 1;
        JobSnapshot snapshot = withVersion(j, nextVersion);
        String newBody = serializer.serializeJob(snapshot, capabilities);

        var oldState = JobState.valueOf(hash.get("state"));
        String oldQueue = hash.get("queue");
        String oldOwnerNode = hash.get("owner_node_id");
        NodeId oldOwnerNodeId = (oldOwnerNode == null || oldOwnerNode.isEmpty()) ? null : NodeId.parse(oldOwnerNode);
        String oldConcurrencyKey = blankToNull(hash.get("concurrency_key"));
        String oldConcurrencyMode = blankToNull(hash.get("concurrency_mode"));
        String oldWorkflowRoot = hash.get("workflow_root_id");
        String oldActive = activeKeyFor(oldState, oldQueue, oldOwnerNodeId);
        String oldPerNode = (oldState == JobState.PROCESSING && oldOwnerNodeId != null)
                ? RedisKeys.processingFor(oldOwnerNodeId)
                : "";

        var keys = new String[] {
            RedisKeys.job(id),
            oldActive == null ? "" : oldActive,
            oldPerNode,
            RedisKeys.byStateTime(oldState),
            RedisKeys.byStateTime(JobState.DELETED),
            RedisKeys.COUNTS,
            concurrencyPendingKey(oldConcurrencyKey),
            concurrencyCountersKey(oldConcurrencyKey),
            concurrencyWorkflowsKey(oldConcurrencyKey)
        };
        var args = new String[] {
            id.toString(),
            newBody,
            Long.toString(now.toEpochMilli()),
            oldState.name(),
            oldConcurrencyKey == null ? "" : oldConcurrencyKey,
            oldConcurrencyMode == null ? "" : oldConcurrencyMode,
            oldWorkflowRoot == null ? "" : oldWorkflowRoot,
            concurrencyPendingMember(oldConcurrencyMode, id)
        };
        Long result = r.eval(LuaScripts.softDelete(), ScriptOutputType.INTEGER, keys, args);
        return result != null && result == 1L;
    }

    // ---------------------------------------------------------------- claim

    @Override
    public List<Job> claimReady(NodeId nodeId, String queue, int max, Instant heartbeatAt) {
        Objects.requireNonNull(nodeId, "nodeId");
        Names.requireName("queue", queue);
        Objects.requireNonNull(heartbeatAt, "heartbeatAt");
        if (max <= 0) return List.of();
        if (Boolean.TRUE.equals(sync().hexists(RedisKeys.QUEUE_PAUSES, queue))) {
            return List.of();
        }
        int cap = Math.min(max, capabilities.maxClaimBatch());

        RedisClusterCommands<String, String> r = sync();
        List<Job> result = new ArrayList<>(cap);
        long pageSize = Math.max(cap * 128L, cap);
        for (int attempt = 0; attempt < 20; attempt++) {
            boolean blocked = false;
            long offset = 0L;
            while (result.size() < cap) {
                List<String> candidateIds = r.zrange(RedisKeys.queue(queue), offset, offset + pageSize - 1L);
                if (candidateIds == null || candidateIds.isEmpty()) break;

                for (String idStr : candidateIds) {
                    if (result.size() >= cap) break;
                    var id = JobId.parse(idStr);
                    String jobKey = RedisKeys.job(id);
                    Map<String, String> hash = r.hgetall(jobKey);
                    if (hash == null || hash.isEmpty()) continue;
                    if (!"ENQUEUED".equals(hash.get("state"))) continue;
                    String oldBody = hash.get("body");
                    String oldVersion = hash.get("version");
                    if (oldBody == null || oldVersion == null) continue;
                    long newVersion = Long.parseLong(oldVersion) + 1L;
                    Job j = serializer.deserializeJob(oldBody);
                    String lockKey = j.concurrencyKey()
                            .map(RedisKeys::concurrencyClaimLock)
                            .orElse(null);
                    String lockToken =
                            lockKey == null ? null : UUID.randomUUID().toString();
                    if (lockKey != null && !tryClaimLock(r, lockKey, lockToken)) {
                        blocked = true;
                        continue;
                    }
                    try {
                        j.transitionTo(JobState.PROCESSING, heartbeatAt, "engine.claim", null);
                        j.assignOwner(nodeId, heartbeatAt);
                        j.incrementAttempts();
                        JobSnapshot snap = withVersion(j, newVersion);
                        String newBody = serializer.serializeJob(snap, capabilities);
                        String concurrencyKey = snap.concurrencyKey();
                        String reply = r.eval(
                                LuaScripts.claimCommit(),
                                ScriptOutputType.VALUE,
                                new String[] {
                                    jobKey,
                                    RedisKeys.queue(queue),
                                    RedisKeys.PROCESSING_ALL,
                                    RedisKeys.processingFor(nodeId),
                                    RedisKeys.byStateTime(JobState.ENQUEUED),
                                    RedisKeys.byStateTime(JobState.PROCESSING),
                                    RedisKeys.COUNTS,
                                    concurrencyCountersKey(concurrencyKey),
                                    concurrencyPendingKey(concurrencyKey),
                                    concurrencyWorkflowsKey(concurrencyKey)
                                },
                                idStr,
                                oldVersion,
                                Long.toString(newVersion),
                                newBody,
                                nodeId.toString(),
                                Long.toString(heartbeatAt.toEpochMilli()),
                                Integer.toString(snap.attempts()),
                                concurrencyKey == null ? "" : concurrencyKey,
                                snap.concurrencyMode() == null
                                        ? ""
                                        : snap.concurrencyMode().name(),
                                snap.workflowRootId().toString(),
                                concurrencyPendingMember(snap),
                                Integer.toString(workflowOutstandingCount(r, snap)));
                        if ("OK".equals(reply)) {
                            result.add(serializer.deserializeJob(newBody));
                        } else if ("BLOCKED".equals(reply)) {
                            blocked = true;
                        }
                    } finally {
                        if (lockKey != null) {
                            releaseClaimLock(r, lockKey, lockToken);
                        }
                    }
                }
                if (candidateIds.size() < pageSize) {
                    break;
                }
                offset += pageSize;
            }
            if (!result.isEmpty() || !blocked) {
                return result;
            }
            java.util.concurrent.locks.LockSupport.parkNanos(
                    Duration.ofMillis(2).toNanos());
        }
        return result;
    }

    // ---------------------------------------------------------------- queue pauses

    @Override
    public void pauseQueue(String queue, String reason) {
        Names.requireName("queue", queue);
        sync().hset(RedisKeys.QUEUE_PAUSES, queue, reason == null ? "" : reason);
    }

    @Override
    public void resumeQueue(String queue) {
        Names.requireName("queue", queue);
        sync().hdel(RedisKeys.QUEUE_PAUSES, queue);
    }

    @Override
    public Set<String> listPausedQueues() {
        var keys = sync().hkeys(RedisKeys.QUEUE_PAUSES);
        return keys == null ? Set.of() : Set.copyOf(keys);
    }

    @Override
    public void touchOwnerHeartbeat(NodeId nodeId, Instant now) {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(now, "now");
        RedisClusterCommands<String, String> r = sync();
        r.eval(
                LuaScripts.touchHeartbeat(),
                ScriptOutputType.INTEGER,
                new String[] {RedisKeys.processingFor(nodeId), RedisKeys.PROCESSING_ALL},
                Long.toString(now.toEpochMilli()),
                RedisKeys.PREFIX + "job:");
    }

    @Override
    public boolean saveExecutionUpdate(Job job, NodeId nodeId) {
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(nodeId, "nodeId");
        JobSnapshot snapshot = withVersion(job, job.version());
        String body = serializer.serializeJob(snapshot, capabilities);
        Instant heartbeat = snapshot.lastCheckinAt() == null ? snapshot.ownerHeartbeatAt() : snapshot.lastCheckinAt();
        Long result = sync().eval(
                        """
                if redis.call('HGET', KEYS[1], 'state') ~= 'PROCESSING' then return 0 end
                if redis.call('HGET', KEYS[1], 'owner_node_id') ~= ARGV[1] then return 0 end
                redis.call('HSET', KEYS[1],
                  'body', ARGV[2],
                  'owner_heartbeat_at', ARGV[3],
                  'last_checkin_at', ARGV[4])
                redis.call('ZADD', KEYS[2], ARGV[3], ARGV[5])
                redis.call('ZADD', KEYS[3], ARGV[3], ARGV[5])
                return 1
                """,
                        ScriptOutputType.INTEGER,
                        new String[] {
                            RedisKeys.job(snapshot.id()), RedisKeys.PROCESSING_ALL, RedisKeys.processingFor(nodeId)
                        },
                        nodeId.toString(),
                        body,
                        heartbeat == null ? "" : Long.toString(heartbeat.toEpochMilli()),
                        snapshot.lastCheckinAt() == null
                                ? ""
                                : Long.toString(snapshot.lastCheckinAt().toEpochMilli()),
                        snapshot.id().toString());
        return result != null && result == 1L;
    }

    @Override
    public void recordNodeHeartbeat(NodeId nodeId, Instant now) {
        RedisClusterCommands<String, String> r = sync();
        r.set(RedisKeys.nodeHeartbeat(nodeId), Long.toString(now.toEpochMilli()), SetArgs.Builder.ex(60));
        r.sadd(RedisKeys.NODES, nodeId.toString());
    }

    @Override
    public Optional<Instant> readNodeHeartbeat(NodeId nodeId) {
        String value = sync().get(RedisKeys.nodeHeartbeat(nodeId));
        return value == null ? Optional.empty() : Optional.of(Instant.ofEpochMilli(Long.parseLong(value)));
    }

    @Override
    public boolean acquireOrRenewMaintenanceLease(NodeId nodeId, Duration leaseDuration) {
        Objects.requireNonNull(nodeId, "nodeId");
        Mutexes.requirePositive(leaseDuration);
        String reply = sync().eval(
                        LuaScripts.leaseAcquire(),
                        ScriptOutputType.VALUE,
                        new String[] {RedisKeys.MAINTENANCE_LEASE},
                        nodeId.toString(),
                        Long.toString(leaseDuration.toMillis()));
        return "OK".equals(reply);
    }

    @Override
    public void releaseMaintenanceLease(NodeId nodeId) {
        Objects.requireNonNull(nodeId, "nodeId");
        sync().eval(
                        LuaScripts.leaseRelease(),
                        ScriptOutputType.VALUE,
                        new String[] {RedisKeys.MAINTENANCE_LEASE},
                        nodeId.toString());
    }

    @Override
    public Optional<NodeId> readMaintenanceLeaseOwner() {
        String holder = sync().get(RedisKeys.MAINTENANCE_LEASE);
        return holder == null ? Optional.empty() : Optional.of(NodeId.parse(holder));
    }

    // ---------------------------------------------------------------- housekeeping

    @Override
    public List<Job> findDueForPromotion(Instant now, int max) {
        Objects.requireNonNull(now, "now");
        if (max <= 0) return List.of();
        List<String> ids = sync().zrangebyscore(
                        RedisKeys.SCHEDULED,
                        io.lettuce.core.Range.create(Double.NEGATIVE_INFINITY, (double) now.toEpochMilli()),
                        io.lettuce.core.Limit.create(0, max));
        return loadJobs(ids);
    }

    @Override
    public List<Job> findOrphaned(Instant heartbeatExpiry, int max) {
        Objects.requireNonNull(heartbeatExpiry, "heartbeatExpiry");
        if (max <= 0) return List.of();
        List<String> ids = sync().zrangebyscore(
                        RedisKeys.PROCESSING_ALL,
                        io.lettuce.core.Range.create(Double.NEGATIVE_INFINITY, (double) heartbeatExpiry.toEpochMilli()),
                        io.lettuce.core.Limit.create(0, max));
        return loadJobs(ids);
    }

    // ---------------------------------------------------------------- counts & search

    @Override
    public Map<JobState, Long> countsByState() {
        Map<String, String> all = sync().hgetall(RedisKeys.COUNTS);
        var out = new EnumMap<JobState, Long>(JobState.class);
        for (JobState s : JobState.values()) out.put(s, 0L);
        if (all == null) return out;
        for (var e : all.entrySet()) {
            try {
                out.put(JobState.valueOf(e.getKey()), Long.parseLong(e.getValue()));
            } catch (IllegalArgumentException ignored) {
                // unknown legacy state — skip
            }
        }
        return out;
    }

    @Override
    public Map<String, Long> queueDepths() {
        Map<String, Long> depths = new HashMap<>();
        RedisClusterCommands<String, String> r = sync();
        for (String queue : r.smembers(RedisKeys.QUEUES)) {
            long depth = r.zcard(RedisKeys.queue(queue));
            if (depth > 0) {
                depths.put(queue, depth);
            }
        }
        return depths;
    }

    @Override
    public List<String> listEnqueuedQueues() {
        return queueDepths().keySet().stream().sorted().toList();
    }

    @Override
    public Optional<Instant> oldestEnqueuedAt(String queue) {
        Names.requireName("queue", queue);
        List<String> ids = sync().zrange(RedisKeys.queue(queue), 0, 0);
        if (ids == null || ids.isEmpty()) return Optional.empty();
        String value = sync().hget(RedisKeys.job(JobId.parse(ids.get(0))), "current_state_at");
        if (value == null || value.isEmpty()) return Optional.empty();
        return Optional.of(Instant.ofEpochMilli(Long.parseLong(value)));
    }

    @Override
    public Optional<Instant> oldestProcessingHeartbeat() {
        List<String> ids = sync().zrange(RedisKeys.PROCESSING_ALL, 0, 0);
        if (ids == null || ids.isEmpty()) return Optional.empty();
        Double score = sync().zscore(RedisKeys.PROCESSING_ALL, ids.get(0));
        return score == null ? Optional.empty() : Optional.of(Instant.ofEpochMilli(score.longValue()));
    }

    @Override
    public List<NodeHeartbeat> listNodeHeartbeats() {
        List<NodeHeartbeat> out = new ArrayList<>();
        RedisClusterCommands<String, String> r = sync();
        for (String node : r.smembers(RedisKeys.NODES)) {
            var nodeId = NodeId.parse(node);
            String value = r.get(RedisKeys.nodeHeartbeat(nodeId));
            if (value != null) {
                out.add(new NodeHeartbeat(nodeId, Instant.ofEpochMilli(Long.parseLong(value))));
            }
        }
        return out;
    }

    @Override
    public long deleteNodeHeartbeatsOlderThan(Instant cutoff) {
        Objects.requireNonNull(cutoff, "cutoff");
        RedisClusterCommands<String, String> r = sync();
        long removed = 0L;
        for (String node : r.smembers(RedisKeys.NODES)) {
            var nodeId = NodeId.parse(node);
            String key = RedisKeys.nodeHeartbeat(nodeId);
            String value = r.get(key);
            if (value == null || !Instant.ofEpochMilli(Long.parseLong(value)).isAfter(cutoff)) {
                Long deleted = r.eval(
                        DELETE_NODE_HEARTBEAT_IF_STALE,
                        ScriptOutputType.INTEGER,
                        new String[] {key, RedisKeys.NODES},
                        value == null ? "" : value,
                        node);
                removed += deleted == null ? 0L : deleted;
            }
        }
        return removed;
    }

    @Override
    public long deleteExpiredDedupKeys(Instant now, int max) {
        Objects.requireNonNull(now, "now");
        if (max <= 0) return 0L;
        RedisClusterCommands<String, String> r = sync();
        List<String> keys = r.zrangebyscore(
                RedisKeys.dedupExpiry(),
                io.lettuce.core.Range.create(Double.NEGATIVE_INFINITY, (double) now.toEpochMilli()),
                io.lettuce.core.Limit.create(0, max));
        long removed = 0L;
        for (String key : keys) {
            String jobId = r.hget(key, "job_id");
            boolean terminal = true;
            if (jobId != null) {
                String state = r.hget(RedisKeys.job(JobId.parse(jobId)), "state");
                terminal = state == null || isTerminal(JobState.valueOf(state));
            }
            if (terminal) {
                r.del(key);
                r.zrem(RedisKeys.dedupExpiry(), key);
                removed++;
            }
        }
        return removed;
    }

    @Override
    public List<Job> findByHandlerSignature(String handlerType, int max) {
        Objects.requireNonNull(handlerType, "handlerType");
        if (max <= 0) return List.of();
        List<String> ids = new ArrayList<>(sync().smembers(RedisKeys.byHandler(handlerType)));
        if (ids.size() > max) ids = ids.subList(0, max);
        return loadJobs(ids);
    }

    // ---------------------------------------------------------------- retention

    @Override
    public long deleteFinishedOlderThan(Instant cutoff, JobState state, int max) {
        Objects.requireNonNull(cutoff, "cutoff");
        Objects.requireNonNull(state, "state");
        if (max <= 0) return 0L;
        RedisClusterCommands<String, String> r = sync();
        List<String> ids = r.zrangebyscore(
                RedisKeys.byStateTime(state),
                io.lettuce.core.Range.create(Double.NEGATIVE_INFINITY, (double) cutoff.toEpochMilli()),
                io.lettuce.core.Limit.create(0, max));
        if (ids.isEmpty()) return 0L;
        long removed = 0;
        for (String idStr : ids) {
            // Best-effort hard delete: remove job hash + index entries.
            String jobKey = RedisKeys.PREFIX + "job:" + idStr;
            String handler = r.hget(jobKey, "handler_signature");
            r.del(jobKey);
            r.zrem(RedisKeys.byStateTime(state), idStr);
            if (handler != null) r.srem(RedisKeys.byHandler(handler), idStr);
            r.hincrby(RedisKeys.COUNTS, state.name(), -1);
            removed++;
        }
        return removed;
    }

    // ---------------------------------------------------------------- relationships & mutexes

    @Override
    public List<Job> findAwaitingByParent(JobId parentId, int max) {
        Objects.requireNonNull(parentId, "parentId");
        if (max <= 0) return List.of();
        List<String> ids = sync().zrange(RedisKeys.AWAITING, 0, Math.max(0, max * 4L));
        List<Job> out = new ArrayList<>();
        for (String id : ids) {
            if (out.size() >= max) break;
            String body = sync().hget(RedisKeys.PREFIX + "job:" + id, "body");
            if (body == null) continue;
            Job j = serializer.deserializeJob(body);
            if (j.relationship().isPresent()
                    && j.relationship().get().parentId().equals(parentId)) {
                out.add(j);
            }
        }
        return out;
    }

    @Override
    public boolean tryAcquireMutex(String name, String holder, Duration leaseDuration) {
        Names.requireName("mutex", name);
        Objects.requireNonNull(holder, "holder");
        Mutexes.requirePositive(leaseDuration);
        String key = RedisKeys.userKey("mutex", name);
        long millis = leaseDuration.toMillis();
        // Acquire-or-refresh inside a single Lua script: a separate SET-NX +
        // GET + PEXPIRE has a microsecond race window where the key can
        // expire between the SET-NX failing and the PEXPIRE running, leaving
        // the caller believing it holds a mutex that no longer exists. The
        // Lua script removes that window.
        String reply = sync().eval(
                        LuaScripts.mutexAcquire(),
                        ScriptOutputType.VALUE,
                        new String[] {key},
                        holder,
                        Long.toString(millis));
        return "ACQUIRED".equals(reply) || "REFRESHED".equals(reply);
    }

    @Override
    public boolean replaceJob(JobId id, long expectedVersion, JobReplacement replacement) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(replacement, "replacement");
        RedisClusterCommands<String, String> r = sync();
        Map<String, String> hash = r.hgetall(RedisKeys.job(id));
        if (hash == null || hash.isEmpty()) return false;
        JobState state;
        try {
            state = JobState.valueOf(hash.get("state"));
        } catch (IllegalArgumentException | NullPointerException e) {
            return false;
        }
        if (state != JobState.ENQUEUED && state != JobState.SCHEDULED && state != JobState.AWAITING) {
            return false;
        }
        long currentVersion = Long.parseLong(hash.get("version"));
        if (currentVersion != expectedVersion) {
            throw new StaleJobException(id, expectedVersion);
        }
        String oldQueue = hash.get("queue");
        String oldConcurrencyKey = blankToNull(hash.get("concurrency_key"));
        String oldConcurrencyMode = blankToNull(hash.get("concurrency_mode"));

        Job current = serializer.deserializeJob(hash.get("body"));
        Job replaced = JobReplacements.apply(current, replacement);
        Names.requireName("queue", replaced.queue());
        long nextVersion = currentVersion + 1;
        JobSnapshot snap = withVersion(replaced, nextVersion);
        // Serialize BEFORE the eval call so an oversize replacement throws without
        // mutating anything in Redis.
        String newBody = serializer.serializeJob(snap, capabilities);
        Instant stateAt = lastTransitionTime(snap, state);
        String newActive = activeKeyFor(state, snap.queue(), snap.ownerNodeId());
        String oldActive = activeKeyFor(state, oldQueue, snap.ownerNodeId());
        double newScore = activeScoreFor(snap, stateAt);

        var keys = new String[] {
            RedisKeys.job(id),
            newActive == null ? "" : newActive,
            oldActive == null ? "" : oldActive,
            RedisKeys.byStateTime(state),
            concurrencyPendingKey(oldConcurrencyKey),
            concurrencyPendingKey(snap)
        };
        var args = new String[] {
            id.toString(),
            Long.toString(expectedVersion),
            Long.toString(nextVersion),
            newBody,
            snap.queue(),
            Integer.toString(snap.priority()),
            snap.spec().handlerType(),
            snap.scheduledFor() == null ? "" : Long.toString(snap.scheduledFor().toEpochMilli()),
            Long.toString(stateAt.toEpochMilli()),
            Double.toString(newScore),
            snap.workflowRootId().toString(),
            snap.concurrencyKey() == null ? "" : snap.concurrencyKey(),
            snap.concurrencyMode() == null ? "" : snap.concurrencyMode().name(),
            concurrencyPendingMember(oldConcurrencyMode, id),
            concurrencyPendingMember(snap),
            Long.toString(toEpochMicros(stateAt))
        };
        String reply = r.eval(LuaScripts.replaceJob(), ScriptOutputType.VALUE, keys, args);
        if ("STALE".equals(reply)) throw new StaleJobException(id, expectedVersion);
        if ("OK".equals(reply)) {
            if (state == JobState.ENQUEUED) r.sadd(RedisKeys.QUEUES, snap.queue());
            return true;
        }
        return false;
    }

    @Override
    public void releaseMutex(String name, String holder) {
        Names.requireName("mutex", name);
        Objects.requireNonNull(holder, "holder");
        String key = RedisKeys.userKey("mutex", name);
        // Compare-and-delete via Lua so we don't race a new acquirer.
        sync().eval(
                        "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end",
                        io.lettuce.core.ScriptOutputType.INTEGER,
                        new String[] {key},
                        holder);
    }

    // ---------------------------------------------------------------- cron tasks

    private static final String CRON_TASKS_INDEX = RedisKeys.PREFIX + "cron_tasks";

    @Override
    public void upsertCronTask(com.hemju.threadmill.core.schedule.CronTask task) {
        Objects.requireNonNull(task, "task");
        Names.requireName("cronTask", task.name());
        Names.requireName("queue", task.queue());
        String kind;
        String value;
        var trigger = task.trigger();
        if (trigger instanceof com.hemju.threadmill.core.schedule.CronTask.Trigger.CronExpr ce) {
            kind = "CRON";
            value = ce.expression().expression();
        } else if (trigger instanceof com.hemju.threadmill.core.schedule.CronTask.Trigger.Interval iv) {
            kind = "INTERVAL";
            value = iv.interval().toString();
        } else {
            throw new IllegalStateException("Unknown trigger: " + trigger);
        }
        RedisClusterCommands<String, String> r = sync();
        r.hset(
                RedisKeys.userKey("cron_task", task.name()),
                Map.of(
                        "trigger_kind", kind,
                        "trigger_value", value,
                        "handler_signature", task.handlerType(),
                        "payload_type_tag", task.payloadArgument().typeTag(),
                        "payload_serialized", task.payloadArgument().serialized(),
                        "queue", task.queue(),
                        "priority", Integer.toString(task.priority()),
                        "missed_run_policy", task.missedRunPolicy().name(),
                        "time_zone", task.zone().getId(),
                        "enabled", Boolean.toString(task.enabled())));
        r.sadd(CRON_TASKS_INDEX, task.name());
    }

    @Override
    public Optional<com.hemju.threadmill.core.schedule.CronTask> findCronTask(String name) {
        Names.requireName("cronTask", name);
        Map<String, String> hash = sync().hgetall(RedisKeys.userKey("cron_task", name));
        if (hash == null || hash.isEmpty()) return Optional.empty();
        return Optional.of(readCronTask(name, hash));
    }

    @Override
    public List<com.hemju.threadmill.core.schedule.CronTask> listCronTasks() {
        List<com.hemju.threadmill.core.schedule.CronTask> out = new ArrayList<>();
        for (String name : sync().smembers(CRON_TASKS_INDEX)) {
            findCronTask(name).ifPresent(out::add);
        }
        return out;
    }

    @Override
    public void deleteCronTask(String name) {
        Names.requireName("cronTask", name);
        RedisClusterCommands<String, String> r = sync();
        r.del(RedisKeys.userKey("cron_task", name));
        r.del(RedisKeys.userKey("cron_task_state", name));
        r.srem(CRON_TASKS_INDEX, name);
    }

    @Override
    public void upsertCronTaskState(CronTaskScheduleState state) {
        Objects.requireNonNull(state, "state");
        Names.requireName("cronTask", state.taskName());
        Map<String, String> fields = new LinkedHashMap<>();
        if (state.lastRunAt() != null)
            fields.put("last_run_at", Long.toString(state.lastRunAt().toEpochMilli()));
        if (state.lastRunJobId() != null)
            fields.put("last_run_job_id", state.lastRunJobId().toString());
        if (state.nextRunAt() != null)
            fields.put("next_run_at", Long.toString(state.nextRunAt().toEpochMilli()));
        if (state.inFlightJobId() != null)
            fields.put("in_flight_job_id", state.inFlightJobId().toString());
        RedisClusterCommands<String, String> r = sync();
        String key = RedisKeys.userKey("cron_task_state", state.taskName());
        r.del(key); // overwrite semantics so cleared fields actually clear
        if (!fields.isEmpty()) r.hset(key, fields);
    }

    @Override
    public Optional<CronTaskScheduleState> findCronTaskState(String name) {
        Names.requireName("cronTask", name);
        Map<String, String> hash = sync().hgetall(RedisKeys.userKey("cron_task_state", name));
        if (hash == null) return Optional.empty();
        if (hash.isEmpty()) {
            // No state at all — but the task might exist with no recorded run yet.
            if (sync().exists(RedisKeys.userKey("cron_task", name)) == 0) return Optional.empty();
            return Optional.of(new CronTaskScheduleState(name, null, null, null, null));
        }
        return Optional.of(new CronTaskScheduleState(
                name,
                hash.containsKey("last_run_at") ? Instant.ofEpochMilli(Long.parseLong(hash.get("last_run_at"))) : null,
                hash.containsKey("last_run_job_id") ? UUID.fromString(hash.get("last_run_job_id")) : null,
                hash.containsKey("next_run_at") ? Instant.ofEpochMilli(Long.parseLong(hash.get("next_run_at"))) : null,
                hash.containsKey("in_flight_job_id") ? UUID.fromString(hash.get("in_flight_job_id")) : null));
    }

    private com.hemju.threadmill.core.schedule.CronTask readCronTask(String name, Map<String, String> hash) {
        String kind = hash.get("trigger_kind");
        String value = hash.get("trigger_value");
        com.hemju.threadmill.core.schedule.CronTask.Trigger trigger;
        if ("CRON".equals(kind)) {
            trigger = new com.hemju.threadmill.core.schedule.CronTask.Trigger.CronExpr(
                    com.hemju.threadmill.core.schedule.CronExpression.parse(value));
        } else {
            trigger = new com.hemju.threadmill.core.schedule.CronTask.Trigger.Interval(Duration.parse(value));
        }
        return new com.hemju.threadmill.core.schedule.CronTask(
                name,
                trigger,
                hash.get("handler_signature"),
                new com.hemju.threadmill.core.spec.JobArgument(
                        hash.get("payload_type_tag"), hash.get("payload_serialized")),
                hash.get("queue"),
                Integer.parseInt(hash.get("priority")),
                com.hemju.threadmill.core.schedule.CronTask.MissedRunPolicy.valueOf(hash.get("missed_run_policy")),
                ZoneId.of(hash.get("time_zone")),
                Boolean.parseBoolean(hash.get("enabled")));
    }

    // ---------------------------------------------------------------- helpers

    private RedisClusterCommands<String, String> sync() {
        return commands;
    }

    private List<Job> loadJobs(List<String> ids) {
        if (ids == null || ids.isEmpty()) return List.of();
        List<Job> out = new ArrayList<>(ids.size());
        RedisClusterCommands<String, String> r = sync();
        for (String idStr : ids) {
            String body = r.hget(RedisKeys.PREFIX + "job:" + idStr, "body");
            if (body != null) out.add(serializer.deserializeJob(body));
        }
        return out;
    }

    private JobSnapshot snapshotForInsert(RedisClusterCommands<String, String> r, Job job, long version) {
        JobSnapshot s = withVersion(job, version);
        if (s.relationship() == null) {
            return s;
        }
        Map<String, String> parent = r.hgetall(RedisKeys.job(s.relationship().parentId()));
        if (parent == null || parent.isEmpty()) {
            return s;
        }
        String root = parent.get("workflow_root_id");
        String key = blankToNull(parent.get("concurrency_key"));
        String modeValue = blankToNull(parent.get("concurrency_mode"));
        return new JobSnapshot(
                s.id(),
                s.spec(),
                s.queue(),
                s.priority(),
                s.createdAt(),
                s.cronTaskId(),
                s.relationship(),
                root == null ? s.workflowRootId() : JobId.parse(root),
                key,
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
                s.attempts());
    }

    private static String concurrencyPendingKey(JobSnapshot snapshot) {
        return concurrencyPendingKey(snapshot.concurrencyKey());
    }

    private static String concurrencyPendingKey(String key) {
        return key == null ? "" : RedisKeys.concurrencyPending(key);
    }

    private static String concurrencyCountersKey(String key) {
        return key == null ? "" : RedisKeys.concurrencyCounters(key);
    }

    private static String concurrencyWorkflowsKey(String key) {
        return key == null ? "" : RedisKeys.concurrencyWorkflows(key);
    }

    private static String concurrencyPendingMember(JobSnapshot snapshot) {
        return snapshot.concurrencyMode() == null
                ? ""
                : RedisKeys.concurrencyPendingMember(snapshot.concurrencyMode(), snapshot.id());
    }

    private static String concurrencyPendingMember(String mode, JobId id) {
        return mode == null ? "" : RedisKeys.concurrencyPendingMember(ConcurrencyMode.valueOf(mode), id);
    }

    private int workflowOutstandingCount(RedisClusterCommands<String, String> r, JobSnapshot candidate) {
        if (candidate.concurrencyKey() == null) {
            return 0;
        }
        int count = 0;
        for (Map<String, String> hash :
                hashesForStates(r, JobState.AWAITING, JobState.SCHEDULED, JobState.ENQUEUED, JobState.PROCESSING)) {
            if (candidate.workflowRootId().toString().equals(hash.get("workflow_root_id"))
                    && candidate.concurrencyKey().equals(blankToNull(hash.get("concurrency_key")))) {
                count++;
            }
        }
        return Math.max(1, count);
    }

    private static boolean tryClaimLock(RedisClusterCommands<String, String> r, String key, String token) {
        String reply = r.set(key, token, SetArgs.Builder.nx().px(30_000));
        return "OK".equals(reply);
    }

    private static List<String> concurrencyClaimLockKeys(List<JobSnapshot> snapshots) {
        return snapshots.stream()
                .map(JobSnapshot::concurrencyKey)
                .filter(Objects::nonNull)
                .map(RedisKeys::concurrencyClaimLock)
                .distinct()
                .sorted()
                .toList();
    }

    private static List<String> claimLockKeys(List<ClaimLock> locks) {
        return locks.stream().map(ClaimLock::key).toList();
    }

    private static List<ClaimLock> acquireClaimLocks(RedisClusterCommands<String, String> r, List<String> keys) {
        if (keys.isEmpty()) {
            return List.of();
        }
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (true) {
            var acquired = new ArrayList<ClaimLock>(keys.size());
            boolean complete = true;
            for (String key : keys) {
                String token = UUID.randomUUID().toString();
                if (tryClaimLock(r, key, token)) {
                    acquired.add(new ClaimLock(key, token));
                } else {
                    complete = false;
                    break;
                }
            }
            if (complete) {
                return List.copyOf(acquired);
            }
            releaseClaimLocks(r, acquired);
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("Timed out waiting for concurrency claim lock");
            }
            java.util.concurrent.locks.LockSupport.parkNanos(
                    Duration.ofMillis(2).toNanos());
        }
    }

    private static void releaseClaimLocks(RedisClusterCommands<String, String> r, List<ClaimLock> locks) {
        for (int i = locks.size() - 1; i >= 0; i--) {
            ClaimLock lock = locks.get(i);
            releaseClaimLock(r, lock.key(), lock.token());
        }
    }

    private static void releaseClaimLock(RedisClusterCommands<String, String> r, String key, String token) {
        r.eval(
                "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end",
                ScriptOutputType.INTEGER,
                new String[] {key},
                token);
    }

    private List<Map<String, String>> hashesForStates(RedisClusterCommands<String, String> r, JobState... states) {
        List<Map<String, String>> out = new ArrayList<>();
        for (JobState state : states) {
            List<String> ids = r.zrange(RedisKeys.byStateTime(state), 0, -1);
            if (ids == null) continue;
            for (String id : ids) {
                Map<String, String> hash = r.hgetall(RedisKeys.job(JobId.parse(id)));
                if (hash != null && !hash.isEmpty()) {
                    var copy = new HashMap<>(hash);
                    copy.put("id", id);
                    out.add(copy);
                }
            }
        }
        return out;
    }

    private static String activeKeyFor(JobState state, String queue, NodeId ownerNode) {
        return switch (state) {
            case ENQUEUED -> RedisKeys.queue(queue);
            case SCHEDULED -> RedisKeys.SCHEDULED;
            case AWAITING -> RedisKeys.AWAITING;
            case PROCESSING -> RedisKeys.PROCESSING_ALL;
            case PROCESSED, SUCCEEDED, FAILED, DELETED, QUARANTINED -> null;
        };
    }

    private static double activeScoreFor(JobSnapshot s, Instant stateAt) {
        return switch (s.currentState()) {
            case ENQUEUED ->
                RedisKeys.queueScore(s.priority(), stateAt.getEpochSecond() * 1_000_000L + stateAt.getNano() / 1_000L);
            case SCHEDULED ->
                s.scheduledFor() == null
                        ? stateAt.toEpochMilli()
                        : (double) s.scheduledFor().toEpochMilli();
            case AWAITING -> (double) stateAt.toEpochMilli();
            case PROCESSING ->
                (double)
                        (s.ownerHeartbeatAt() == null
                                ? stateAt.toEpochMilli()
                                : s.ownerHeartbeatAt().toEpochMilli());
            case PROCESSED, SUCCEEDED, FAILED, DELETED, QUARANTINED -> 0d;
        };
    }

    private static Instant lastTransitionTime(JobSnapshot snapshot, JobState state) {
        List<JobStateEntry> history = snapshot.stateHistory();
        for (int i = history.size() - 1; i >= 0; i--) {
            if (history.get(i).state() == state) return history.get(i).at();
        }
        return snapshot.createdAt();
    }

    private static long toEpochMicros(Instant instant) {
        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
    }

    private static JobSnapshot withVersion(Job job, long version) {
        JobSnapshot s = job.snapshot();
        return new JobSnapshot(
                s.id(),
                s.spec(),
                s.queue(),
                s.priority(),
                s.createdAt(),
                s.cronTaskId(),
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

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static JobStoreCapabilities defaultCapabilities() {
        return new JobStoreCapabilities(
                JobStoreCapabilities.DEFAULT_MAX_SERIALIZED_BYTES,
                JobStoreCapabilities.DEFAULT_MAX_JOB_LOG_BYTES,
                JobStoreCapabilities.DEFAULT_MAX_FAILURE_METADATA_BYTES,
                1000,
                false,
                true,
                true,
                true);
    }

    @SuppressWarnings("unused")
    private static List<String> ensureList(Object o) {
        if (o == null) return Collections.emptyList();
        if (o instanceof List<?> l) {
            List<String> out = new ArrayList<>(l.size());
            for (Object e : l) out.add(String.valueOf(e));
            return out;
        }
        throw new IllegalStateException("expected list, got " + o.getClass());
    }

    @SuppressWarnings("unused")
    private static String requireNonEmpty(String s) {
        if (s == null || s.isEmpty()) throw new IllegalStateException("expected non-empty value");
        return s;
    }

    @SuppressWarnings("unused")
    private static ZAddArgs xx() {
        return ZAddArgs.Builder.xx();
    }
}
