package com.hemju.threadmill.store.redis;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;

/**
 * Centralised key naming for {@link RedisJobStore}.
 *
 * <p>Every key Threadmill uses is built here. Any new key shape must be
 * added here so the keys-used inventory stays auditable and so no
 * accidental collisions slip in.
 *
 * <h2>Layout</h2>
 * <ul>
 *   <li>{@code {threadmill}:job:{id}} — HASH per job (body + scalar fields + version).</li>
 *   <li>{@code {threadmill}:queue:{queue}} — ZSET of ENQUEUED job ids, scored
 *       {@code -priority * 1e13 + enqueue_micros} so {@code ZRANGE LIMIT 0 N}
 *       returns highest-priority, oldest first.</li>
 *   <li>{@code {threadmill}:scheduled} — ZSET of SCHEDULED ids scored by
 *       {@code scheduled_at} (millis).</li>
 *   <li>{@code {threadmill}:awaiting} — ZSET of AWAITING ids scored by
 *       state-entry time.</li>
 *   <li>{@code {threadmill}:processing} — global ZSET of PROCESSING ids
 *       scored by {@code owner_heartbeat_at}; used by orphan recovery.</li>
 *   <li>{@code {threadmill}:processing:{nodeId}} — per-node ZSET (same score)
 *       used for cheap {@code touchOwnerHeartbeat}.</li>
 *   <li>{@code {threadmill}:by_handler:{handlerType}} — SET of ids.</li>
 *   <li>{@code {threadmill}:by_state_time:{STATE}} — ZSET of ids scored by
 *       {@code current_state_at}; used for retention.</li>
 *   <li>{@code {threadmill}:counts} — HASH state → cardinality (engine-maintained
 *       inside the same atomic transition that changes the state).</li>
 *   <li>{@code {threadmill}:concurrency:{key}:counters} — HASH with in-flight
 *       counts for claim-time per-key concurrency.</li>
 *   <li>{@code {threadmill}:concurrency:{key}:pending} — ZSET of pending
 *       concurrency members, scored by enqueue-time micros.</li>
 *   <li>{@code {threadmill}:concurrency:{key}:workflows} — HASH workflow root
 *       id → active outstanding hold count.</li>
 *   <li>{@code {threadmill}:concurrency:{key}:workflow_counts} — HASH workflow
 *       root id → total non-terminal job count.</li>
 *   <li>{@code {threadmill}:concurrency:{key}:claim_lock} — short-lived mutex
 *       around per-key claim bookkeeping.</li>
 *   <li>{@code {threadmill}:node:heartbeat:{nodeId}} — STRING with TTL; the key
 *       is the heartbeat and natural-expiry is the timeout.</li>
 * </ul>
 */
public final class RedisKeys {

    public static final String PREFIX = "{threadmill}:";

    public static final String COUNTS = PREFIX + "counts";
    public static final String SCHEDULED = PREFIX + "scheduled";
    public static final String AWAITING = PREFIX + "awaiting";
    public static final String PROCESSING_ALL = PREFIX + "processing";
    public static final String QUEUES = PREFIX + "queues";
    public static final String NODES = PREFIX + "nodes";
    public static final String MAINTENANCE_LEASE = PREFIX + "lease:maintenance";
    /** HASH queue-name &rarr; pause-reason (empty string if no reason supplied). */
    public static final String QUEUE_PAUSES = PREFIX + "queue_pauses";

    private RedisKeys() {}

    public static String job(JobId id) {
        Objects.requireNonNull(id, "id");
        return PREFIX + "job:" + id;
    }

    public static String queue(String queue) {
        Objects.requireNonNull(queue, "queue");
        return PREFIX + "queue:" + userSegment(queue);
    }

    public static String processingFor(NodeId node) {
        Objects.requireNonNull(node, "node");
        return PREFIX + "processing:" + node;
    }

    public static String byHandler(String handlerType) {
        Objects.requireNonNull(handlerType, "handlerType");
        return PREFIX + "by_handler:" + userSegment(handlerType);
    }

    public static String byStateTime(JobState state) {
        Objects.requireNonNull(state, "state");
        return PREFIX + "by_state_time:" + state.name();
    }

    public static String nodeHeartbeat(NodeId node) {
        Objects.requireNonNull(node, "node");
        return PREFIX + "node:heartbeat:" + node;
    }

    public static String userKey(String prefix, String name) {
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(name, "name");
        return PREFIX + prefix + ":" + userSegment(name);
    }

    public static String cronTaskNamespace(String namespace) {
        Objects.requireNonNull(namespace, "namespace");
        return PREFIX + "cron_task_namespace:" + userSegment(namespace);
    }

    public static String dedup(String queue, String dedupKey) {
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(dedupKey, "dedupKey");
        return PREFIX + "dedup:" + userSegment(queue) + ":" + userSegment(dedupKey);
    }

    public static String dedupExpiry() {
        return PREFIX + "dedup_expiry";
    }

    public static String concurrencyClaimLock(String key) {
        Objects.requireNonNull(key, "key");
        return PREFIX + "concurrency:" + userSegment(key) + ":claim_lock";
    }

    public static String concurrencyCounters(String key) {
        Objects.requireNonNull(key, "key");
        return PREFIX + "concurrency:" + userSegment(key) + ":counters";
    }

    public static String concurrencyPending(String key) {
        Objects.requireNonNull(key, "key");
        return PREFIX + "concurrency:" + userSegment(key) + ":pending";
    }

    public static String concurrencyWorkflows(String key) {
        Objects.requireNonNull(key, "key");
        return PREFIX + "concurrency:" + userSegment(key) + ":workflows";
    }

    public static String concurrencyWorkflowCounts(String key) {
        Objects.requireNonNull(key, "key");
        return PREFIX + "concurrency:" + userSegment(key) + ":workflow_counts";
    }

    public static String concurrencyPendingMember(ConcurrencyMode mode, JobId id) {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(id, "id");
        return mode.name() + ":" + id;
    }

    public static String userSegment(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    /** Computes the ZSET score used by per-queue ready sets. */
    public static double queueScore(int priority, long enqueueMicros) {
        return (-(double) priority) * 1e13 + (double) enqueueMicros;
    }
}
