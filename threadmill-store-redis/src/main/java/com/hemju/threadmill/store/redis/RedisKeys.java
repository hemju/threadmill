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
 *       {@code -priority} so {@code ZRANGE LIMIT 0 N} returns highest-priority
 *       first and Redis's lexicographic member tie-break orders equal-priority
 *       UUIDv7 ids.</li>
 *   <li>{@code {threadmill}:scheduled} — ZSET of SCHEDULED ids scored by
 *       {@code scheduled_at} (millis).</li>
 *   <li>{@code {threadmill}:awaiting} — ZSET of AWAITING ids scored by
 *       state-entry time.</li>
 *   <li>{@code {threadmill}:processing} — global ZSET of PROCESSING ids
 *       scored by {@code owner_heartbeat_at}; used by orphan recovery.</li>
 *   <li>{@code {threadmill}:processing:{nodeId}} — per-node ZSET (same score)
 *       used for cheap {@code touchOwnerHeartbeat}.</li>
 *   <li>{@code {threadmill}:by_handler:{handlerType}} — SET of ids.</li>
 *   <li>{@code {threadmill}:awaiting_by_parent:{parentId}} — SET of AWAITING
 *       child ids, maintained inside the insert / save / delete scripts so
 *       successor lookups never scan the global AWAITING ZSET.</li>
 *   <li>{@code {threadmill}:by_state_time:{STATE}} — ZSET of ids scored by
 *       {@code current_state_at}; used for retention.</li>
 *   <li>{@code {threadmill}:counts} — HASH state → cardinality (engine-maintained
 *       inside the same atomic transition that changes the state).</li>
 *   <li>{@code {threadmill}:concurrency:{key}:counters} — HASH with in-flight
 *       counts for claim-time per-key concurrency.</li>
 *   <li>{@code {threadmill}:concurrency:{key}:pending} — ZSET of pending
 *       concurrency members, scored by enqueue-time micros.</li>
 *   <li>{@code {threadmill}:concurrency:{key}:pending_root:{rootId}} — per
 *       workflow-root mirror of the pending ZSET (same members and scores),
 *       maintained only for jobs whose {@code workflow_root_id} differs from
 *       their own id. Lets the claim path find active-hold members without
 *       scanning the whole pending population.</li>
 *   <li>{@code {threadmill}:queue_keys:{queue}} — HASH concurrency-key &rarr;
 *       count of ENQUEUED keyed jobs of that key in the queue. The claim
 *       path advances a bounded rotating HSCAN cursor over this registry.</li>
 *   <li>{@code {threadmill}:queue_unkeyed:{queue}} — ZSET of ENQUEUED
 *       unkeyed job ids, scored like the queue ZSET, so the unkeyed claim
 *       lane never pages past keyed work.</li>
 *   <li>{@code {threadmill}:queue_enqueued_at:{queue}} — ZSET of every
 *       ENQUEUED job id in the queue scored by {@code current_state_at}
 *       millis. {@code oldestEnqueuedAt} reads its head, so the age gauge
 *       is one indexed read instead of a per-member scan of the
 *       priority-ordered queue ZSET.</li>
 *   <li>{@code {threadmill}:layout:queue_enqueued_at} — STRING upgrade state
 *       of the age index ({@code backfilled} / {@code complete}).</li>
 *   <li>{@code {threadmill}:layout:queue_priority} — STRING priority-score
 *       upgrade state ({@code rescored} / {@code priority_only_v1}).</li>
 *   <li>{@code {threadmill}:node:layout:{nodeId}} — STRING layout version with
 *       the heartbeat TTL. The integer increases monotonically as layouts are
 *       added: value {@code 1} identifies an age-index-aware node that still
 *       writes legacy priority scores; values {@code >= 2} maintain both
 *       current layouts; a missing or malformed value identifies an older
 *       release.</li>
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
  /** Same-slot sentinel passed for optional Lua KEYS entries that are absent. */
  public static final String NO_KEY = PREFIX + "no_key";
  /** HASH queue-name &rarr; pause-reason (empty string if no reason supplied). */
  public static final String QUEUE_PAUSES = PREFIX + "queue_pauses";

  /**
   * STRING recording the upgrade state of the per-queue {@code queue_enqueued_at}
   * age index. Absent on stores written by releases before the index existed
   * (v0.2.1 and earlier); {@code backfilled} once a new-release store has
   * rebuilt it from the queue ZSETs while old-release writers may still be
   * live; {@code complete} once no old-release node heartbeat remains and a
   * final exact reconciliation has run.
   */
  public static final String QUEUE_ENQUEUED_AT_LAYOUT = PREFIX + "layout:queue_enqueued_at";

  /**
   * STRING recording the queue-score upgrade state. {@code rescored} means an
   * exact pass ran while a legacy-scoring node may still be live;
   * {@code priority_only_v1} means a final pass ran after those heartbeats
   * disappeared.
   */
  public static final String QUEUE_PRIORITY_LAYOUT = PREFIX + "layout:queue_priority";

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

  /**
   * Written alongside the heartbeat as a monotonically increasing integer
   * advertising the node's maintained Redis layouts.
   */
  public static String nodeLayout(NodeId node) {
    Objects.requireNonNull(node, "node");
    return PREFIX + "node:layout:" + node;
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

  /** Per-parent index of AWAITING children, so successor lookups need no scan. */
  public static String awaitingByParent(JobId parentId) {
    Objects.requireNonNull(parentId, "parentId");
    return PREFIX + "awaiting_by_parent:" + parentId;
  }

  public static String dedupExpiry() {
    return PREFIX + "dedup_expiry";
  }

  public static String resetAnchor() {
    return PREFIX + "reset_anchor";
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

  /**
   * Per workflow-root mirror of {@link #concurrencyPending(String)}, kept only
   * for members whose workflow root differs from their own job id.
   */
  public static String concurrencyPendingRoot(String key, String rootId) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(rootId, "rootId");
    return PREFIX + "concurrency:" + userSegment(key) + ":pending_root:" + rootId;
  }

  /** HASH concurrency-key &rarr; count of ENQUEUED keyed jobs of that key in the queue. */
  public static String queueKeys(String queue) {
    Objects.requireNonNull(queue, "queue");
    return PREFIX + "queue_keys:" + userSegment(queue);
  }

  /** ZSET of ENQUEUED unkeyed job ids in the queue, scored like the queue ZSET. */
  public static String queueUnkeyed(String queue) {
    Objects.requireNonNull(queue, "queue");
    return PREFIX + "queue_unkeyed:" + userSegment(queue);
  }

  /**
   * ZSET of ENQUEUED job ids in the queue scored by {@code current_state_at}
   * millis; the head is the queue's oldest enqueued job.
   */
  public static String queueEnqueuedAt(String queue) {
    Objects.requireNonNull(queue, "queue");
    return PREFIX + "queue_enqueued_at:" + userSegment(queue);
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
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Computes the exact ZSET score used by per-queue ready sets.
   *
   * <p>Every {@code int} is exactly representable as a Redis double score.
   * Negating the priority makes the smallest score the highest priority;
   * equal scores use Redis's lexicographic member order, which is the job-id
   * tie-break required by the store contract.
   */
  public static double queueScore(int priority) {
    // Canonicalize zero: negation yields -0.0, while Redis returns +0.0 and
    // boxed Double equality distinguishes their raw representations.
    return priority == 0 ? 0d : -(double) priority;
  }

  /**
   * Computes the exact queue score while accepting the legacy enqueue-time
   * argument for source compatibility. Time no longer participates in queue
   * ordering.
   *
   * @param priority job priority
   * @param ignoredEnqueueMicros legacy enqueue-time argument, ignored
   * @return the exact negated-priority score
   * @deprecated use {@link #queueScore(int)}
   */
  @Deprecated(since = "0.2.2", forRemoval = true)
  public static double queueScore(int priority, long ignoredEnqueueMicros) {
    return queueScore(priority);
  }
}
