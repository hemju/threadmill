package com.hemju.threadmill.core.store;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobReplacement;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.OversizedJobException;
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;

/**
 * Persistence SPI for Threadmill jobs and nodes.
 *
 * <p>The contract is intentionally expressed in <strong>operations and
 * guarantees</strong>, not in SQL. A relational store and a key-value store
 * must both be able to satisfy it honestly — the abstract contract test in
 * {@code threadmill-test-support} pins down the exact semantics, and every
 * concrete store passes the same suite.
 *
 * <h2>Concurrency &amp; durability</h2>
 * <ul>
 *   <li>Every mutating operation is durable on return — either the change is
 *       persisted, or an exception was thrown.</li>
 *   <li>Optimistic concurrency: {@link #saveAtomic(Job, long)} succeeds only
 *       if the persisted version matches {@code expectedVersion}, and the
 *       new version is adopted into the in-memory job only after that
 *       success.</li>
 *   <li>{@link #claimReady(NodeId, String, int, Instant)} is atomic across
 *       contending nodes — a given job is claimed by exactly one node.</li>
 *   <li>Operations against a vanished id behave as a defined,
 *       non-exceptional case (an {@link Optional#empty()} or a no-op, as
 *       documented per method).</li>
 * </ul>
 *
 * <h2>Size bound</h2>
 * <p>{@link #insert(Job)} and {@link #saveAtomic(Job, long)} throw
 * {@link OversizedJobException} when a job's serialized form exceeds
 * {@link JobStoreCapabilities#maxSerializedJobBytes()}. They never corrupt
 * the in-memory version.
 */
public interface JobStore {

    // ---------------------------------------------------------------- capabilities

    /** Returns the static capabilities of this store. */
    JobStoreCapabilities capabilities();

    /**
     * Human-readable, single-line identification of the backing store —
     * shown in startup banners and operator-facing logs. Should include the
     * concrete technology plus enough topology / version detail that an
     * operator can tell which datastore the engine is pointed at (for
     * example {@code "PostgreSQL 18.1 @ threadmill"} or
     * {@code "Redis standalone host=localhost port=6379"}). Implementations
     * must return this in constant time without any I/O against the store.
     *
     * <p>The default returns the implementation's simple class name so
     * existing third-party stores remain compilable.
     */
    default String describe() {
        return getClass().getSimpleName();
    }

    /**
     * Lightweight writable probe used after capacity-related store failures.
     * Implementations with a meaningful no-op write can override this method;
     * the default preserves the historical read-only probe.
     */
    default void verifyWritable() {
        capabilities();
    }

    // ---------------------------------------------------------------- single-job ops

    /**
     * Insert a freshly-created job. The job's persisted version is set to 1
     * on success and adopted into the in-memory job via
     * {@link Job#adoptVersion(long)}.
     *
     * @throws IllegalStateException     if a job with the same id already exists
     * @throws OversizedJobException     if the serialized form exceeds the limit
     */
    void insert(Job job);

    /**
     * Atomically insert {@code jobs} as a single logical operation. Either
     * every job is persisted (and each in-memory job's version adopted), or
     * none are.
     *
     * <p>Failure semantics: if any job in the batch fails serialization
     * (e.g. an {@link OversizedJobException}), the <strong>entire batch is
     * rejected</strong> and <strong>no in-memory {@code Job} has its version
     * mutated</strong>. Backends must funnel serialization through a single
     * pre-flight pass so partial-batch corruption is impossible by
     * construction.
     *
     * <p>Concurrency-keyed jobs are accepted in the batch without a
     * fallback to per-job inserts. Threadmill enforces concurrency at
     * <em>claim</em> time (via {@link JobStoreCapabilities#supportsConcurrencyGroups()}),
     * so bulk insertion is safe regardless of per-job
     * {@code concurrencyKey} / {@code concurrencyMode}.
     *
     * <p>Implementations should be at least one round-trip cheaper than
     * {@code jobs.size()} calls to {@link #insert(Job)}. Hosts using
     * PostgreSQL should set {@code reWriteBatchedInserts=true} on the
     * pgJDBC URL to realise the batched-insert win.
     *
     * @return the inserted job ids, in input order
     * @throws IllegalStateException if any job's id already exists; the
     *     batch is rejected as a whole
     * @throws OversizedJobException if any job's serialized form exceeds
     *     the limit; the batch is rejected as a whole
     */
    List<JobId> insertAll(List<Job> jobs);

    /**
     * Atomically insert {@code job} unless the deduplication key for its queue
     * is already active.
     *
     * @return {@link EnqueueResult.Created} for a new job, or
     *         {@link EnqueueResult.Coalesced} with the existing job id
     */
    EnqueueResult enqueueIfAbsent(Job job, String dedupKey, Duration ttl, Instant now);

    /** Load a job by id; {@code Optional.empty()} if the job does not exist. */
    Optional<Job> findById(JobId id);

    /**
     * Conditional update: persist {@code job} only if the store's version
     * still equals {@code expectedVersion}. On success, the new persisted
     * version is adopted into the in-memory job via {@link Job#adoptVersion(long)}.
     *
     * @throws StaleJobException     if the persisted version no longer matches
     * @throws OversizedJobException if the new serialized form exceeds the limit;
     *                               the in-memory version is unchanged
     */
    void saveAtomic(Job job, long expectedVersion);

    /**
     * Soft-delete by id (transition to {@code DELETED}). Acts on the
     * job's <em>current</em> persisted version atomically. A vanished id is
     * a no-op (returns {@code false}).
     *
     * @return {@code true} if the job existed and is now {@code DELETED}
     */
    boolean softDelete(JobId id);

    // ---------------------------------------------------------------- claim & heartbeat

    /**
     * Atomically claim up to {@code max} jobs in {@code queue} that are
     * ready for dispatch. Each claimed job moves to {@code PROCESSING} with
     * the given node as owner and {@code heartbeatAt} as its initial
     * heartbeat. No two nodes can claim the same job.
     *
     * <p>If the queue is currently paused via {@link #pauseQueue(String, String)},
     * the call returns an empty list — pending jobs remain {@code ENQUEUED}
     * and become claimable again on {@link #resumeQueue(String)}.
     *
     * @return the claimed jobs (newest persisted state)
     */
    List<Job> claimReady(NodeId nodeId, String queue, int max, Instant heartbeatAt);

    // ---------------------------------------------------------------- queue pauses

    /**
     * Pause claiming from {@code queue}. Idempotent — repeated calls update
     * the reason / timestamp but never throw. Pending jobs stay
     * {@code ENQUEUED}; in-flight jobs continue to run to completion.
     *
     * @param queue  the queue to pause
     * @param reason a short free-text label for ops audit trails (nullable)
     */
    void pauseQueue(String queue, String reason);

    /**
     * Resume claiming from {@code queue}. Idempotent — resuming a queue
     * that is not currently paused is a no-op.
     */
    void resumeQueue(String queue);

    /** Snapshot of queues currently paused. */
    Set<String> listPausedQueues();

    /**
     * Update the heartbeat for all jobs this node currently owns to {@code now}.
     */
    void touchOwnerHeartbeat(NodeId nodeId, Instant now);

    /**
     * Persist execution-time updates such as check-ins, progress, and logs
     * without advancing the optimistic-lock version. The update applies only
     * while the job is still {@code PROCESSING} and owned by {@code nodeId}.
     */
    boolean saveExecutionUpdate(Job job, NodeId nodeId);

    /** Record a node-level heartbeat. */
    void recordNodeHeartbeat(NodeId nodeId, Instant now);

    /** Read the last recorded node-level heartbeat, if any. */
    Optional<Instant> readNodeHeartbeat(NodeId nodeId);

    /**
     * Acquire or renew the cluster-wide maintenance lease for this node.
     * At most one node may hold the lease at a time; the current holder may
     * renew it before expiry. Stores should use datastore-side time or TTL
     * semantics where possible.
     *
     * @throws IllegalArgumentException if {@code leaseDuration} is null,
     *     zero, or negative
     */
    boolean acquireOrRenewMaintenanceLease(NodeId nodeId, Duration leaseDuration);

    /** Release the maintenance lease iff it is currently held by {@code nodeId}. */
    void releaseMaintenanceLease(NodeId nodeId);

    /** Read the current maintenance lease owner, if a non-expired owner exists. */
    Optional<NodeId> readMaintenanceLeaseOwner();

    // ---------------------------------------------------------------- housekeeping queries

    /**
     * Jobs in {@code SCHEDULED} whose {@code scheduledFor} is at or before
     * {@code now}. Used by {@code MaintenanceCycle} to promote them to
     * {@code ENQUEUED}.
     */
    List<Job> findDueForPromotion(Instant now, int max);

    /**
     * Jobs in {@code PROCESSING} whose owner heartbeat is at or before
     * {@code heartbeatExpiry}. Used by the orphan-recovery path. Recovery
     * itself flows through {@code FAILED} — never directly back to
     * {@code ENQUEUED} — so the engine's single failure code path runs.
     */
    List<Job> findOrphaned(Instant heartbeatExpiry, int max);

    // ---------------------------------------------------------------- counts & search

    /**
     * Point-in-time count of jobs per state. May be approximate on stores
     * that advertise {@code !supportsExactCounts}, but must always include
     * every state — never omit a key for state with zero jobs.
     */
    Map<JobState, Long> countsByState();

    /** Point-in-time queue depths for active queues. */
    Map<String, Long> queueDepths();

    /** Queue names that currently have at least one ENQUEUED job. */
    List<String> listEnqueuedQueues();

    /** Oldest currently ENQUEUED job time for the queue, if the queue has jobs. */
    Optional<Instant> oldestEnqueuedAt(String queue);

    /** Oldest owner heartbeat among PROCESSING jobs, if any. */
    Optional<Instant> oldestProcessingHeartbeat();

    /** Last heartbeat recorded for each known processing node. */
    List<NodeHeartbeat> listNodeHeartbeats();

    /**
     * Delete node heartbeat records at or before {@code cutoff}. This is
     * registry retention only; it must not affect in-flight job owner
     * heartbeats or the maintenance lease.
     *
     * @return the number of node heartbeat records removed
     */
    long deleteNodeHeartbeatsOlderThan(Instant cutoff);

    /** Delete expired producer-side deduplication records that no longer protect active jobs. */
    long deleteExpiredDedupKeys(Instant now, int max);

    /**
     * Find jobs whose handler-type signature matches {@code handlerType}.
     * Returns at most {@code max}; primarily used by the dashboard and by
     * cross-job features (workflows / batches / replacement) to locate
     * candidates.
     */
    List<Job> findByHandlerSignature(String handlerType, int max);

    // ---------------------------------------------------------------- retention

    /**
     * Hard-delete up to {@code max} jobs in {@code state} that entered that
     * state at or before {@code cutoff}. Returns the number actually deleted.
     */
    long deleteFinishedOlderThan(Instant cutoff, JobState state, int max);

    // ---------------------------------------------------------------- relationships, mutexes, replacement

    /**
     * Find AWAITING jobs whose {@code JobRelationship.parentId} equals
     * {@code parentId}. Used by the workflow successor and batch member
     * promotion paths. Returns at most {@code max}.
     */
    List<Job> findAwaitingByParent(JobId parentId, int max);

    /**
     * Acquire a named cross-cluster mutex for {@code holder} for at most
     * {@code leaseDuration}. Returns {@code true} on success, {@code false}
     * if the mutex is currently held by another holder. The lease must
     * survive a holder crash — a stale entry must expire at the lease end,
     * never block forever.
     *
     * <p>Reentrant for the same holder: a successful re-acquire by the
     * current holder refreshes the lease.
     *
     * @throws IllegalArgumentException if {@code leaseDuration} is
     *     {@code null}, zero, or negative. A non-positive lease has no
     *     defensible semantics — different stores would diverge on what
     *     "already-expired" means at acquire time. Surfaced eagerly at the
     *     call site.
     */
    boolean tryAcquireMutex(String name, String holder, Duration leaseDuration);

    /** Release a named mutex iff currently held by {@code holder}. */
    void releaseMutex(String name, String holder);

    /**
     * Atomically replace a non-running job's spec / queue / priority /
     * scheduled-for, contingent on its current version.
     *
     * <p>Only succeeds if:
     * <ul>
     *   <li>the job exists,</li>
     *   <li>its persisted version equals {@code expectedVersion}, and</li>
     *   <li>its current state is one of
     *       {@link JobState#ENQUEUED}, {@link JobState#SCHEDULED},
     *       {@link JobState#AWAITING}.</li>
     * </ul>
     *
     * <p>On success, the version is bumped by one. On version mismatch a
     * {@link StaleJobException} is thrown. On wrong state or vanished id,
     * returns {@code false}.
     *
     * @return {@code true} if the replacement was applied
     */
    boolean replaceJob(JobId id, long expectedVersion, JobReplacement replacement);

    // ---------------------------------------------------------------- recurring tasks

    /**
     * Insert or update a {@link com.hemju.threadmill.core.schedule.CronTask}
     * by name. Identity is the task name; schedule-state is held separately
     * (see {@link #upsertCronTaskState}). An upsert that replaces the
     * definition must not silently reset the schedule-state — that is the
     * caller's responsibility.
     */
    void upsertCronTask(CronTask task);

    /** Load a cron task by name; {@code Optional.empty()} if it does not exist. */
    Optional<CronTask> findCronTask(String name);

    /** List every registered cron task. */
    List<CronTask> listCronTasks();

    /** Delete a cron task and its schedule-state. */
    void deleteCronTask(String name);

    /** Record that {@code namespace} owns the durable cron task {@code taskName}. */
    void recordCronTaskOwnership(String namespace, String taskName);

    /** List task names currently owned by {@code namespace}. */
    Set<String> listCronTaskNamesOwnedBy(String namespace);

    /** Insert or update the schedule-state for a cron task. */
    void upsertCronTaskState(CronTaskScheduleState state);

    /** Read a cron task's current schedule-state. */
    Optional<CronTaskScheduleState> findCronTaskState(String name);
}
