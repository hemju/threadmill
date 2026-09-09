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
import com.hemju.threadmill.core.engine.RemoteWakeChannel;
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
   */
  String describe();

  /**
   * Return the wrapped store when this instance is a decorator.
   *
   * <p>Framework integrations use this hook to discover concrete store
   * capabilities such as PostgreSQL transaction participation without
   * forcing optional decorators to leak their implementation type. Concrete
   * stores return {@code this}; decorators should return their immediate
   * delegate.
   */
  JobStore delegate();

  /**
   * Lightweight writable probe used after capacity-related store failures.
   * Implementations must perform a meaningful no-op write.
   */
  void verifyWritable();

  /**
   * Whether this store can participate in an externally-managed transaction
   * (e.g. join a caller's JDBC transaction).
   *
   * <p>Returned as a generic capability so framework integrations can route
   * to a transaction-joined scheduler without instanceof-checking concrete
   * store classes — which keeps the integration module free of optional
   * store-implementation class references in its constant pool, so it can
   * be loaded even when the implementation module is not on the classpath.
   *
   * <p>Stores that genuinely support external transactions (today:
   * {@code PostgresJobStore} configured with an external transaction boundary)
   * return {@code true}.
   */
  boolean supportsExternalTransactions();

  /**
   * Create a {@link com.hemju.threadmill.core.engine.RemoteWakeChannel} that
   * delivers cross-node wake hints for jobs in this store, if the backing
   * technology offers a native pub/sub-style notification mechanism.
   *
   * <p>Returned as an SPI hook so framework integrations can wire the
   * channel without instanceof-checking concrete store classes or
   * referencing optional store-implementation types. Backends that have a
   * native notification path (today: {@code PostgresJobStore} via
   * {@code LISTEN}/{@code NOTIFY} and {@code RedisJobStore} via Pub/Sub)
   * return a channel; others return {@link java.util.Optional#empty()}.
   *
   * @param channelName the name to use for the notification channel; if
   *                    {@code null} the store may pick a sensible default.
   */
  Optional<RemoteWakeChannel> createRemoteWakeChannel(String channelName);

  // ---------------------------------------------------------------- single-job ops

  /**
   * Insert a freshly-created job. The job's persisted version is set to 1
   * on success and adopted into the in-memory job via
   * {@link Job#adoptVersion(long)}.
   *
   * @throws IllegalStateException     if a job with the same id already exists
   *                                  or inserting would reset a persisted version greater than 1
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
   * @throws IllegalArgumentException if the batch exceeds the capabilities
   *     job-count or combined serialized-byte budget; nothing is inserted
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
   * while the job is still {@code PROCESSING}, owned by {@code nodeId}, and
   * both its state version and execution revision match persisted state.
   * Success advances {@link Job#executionRevision()} after commit; a rejected
   * write leaves it unchanged. Owner heartbeats and check-ins never regress.
   * Return {@code false} for stale or superseded updates.
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

  /**
   * Bounded dashboard/search query over persisted jobs. Results should be
   * ordered newest state-transition first, then by id for stable paging.
   */
  List<Job> searchJobs(JobSearch search);

  /**
   * Bounded maintenance page in ascending canonical job-id order. Null
   * {@code after} starts a sweep; otherwise the id is exclusive. Removing
   * earlier rows cannot skip later rows. Concurrent changes may be observed
   * on the following sweep. Implementations cap {@code max} at 500.
   */
  List<Job> scanJobs(JobState state, JobId after, int max);

  /**
   * Bounded recurring-definition page in the backend's stable ascending name order.
   * Null starts a sweep; otherwise the name is exclusive. Maximum page size is 500.
   */
  List<CronTask> scanCronTasks(String after, int max);

  /**
   * Oldest maintenance timestamp in a state: scheduled due time for SCHEDULED,
   * state-entry time otherwise. Empty when no timestamp exists. This is an
   * indexed diagnostic, not an assertion that a failed job needs recovery or
   * that a terminal job is eligible for retention (dedup may protect it).
   */
  Optional<Instant> oldestMaintenanceAt(JobState state);

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

  /**
   * Inspect at most 100 concurrency groups (or the smaller requested maximum)
   * and remove bookkeeping only when no active hold or nonterminal work needs it.
   * Implementations retain a bounded cursor so busy keys cannot hide idle keys.
   * Returns groups removed; stores without persistent group bookkeeping return zero.
   */
  long deleteIdleConcurrencyGroups(int max);

  /**
   * Inspect at most {@code min(max, 100)} queue metadata groups and reclaim
   * obsolete bookkeeping without changing any job or aggregate count.
   * Returns metadata rows removed. Stores without persistent empty-queue
   * bookkeeping need no work. Implementations must advance past busy queues.
   */
  default long deleteIdleQueueMetadata(int max) {
    return 0;
  }

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
   * Inspect the first bounded retention page, returning the number deleted.
   * Use {@link #deleteFinishedPage} to resume beyond protected records.
   */
  default long deleteFinishedOlderThan(Instant cutoff, JobState state, int max) {
    return deleteFinishedPage(cutoff, state, max, null).deleted();
  }

  /**
   * Inspect at most {@code min(max, 100)} cutoff-eligible records in a terminal
   * state, resuming after the store's opaque cursor. Select candidates at or before cutoff
   * with no live dedup key or AWAITING child. FAILED records require an explicit
   * final failure decision; pending retries and legacy unknown decisions remain.
   * State/version and protections must be checked atomically with deletion.
   * The returned cursor advances even when no record can be deleted. A null
   * cursor completes this pass; changed/skipped/new earlier records wait for the next.
   * Keep the same state and cutoff throughout a pass. Recent records must not
   * consume its candidate budget or require body reads. A deleted cursor record
   * must not prevent the next page from progressing, including timestamp ties.
   */
  RetentionPage deleteFinishedPage(Instant cutoff, JobState state, int max, RetentionCursor after);

  // ---------------------------------------------------------------- relationships, mutexes,
  // replacement

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

  /**
   * Insert or update the schedule-state for a cron task.
   *
   * <p>Deliberately never writes
   * {@link CronTaskScheduleState#nudgeRequestedAt()}: a blanket state upsert
   * must not clobber a nudge accepted concurrently by another node. An
   * existing pending nudge survives this call unchanged; the only writers of
   * that field are {@link #requestCronNudge} and {@link #clearCronNudge}.
   */
  void upsertCronTaskState(CronTaskScheduleState state);

  /** Read a cron task's current schedule-state. */
  Optional<CronTaskScheduleState> findCronTaskState(String name);

  /**
   * Record an on-demand materialization request (a "nudge") for the named
   * recurring task by stamping
   * {@link CronTaskScheduleState#nudgeRequestedAt()} with
   * {@code requestedAt} and advancing the store-generated
   * {@link CronTaskScheduleState#nudgeRevision()}. A single cell per task:
   * a burst of nudges overwrites one value and therefore coalesces to at
   * most one follow-up materialization. The write is durable — it is
   * consumed by the maintenance master's recurring tick, so acceptance
   * from any node reaches the materializing node without any transient
   * signal.
   *
   * <p>Guarded: {@link NudgeOutcome#UNKNOWN_TASK} when no such task exists
   * (a nudge racing task removal must not resurrect schedule state) and
   * {@link NudgeOutcome#DISABLED} when the task is disabled — an explicit
   * pause wins over a nudge. The existence/enabled check and the write are
   * atomic with respect to concurrent task lifecycle operations.
   */
  NudgeOutcome requestCronNudge(String taskName, Instant requestedAt);

  /**
   * Clear a pending nudge, but only if its current
   * {@link CronTaskScheduleState#nudgeRevision()} still equals
   * {@code observedRevision} (compare-and-clear). The revision — never the
   * wall-clock timestamp, whose finite store precision can collide — is the
   * CAS identity: a nudge accepted between the caller's read and this clear
   * carries a strictly greater revision and survives, so the materializer's
   * next tick produces the follow-up run it promises. That is what makes
   * the run-after-wake guarantee hold without a producer-side mutex. The
   * revision is never reset for the lifetime of the task's schedule state,
   * so a cleared value cannot be reused while the task exists (deleting the
   * task drops the state row with it, so a task re-registered under the
   * same name legitimately starts over at one). Clearing an
   * already-cleared or never-set nudge is a no-op.
   */
  void clearCronNudge(String taskName, long observedRevision);

  /** Result of {@link #requestCronNudge}. */
  enum NudgeOutcome {
    /** The nudge was recorded (or refreshed an already-pending one). */
    ACCEPTED,
    /** No recurring task with that name exists. */
    UNKNOWN_TASK,
    /** The task exists but is disabled; the nudge was not recorded. */
    DISABLED
  }
}
