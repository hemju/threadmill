package com.hemju.threadmill.core.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.internal.ExecutionHeartbeats;
import com.hemju.threadmill.core.internal.FatalErrors;
import com.hemju.threadmill.core.schedule.RecurringMaterializer;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.core.store.RetentionCursor;

/**
 * Master-only housekeeping loop. Runs on the elected master node only;
 * other nodes' MaintenanceCycle threads sleep until they win the lease.
 *
 * <h2>Responsibilities</h2>
 * <ol>
 *   <li>Promote due {@code SCHEDULED} jobs to {@code ENQUEUED}.</li>
 *   <li>Refresh this node's owner heartbeats on its in-progress jobs.</li>
 *   <li>Scan for orphans (jobs whose owner's heartbeat has expired) and
 *       route them through the engine's single failure path via
 *       {@link JobRunner#reclaimOrphan(Job)}.</li>
 *   <li>Retention deletes (succeeded jobs older than the cutoff).</li>
 *   <li>Node-registry retention deletes stale heartbeat records for nodes
 *       that have not returned.</li>
 * </ol>
 *
 * <h2>Cadences</h2>
 * <p>Three independent cadences share one loop thread to avoid coupling
 * latency-sensitive ops to slow housekeeping:
 * <ul>
 *   <li>{@code maintenancePollInterval} (the loop tick) paces materialization,
 *       promotion, and orphan reclaim latency. Default 1 s.</li>
 *   <li>{@code claimHeartbeat} drives owner-heartbeat refresh — slow enough not
 *       to thrash the store, fast enough to stay well below {@code heartbeatTimeout}.
 *       Default 15 s.</li>
 *   <li>{@code retentionInterval} drives retention sweeps — old SUCCEEDED jobs,
 *       expired dedup keys, and stale node-heartbeat rows. Default 1 h.</li>
 * </ul>
 *
 * <p><strong>Engine-internal.</strong> This class is {@code public} only for
 * the engine's own cross-package wiring and its test harnesses; it is NOT
 * part of Threadmill's supported public API. Its constructors, methods, and
 * behavior may change in any release without notice — do not reference it
 * from application code. The supported surface is {@code ProcessingNode},
 * {@code Scheduler}, and the SPI interfaces.
 */
public final class MaintenanceCycle {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceCycle.class);

  private final JobStore store;
  private final NodeId nodeId;
  private final NodeRegistry registry;
  private final JobRunner runner;
  private final RecurringMaterializer materializer;
  private final RetryInterceptor retryInterceptor;
  private final ProcessingNodeConfig config;
  private final LocalWakeBus wakeBus;
  private final WorkflowInterceptor workflowInterceptor;
  private Instant nextRetention = Instant.EPOCH;
  private Instant nextRetryRecovery = Instant.EPOCH;
  private Instant nextWorkflowReconciliation = Instant.EPOCH;
  private static final Duration RECOVERY_PASS_INTERVAL = Duration.ofSeconds(30);
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicReference<Thread> loopThread = new AtomicReference<>();
  private final AtomicReference<Thread> heartbeatThread = new AtomicReference<>();

  public MaintenanceCycle(
      JobStore store,
      NodeId nodeId,
      NodeRegistry registry,
      JobRunner runner,
      RecurringMaterializer materializer,
      RetryInterceptor retryInterceptor,
      ProcessingNodeConfig config) {
    this(
        store,
        nodeId,
        registry,
        runner,
        materializer,
        retryInterceptor,
        config,
        new LocalWakeBus());
  }

  public MaintenanceCycle(
      JobStore store,
      NodeId nodeId,
      NodeRegistry registry,
      JobRunner runner,
      RecurringMaterializer materializer,
      RetryInterceptor retryInterceptor,
      ProcessingNodeConfig config,
      LocalWakeBus wakeBus) {
    this.store = Objects.requireNonNull(store, "store");
    this.workflowInterceptor = new WorkflowInterceptor(store);
    this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
    this.registry = Objects.requireNonNull(registry, "registry");
    this.runner = Objects.requireNonNull(runner, "runner");
    this.materializer = Objects.requireNonNull(materializer, "materializer");
    this.retryInterceptor = Objects.requireNonNull(retryInterceptor, "retryInterceptor");
    this.config = Objects.requireNonNull(config, "config");
    this.wakeBus = Objects.requireNonNull(wakeBus, "wakeBus");
  }

  public void start() {
    if (!running.compareAndSet(false, true)) return;
    Thread t = Thread.ofPlatform()
        .name("threadmill-maintenance-" + nodeId)
        .daemon(true)
        .start(this::loop);
    loopThread.set(t);
    // Owner-heartbeat refresh gets its own lightweight thread: master
    // work (an unbounded CATCH_UP burst, a slow retention sweep) must
    // never starve the refresh that keeps this node's own PROCESSING
    // jobs from expiring into orphan reclaim mid-run.
    Thread hb = Thread.ofPlatform()
        .name("threadmill-owner-heartbeat-" + nodeId)
        .daemon(true)
        .start(this::heartbeatLoop);
    heartbeatThread.set(hb);
  }

  public void stop() {
    running.set(false);
    Thread t = loopThread.getAndSet(null);
    if (t != null) t.interrupt();
    Thread hb = heartbeatThread.getAndSet(null);
    if (hb != null) hb.interrupt();
  }

  /**
   * Gate flipped to {@code true} when this node's owner-heartbeat writes keep
   * failing. ProcessingNode wires it into the dispatchers so they stop claiming
   * new work — otherwise the maintenance leader orphan-reclaims this node's
   * in-flight jobs (their heartbeats have gone stale) while this node keeps
   * claiming more, compounding duplicate execution. Null = no escalation.
   */
  private volatile AtomicBoolean claimSuspended;

  void setClaimSuspensionGate(AtomicBoolean gate) {
    this.claimSuspended = gate;
  }

  private void heartbeatLoop() {
    // Suspend claiming after heartbeats have failed for close to
    // heartbeatTimeout (when the leader would start reclaiming), leaving a
    // tick of margin. heartbeatTimeout >= 2 * claimHeartbeat is enforced.
    int threshold = Math.max(
        1,
        (int) (config.heartbeatTimeout().toMillis()
                / Math.max(1, config.claimHeartbeat().toMillis()))
            - 1);
    int consecutiveFailures = 0;
    while (running.get() && !Thread.currentThread().isInterrupted()) {
      try {
        refreshActiveHeartbeats();
        if (consecutiveFailures > 0) {
          consecutiveFailures = 0;
          if (claimSuspended != null && claimSuspended.compareAndSet(true, false)) {
            LOG.warn("Owner-heartbeat refresh recovered on node {} — resuming claiming", nodeId);
          }
        }
      } catch (RuntimeException t) {
        FatalErrors.rethrowIfFatal(t);
        consecutiveFailures++;
        LOG.warn(
            "Owner-heartbeat refresh failed on node {} ({} consecutive)",
            nodeId,
            consecutiveFailures,
            t);
        if (consecutiveFailures >= threshold
            && claimSuspended != null
            && claimSuspended.compareAndSet(false, true)) {
          LOG.error(
              "Owner-heartbeat refresh failing on node {} ({} consecutive) — suspending claiming so the"
                  + " maintenance leader's orphan reclaim does not duplicate this node's in-flight jobs",
              nodeId,
              consecutiveFailures);
        }
      }
      sleep(config.claimHeartbeat());
    }
  }

  private void loop() {
    // Two cadences share the master thread:
    //   - the loop ticks at maintenancePollInterval (fast; resumes materialize/promote/orphan
    // latency)
    //   - retention sweeps fire at retentionInterval (slowest; deletion is not time-sensitive)
    // Owner-heartbeat refresh runs on its own thread (see start()).
    while (running.get() && !Thread.currentThread().isInterrupted()) {
      try {
        Instant now = Instant.now();
        if (registry.isMaster()) {
          runActivity("promotion", this::promoteScheduled);
          runActivity("recurring", () -> materializer.tick(now));
          if (!now.isBefore(nextRetryRecovery)) {
            runActivity("retry recovery", () -> {
              recoverStrandedFailedJobs();
              if (retryInterceptor.recoveryPassComplete())
                nextRetryRecovery = Instant.now().plus(RECOVERY_PASS_INTERVAL);
            });
          }
          if (!now.isBefore(nextWorkflowReconciliation)) {
            runActivity("workflow reconciliation", () -> {
              reconcileOrphanedWorkflowChildren();
              if (workflowInterceptor.reconciliationPassComplete())
                nextWorkflowReconciliation = Instant.now().plus(RECOVERY_PASS_INTERVAL);
            });
          }
          runActivity("orphan recovery", this::reclaimOrphans);
          runActivity("concurrency metadata", () -> store.deleteIdleConcurrencyGroups(100));
          runActivity("queue metadata", () -> store.deleteIdleQueueMetadata(100));
          if (!now.isBefore(nextRetention)) {
            runActivity("retention", () -> {
              boolean more = retentionSweep();
              more |= dedupRetentionSweep();
              nodeHeartbeatRetentionSweep();
              if (!more) {
                completedRetentionStates.clear();
                retentionCutoffs.clear();
              }
              nextRetention = more ? now : now.plus(config.retentionInterval());
            });
          }
        }
        sleep(config.maintenancePollInterval());
      } catch (RuntimeException t) {
        FatalErrors.rethrowIfFatal(t);
        LOG.warn("Maintenance cycle failed", t);
        sleep(config.maintenancePollInterval());
      }
    }
  }

  private void refreshActiveHeartbeats() {
    var now = Instant.now();
    var batch = new HashMap<JobId, Long>();
    for (var claim : runner.activeClaims().entrySet()) {
      batch.put(claim.getKey(), claim.getValue());
      if (batch.size() == ExecutionHeartbeats.MAX_BATCH) {
        store.touchExecutionHeartbeats(nodeId, batch, now);
        batch.clear();
      }
    }
    if (!batch.isEmpty()) store.touchExecutionHeartbeats(nodeId, batch, now);
  }

  private static void runActivity(String name, Runnable activity) {
    try {
      activity.run();
    } catch (RuntimeException failure) {
      FatalErrors.rethrowIfFatal(failure);
      LOG.warn("Maintenance {} failed; continuing other activities", name, failure);
    }
  }

  private void promoteScheduled() {
    long deadline = System.nanoTime() + 200_000_000L;
    int promoted = 0;
    while (promoted < 500) {
      var due = store.findDueForPromotion(Instant.now(), Math.min(50, 500 - promoted));
      for (var job : due) {
        if (promoted > 0 && System.nanoTime() >= deadline) return;
        try {
          long version = job.version();
          job.transitionTo(JobState.ENQUEUED, Instant.now(), "engine.promote", null);
          job.clearScheduledFor();
          store.saveAtomic(job, version);
          wakeBus.wake(job.queue());
        } catch (StaleJobException ignored) {
          // Another node handled this candidate.
        }
        promoted++;
      }
      if (due.size() < 50 || System.nanoTime() >= deadline) return;
    }
  }

  private void reclaimOrphans() {
    var cutoff = Instant.now().minus(config.heartbeatTimeout());
    List<Job> orphans = store.findOrphaned(cutoff, 100);
    for (Job j : orphans) {
      runner.reclaimOrphan(j);
    }
  }

  /** Batch size for each retention delete call. */
  private static final int RETENTION_BATCH = 100;

  /**
   * Per-tick cap on retention batches per state, bounding tick duration so
   * housekeeping cannot starve promotion and recovery. Anything left over
   * resumes on the next maintenance tick, without waiting for the retention interval.
   */
  private static final int MAX_RETENTION_BATCHES_PER_TICK = 50;

  /** Cap on AWAITING jobs inspected per reconciliation pass. */
  private static final int WORKFLOW_RECONCILE_SCAN = 500;

  /**
   * A FAILED job younger than this is left to the live retry hook — the
   * reschedule save normally lands within milliseconds of the FAILED save.
   */
  private static final Duration STRANDED_FAILED_MIN_AGE = Duration.ofMinutes(5);

  /**
   * Rescue jobs stranded in FAILED with unspent retry budget (a crash in
   * the window between the FAILED save and the reschedule save). Must run
   * BEFORE the workflow reconciliation sweep: a recovered parent is
   * SCHEDULED again by the time the sweep judges its AWAITING children,
   * so they are left alone instead of abandoned.
   */
  private void recoverStrandedFailedJobs() {
    int recovered =
        retryInterceptor.recoverStrandedFailures(WORKFLOW_RECONCILE_SCAN, STRANDED_FAILED_MIN_AGE);
    if (recovered > 0) {
      LOG.info("Recovered {} jobs stranded in FAILED with unspent retry budget", recovered);
    }
  }

  /**
   * Recover workflow children stranded in AWAITING because their predecessor
   * reached a terminal state but the promote/abandon hook never ran (a crash
   * between the terminal save and the interceptor). Reuses the workflow
   * interceptor's idempotent transitions. An unfinished pass advances every
   * maintenance tick; complete passes pause for 30 seconds independently of retention.
   */
  private void reconcileOrphanedWorkflowChildren() {
    workflowInterceptor.reconcileOrphanedAwaitingChildren(WORKFLOW_RECONCILE_SCAN);
  }

  private final Map<JobState, RetentionCursor> retentionCursors = new EnumMap<>(JobState.class);
  private final Map<JobState, Instant> retentionCutoffs = new EnumMap<>(JobState.class);

  private final Set<JobState> completedRetentionStates = EnumSet.noneOf(JobState.class);

  private boolean retentionSweep() {
    var now = Instant.now();
    boolean more = sweepTerminalState(JobState.SUCCEEDED, now.minus(config.succeededRetention()));
    more |= sweepTerminalState(JobState.FAILED, now.minus(config.failedRetention()));
    more |= sweepTerminalState(JobState.DELETED, now.minus(config.deletedRetention()));
    more |= sweepTerminalState(JobState.QUARANTINED, now.minus(config.quarantinedRetention()));
    return more;
  }

  private boolean sweepTerminalState(JobState state, Instant cutoff) {
    if (completedRetentionStates.contains(state)) return false;
    var passCutoff = retentionCutoffs.computeIfAbsent(state, ignored -> cutoff);
    long deadline = System.nanoTime() + 200_000_000L;
    for (int i = 0; i < MAX_RETENTION_BATCHES_PER_TICK; i++) {
      var page =
          store.deleteFinishedPage(passCutoff, state, RETENTION_BATCH, retentionCursors.get(state));
      if (page.nextAfter() == null) {
        retentionCursors.remove(state);
        completedRetentionStates.add(state);
        return false;
      }
      retentionCursors.put(state, page.nextAfter());
      if (System.nanoTime() >= deadline) return true;
    }
    return true;
  }

  private void nodeHeartbeatRetentionSweep() {
    var cutoff = Instant.now().minus(config.nodeHeartbeatRetention());
    long removed = store.deleteNodeHeartbeatsOlderThan(cutoff);
    if (removed > 0) {
      LOG.debug("Deleted {} stale node heartbeat records older than {}", removed, cutoff);
    }
  }

  private boolean dedupRetentionSweep() {
    var now = Instant.now();
    long deadline = System.nanoTime() + 200_000_000L;
    for (int i = 0; i < MAX_RETENTION_BATCHES_PER_TICK; i++) {
      long deleted = store.deleteExpiredDedupKeys(now, RETENTION_BATCH);
      if (deleted < RETENTION_BATCH) {
        return false;
      }
      if (System.nanoTime() >= deadline) return true;
    }
    return true;
  }

  private static void sleep(Duration d) {
    try {
      Thread.sleep(d.toMillis());
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }
}
