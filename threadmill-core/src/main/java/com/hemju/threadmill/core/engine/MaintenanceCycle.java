package com.hemju.threadmill.core.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.schedule.RecurringMaterializer;
import com.hemju.threadmill.core.store.JobStore;

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
 *   <li>{@code maintenancePollInterval} (the loop tick) bounds materialization,
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
        this(store, nodeId, registry, runner, materializer, retryInterceptor, config, new LocalWakeBus());
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
                store.touchOwnerHeartbeat(nodeId, Instant.now());
                if (consecutiveFailures > 0) {
                    consecutiveFailures = 0;
                    if (claimSuspended != null && claimSuspended.compareAndSet(true, false)) {
                        LOG.warn("Owner-heartbeat refresh recovered on node {} — resuming claiming", nodeId);
                    }
                }
            } catch (Throwable t) {
                consecutiveFailures++;
                LOG.warn("Owner-heartbeat refresh failed on node {} ({} consecutive)", nodeId, consecutiveFailures, t);
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
        //   - the loop ticks at maintenancePollInterval (fast; bounds materialize/promote/orphan latency)
        //   - retention sweeps fire at retentionInterval (slowest; deletion is not time-sensitive)
        // Owner-heartbeat refresh runs on its own thread (see start()).
        Instant nextRetention = Instant.EPOCH;
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                Instant now = Instant.now();
                if (registry.isMaster()) {
                    materializer.tick(now);
                    promoteScheduled();
                    reclaimOrphans();
                    if (!now.isBefore(nextRetention)) {
                        retentionSweep();
                        recoverStrandedFailedJobs();
                        nodeHeartbeatRetentionSweep();
                        dedupRetentionSweep();
                        reconcileOrphanedWorkflowChildren();
                        nextRetention = now.plus(config.retentionInterval());
                    }
                }
                sleep(config.maintenancePollInterval());
            } catch (Throwable t) {
                LOG.warn("Maintenance cycle failed", t);
                sleep(config.maintenancePollInterval());
            }
        }
    }

    private void promoteScheduled() {
        List<Job> due = store.findDueForPromotion(Instant.now(), 100);
        for (Job j : due) {
            try {
                long v = j.version();
                JobState from = j.currentState();
                j.transitionTo(JobState.ENQUEUED, Instant.now(), "engine.promote", null);
                j.clearScheduledFor();
                store.saveAtomic(j, v);
                wakeBus.wake(j.queue());
                // No interceptor for state-change here keeps the failure path responsibility-clear;
                // the engine's only state-change hook is JobInterceptors via JobRunner.
            } catch (StaleJobException ignored) {
                // Another node beat us; that's fine.
            }
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
     * housekeeping cannot starve the owner-heartbeat refresh that shares
     * this loop. Anything left over carries to the next retention tick.
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
        int recovered = retryInterceptor.recoverStrandedFailures(WORKFLOW_RECONCILE_SCAN, STRANDED_FAILED_MIN_AGE);
        if (recovered > 0) {
            LOG.info("Recovered {} jobs stranded in FAILED with unspent retry budget", recovered);
        }
    }

    /**
     * Recover workflow children stranded in AWAITING because their predecessor
     * reached a terminal state but the promote/abandon hook never ran (a crash
     * between the terminal save and the interceptor). Reuses the workflow
     * interceptor's idempotent promote/abandon logic. Runs on the retention
     * cadence — recovery latency for this rare crash window is not urgent.
     */
    private void reconcileOrphanedWorkflowChildren() {
        new WorkflowInterceptor(store).reconcileOrphanedAwaitingChildren(WORKFLOW_RECONCILE_SCAN);
    }

    private void retentionSweep() {
        var now = Instant.now();
        sweepTerminalState(JobState.SUCCEEDED, now.minus(config.succeededRetention()));
        sweepTerminalState(JobState.FAILED, now.minus(config.failedRetention()));
        sweepTerminalState(JobState.DELETED, now.minus(config.deletedRetention()));
        sweepTerminalState(JobState.QUARANTINED, now.minus(config.quarantinedRetention()));
    }

    private void sweepTerminalState(JobState state, Instant cutoff) {
        for (int i = 0; i < MAX_RETENTION_BATCHES_PER_TICK; i++) {
            long deleted = store.deleteFinishedOlderThan(cutoff, state, RETENTION_BATCH);
            if (deleted < RETENTION_BATCH) {
                return;
            }
        }
        LOG.debug("Retention sweep for {} hit the per-tick batch cap; continuing on the next tick", state);
    }

    private void nodeHeartbeatRetentionSweep() {
        var cutoff = Instant.now().minus(config.nodeHeartbeatRetention());
        long removed = store.deleteNodeHeartbeatsOlderThan(cutoff);
        if (removed > 0) {
            LOG.debug("Deleted {} stale node heartbeat records older than {}", removed, cutoff);
        }
    }

    private void dedupRetentionSweep() {
        var now = Instant.now();
        for (int i = 0; i < MAX_RETENTION_BATCHES_PER_TICK; i++) {
            long deleted = store.deleteExpiredDedupKeys(now, RETENTION_BATCH);
            if (deleted < RETENTION_BATCH) {
                return;
            }
        }
        LOG.debug("Dedup retention sweep hit the per-tick batch cap; continuing on the next tick");
    }

    private static void sleep(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
