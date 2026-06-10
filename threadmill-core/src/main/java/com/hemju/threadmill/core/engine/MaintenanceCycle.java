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
 */
public final class MaintenanceCycle {

    private static final Logger LOG = LoggerFactory.getLogger(MaintenanceCycle.class);

    private final JobStore store;
    private final NodeId nodeId;
    private final NodeRegistry registry;
    private final JobRunner runner;
    private final RecurringMaterializer materializer;
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
            ProcessingNodeConfig config) {
        this(store, nodeId, registry, runner, materializer, config, new LocalWakeBus());
    }

    public MaintenanceCycle(
            JobStore store,
            NodeId nodeId,
            NodeRegistry registry,
            JobRunner runner,
            RecurringMaterializer materializer,
            ProcessingNodeConfig config,
            LocalWakeBus wakeBus) {
        this.store = Objects.requireNonNull(store, "store");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.runner = Objects.requireNonNull(runner, "runner");
        this.materializer = Objects.requireNonNull(materializer, "materializer");
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

    private void heartbeatLoop() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                store.touchOwnerHeartbeat(nodeId, Instant.now());
            } catch (Throwable t) {
                LOG.warn("Owner-heartbeat refresh failed", t);
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
                        nodeHeartbeatRetentionSweep();
                        dedupRetentionSweep();
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
