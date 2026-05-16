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
 */
public final class MaintenanceCycle {

    private static final Logger LOG = LoggerFactory.getLogger(MaintenanceCycle.class);

    private final JobStore store;
    private final NodeId nodeId;
    private final NodeRegistry registry;
    private final JobRunner runner;
    private final RecurringMaterializer materializer;
    private final ProcessingNodeConfig config;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<Thread> loopThread = new AtomicReference<>();

    public MaintenanceCycle(
            JobStore store,
            NodeId nodeId,
            NodeRegistry registry,
            JobRunner runner,
            RecurringMaterializer materializer,
            ProcessingNodeConfig config) {
        this.store = Objects.requireNonNull(store, "store");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.runner = Objects.requireNonNull(runner, "runner");
        this.materializer = Objects.requireNonNull(materializer, "materializer");
        this.config = Objects.requireNonNull(config, "config");
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;
        Thread t = Thread.ofPlatform()
                .name("threadmill-maintenance-" + nodeId)
                .daemon(true)
                .start(this::loop);
        loopThread.set(t);
    }

    public void stop() {
        running.set(false);
        Thread t = loopThread.getAndSet(null);
        if (t != null) t.interrupt();
    }

    private void loop() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                // Heartbeat is always done, even before knowing if we're master,
                // so the registry sees us alive.
                store.touchOwnerHeartbeat(nodeId, Instant.now());
                if (registry.isMaster()) {
                    materializer.tick(Instant.now());
                    promoteScheduled();
                    reclaimOrphans();
                    retentionSweep();
                    nodeHeartbeatRetentionSweep();
                    dedupRetentionSweep();
                }
                sleep(config.claimHeartbeat());
            } catch (Throwable t) {
                LOG.warn("Maintenance cycle failed", t);
                sleep(config.claimHeartbeat());
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

    private void retentionSweep() {
        Duration retention = Duration.ofDays(7);
        var cutoff = Instant.now().minus(retention);
        store.deleteFinishedOlderThan(cutoff, JobState.SUCCEEDED, 100);
    }

    private void nodeHeartbeatRetentionSweep() {
        var cutoff = Instant.now().minus(config.nodeHeartbeatRetention());
        long removed = store.deleteNodeHeartbeatsOlderThan(cutoff);
        if (removed > 0) {
            LOG.debug("Deleted {} stale node heartbeat records older than {}", removed, cutoff);
        }
    }

    private void dedupRetentionSweep() {
        store.deleteExpiredDedupKeys(Instant.now(), 100);
    }

    private static void sleep(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
