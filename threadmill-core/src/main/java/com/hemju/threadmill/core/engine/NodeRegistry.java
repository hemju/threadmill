package com.hemju.threadmill.core.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.store.JobStore;

/**
 * Cluster membership: each node records a heartbeat in the store and tries
 * to hold the store-backed maintenance lease.
 *
 * <p>The registry exposes a simple {@link #isMaster()} check the
 * {@link MaintenanceCycle} consults each tick. At most one node can hold the
 * lease; under store uncertainty, no node acts as master.
 *
 * <p><strong>Engine-internal.</strong> This class is {@code public} only for
 * the engine's own cross-package wiring and its test harnesses; it is NOT
 * part of Threadmill's supported public API. Its constructors, methods, and
 * behavior may change in any release without notice — do not reference it
 * from application code. The supported surface is {@code ProcessingNode},
 * {@code Scheduler}, and the SPI interfaces.
 */
public final class NodeRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(NodeRegistry.class);

    private final JobStore store;
    private final NodeId nodeId;
    private final Duration heartbeatTimeout;
    private final Duration heartbeatInterval;
    private final Duration maintenanceLeaseDuration;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<Thread> loopThread = new AtomicReference<>();
    private volatile boolean master;
    private volatile Instant masterUntil = Instant.EPOCH;

    public NodeRegistry(
            JobStore store,
            NodeId nodeId,
            Duration heartbeatTimeout,
            Duration heartbeatInterval,
            Duration maintenanceLeaseDuration) {
        this.store = Objects.requireNonNull(store, "store");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.heartbeatTimeout = Objects.requireNonNull(heartbeatTimeout, "heartbeatTimeout");
        this.heartbeatInterval = Objects.requireNonNull(heartbeatInterval, "heartbeatInterval");
        this.maintenanceLeaseDuration = Objects.requireNonNull(maintenanceLeaseDuration, "maintenanceLeaseDuration");
    }

    public NodeId nodeId() {
        return nodeId;
    }

    /**
     * Whether this node currently holds the maintenance lease. Mastership
     * self-expires at the local lease deadline: a registry tick that hangs
     * (rather than throws) for longer than the lease duration must not leave
     * this node acting as master while another node legitimately acquires
     * the expired lease.
     */
    public boolean isMaster() {
        return master && Instant.now().isBefore(masterUntil);
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;
        // The first heartbeat and election run synchronously so a freshly
        // started node is registered before dispatchers begin claiming.
        tickOnce();
        Thread t = Thread.ofPlatform()
                .name("threadmill-registry-" + nodeId)
                .daemon(true)
                .start(this::loop);
        loopThread.set(t);
    }

    public void stop() {
        running.set(false);
        Thread t = loopThread.getAndSet(null);
        if (t != null) t.interrupt();
        try {
            store.recordNodeHeartbeat(nodeId, Instant.EPOCH);
            store.releaseMaintenanceLease(nodeId);
        } catch (Throwable ignored) {
            // best-effort
        }
    }

    private void loop() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            tickOnce();
            try {
                Thread.sleep(heartbeatInterval.toMillis());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void tickOnce() {
        try {
            // Captured before the store calls: the lease is acquired at or
            // after this instant, so the local deadline is conservative.
            Instant renewalStart = Instant.now();
            store.recordNodeHeartbeat(nodeId, renewalStart);
            boolean elected = electedMaster();
            if (elected) {
                masterUntil = renewalStart.plus(maintenanceLeaseDuration);
            }
            master = elected;
        } catch (Throwable t) {
            LOG.warn("NodeRegistry tick failed", t);
            master = false; // refuse to act as master under store uncertainty
        }
    }

    /**
     * We are master if our node heartbeat is fresh and this node holds the
     * store-backed maintenance lease.
     */
    private boolean electedMaster() {
        Instant ourBeat = store.readNodeHeartbeat(nodeId).orElse(null);
        if (ourBeat == null) return false;
        if (!ourBeat.isAfter(Instant.now().minus(heartbeatTimeout))) return false;
        return store.acquireOrRenewMaintenanceLease(nodeId, maintenanceLeaseDuration);
    }
}
