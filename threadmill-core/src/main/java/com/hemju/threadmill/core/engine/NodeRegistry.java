package com.hemju.threadmill.core.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.internal.FatalErrors;
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
    this.maintenanceLeaseDuration =
        Objects.requireNonNull(maintenanceLeaseDuration, "maintenanceLeaseDuration");
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
    return running.get() && master && Instant.now().isBefore(masterUntil);
  }

  public void start() {
    if (!running.compareAndSet(false, true)) return;
    boolean initialized = false;
    try {
      // The first heartbeat and election run synchronously so a freshly
      // started node is registered before dispatchers begin claiming.
      tickOnce();
      Thread t =
          Thread.ofPlatform().name("threadmill-registry-" + nodeId).daemon(true).start(this::loop);
      loopThread.set(t);
      initialized = true;
    } finally {
      // A failed synchronous tick must not leave start() permanently wedged behind running=true.
      if (!initialized) running.set(false);
    }
  }

  public void stop() {
    running.set(false);
    master = false;
    masterUntil = Instant.EPOCH;
    Thread t = loopThread.getAndSet(null);
    if (t != null) {
      t.interrupt();
      if (t != Thread.currentThread()) {
        try {
          // Finish ordinary in-flight writes before returning. An unresponsive
          // store must not make this join unbounded; the loop's finally still
          // withdraws if its last write completes after this wait.
          t.join(Duration.ofSeconds(1));
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
    withdrawFromStore();
  }

  private void withdrawFromStore() {
    // Let interrupt-sensitive clients send the final withdrawal; restore the
    // caller's cancellation state after the datastore operation.
    boolean interrupted = Thread.interrupted();
    try {
      store.recordNodeHeartbeat(nodeId, Instant.EPOCH);
      store.releaseMaintenanceLease(nodeId);
    } catch (Throwable cleanupFailure) {
      FatalErrors.rethrowIfFatal(cleanupFailure);
      // best-effort
    } finally {
      if (interrupted) Thread.currentThread().interrupt();
    }
  }

  private void loop() {
    Throwable primaryFailure = null;
    try {
      while (running.get() && !Thread.currentThread().isInterrupted()) {
        tickOnce();
        try {
          Thread.sleep(heartbeatInterval.toMillis());
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    } catch (RuntimeException | Error failure) {
      primaryFailure = failure;
      throw failure;
    } finally {
      master = false;
      masterUntil = Instant.EPOCH;
      // A datastore call may ignore interruption and commit after stop()'s
      // immediate withdrawal. This cleanup follows the last possible tick write.
      try {
        withdrawFromStore();
      } catch (RuntimeException | Error cleanupFailure) {
        if (primaryFailure != null) {
          if (cleanupFailure != primaryFailure) primaryFailure.addSuppressed(cleanupFailure);
        } else throw cleanupFailure;
      }
    }
  }

  private void tickOnce() {
    try {
      // Captured before the store calls: the lease is acquired at or
      // after this instant, so the local deadline is conservative.
      Instant renewalStart = Instant.now();
      store.recordNodeHeartbeat(nodeId, renewalStart);
      boolean elected = running.get() && electedMaster();
      if (elected) {
        masterUntil = renewalStart.plus(maintenanceLeaseDuration);
      }
      master = running.get() && elected;
    } catch (RuntimeException t) {
      FatalErrors.rethrowIfFatal(t);
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
