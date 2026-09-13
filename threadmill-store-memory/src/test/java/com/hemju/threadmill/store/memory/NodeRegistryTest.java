package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.NodeRegistry;
import com.hemju.threadmill.core.store.ForwardingJobStore;

class NodeRegistryTest {

  private NodeRegistry registry;

  @AfterEach
  void tearDown() {
    if (registry != null) registry.stop();
  }

  @Test
  void mastershipSelfExpiresWhenATickHangsPastTheLeaseDuration() {
    var store = new InMemoryJobStore();
    var hang = new AtomicBoolean(false);
    var hanging = new ForwardingJobStore(store) {
      @Override
      public void recordNodeHeartbeat(NodeId nodeId, Instant now) {
        if (hang.get() && now.isAfter(Instant.EPOCH)) {
          try {
            Thread.sleep(60_000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
        super.recordNodeHeartbeat(nodeId, now);
      }
    };
    registry = new NodeRegistry(
        hanging,
        NodeId.newId(),
        Duration.ofSeconds(5),
        Duration.ofMillis(50),
        Duration.ofMillis(300));

    // The first tick runs synchronously inside start() and wins the lease.
    registry.start();
    assertThat(registry.isMaster()).isTrue();

    // A store call that hangs (rather than throws) must not leave this
    // node acting as master past its local lease deadline.
    hang.set(true);
    await().atMost(Duration.ofSeconds(2)).until(() -> !registry.isMaster());
  }

  @ParameterizedTest
  @EnumSource(BlockedWrite.class)
  void stoppedRegistryCannotLeaveARenewedLeaseAfterAnInFlightTickCompletes(BlockedWrite write)
      throws Exception {
    var store = new InMemoryJobStore();
    var armed = new AtomicBoolean(false);
    var entered = new CountDownLatch(1);
    var resume = new CountDownLatch(1);
    var tickThread = new AtomicReference<Thread>();
    var delayed = new ForwardingJobStore(store) {
      @Override
      public void recordNodeHeartbeat(NodeId nodeId, Instant now) {
        if (write == BlockedWrite.HEARTBEAT && now.isAfter(Instant.EPOCH)) pause();
        super.recordNodeHeartbeat(nodeId, now);
      }

      @Override
      public boolean acquireOrRenewMaintenanceLease(NodeId nodeId, Duration duration) {
        if (write == BlockedWrite.LEASE) pause();
        return super.acquireOrRenewMaintenanceLease(nodeId, duration);
      }

      private void pause() {
        if (!armed.compareAndSet(true, false)) return;
        tickThread.set(Thread.currentThread());
        entered.countDown();
        boolean interrupted = false;
        try {
          while (true) {
            try {
              resume.await();
              return;
            } catch (InterruptedException ignored) {
              interrupted = true;
            }
          }
        } finally {
          if (interrupted) Thread.currentThread().interrupt();
        }
      }
    };
    var owner = NodeId.newId();
    registry = new NodeRegistry(
        delayed, owner, Duration.ofMinutes(1), Duration.ofMillis(20), Duration.ofSeconds(30));
    registry.start();
    armed.set(true);
    try {
      assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
      registry.stop();
    } finally {
      resume.countDown();
    }
    tickThread.get().join(Duration.ofSeconds(3));
    assertThat(tickThread.get().isAlive()).as("stopped registry loop").isFalse();
    assertThat(store.readNodeHeartbeat(owner)).contains(Instant.EPOCH);
    assertThat(store.acquireOrRenewMaintenanceLease(NodeId.newId(), Duration.ofSeconds(30)))
        .isTrue();
    assertThat(registry.isMaster()).isFalse();
  }

  enum BlockedWrite {
    HEARTBEAT,
    LEASE
  }
}
