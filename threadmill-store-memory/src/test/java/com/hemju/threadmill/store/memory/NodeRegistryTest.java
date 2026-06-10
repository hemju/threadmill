package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.NodeRegistry;
import com.hemju.threadmill.test.ForwardingJobStore;

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
                if (hang.get()) {
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
                hanging, NodeId.newId(), Duration.ofSeconds(5), Duration.ofMillis(50), Duration.ofMillis(300));

        // The first tick runs synchronously inside start() and wins the lease.
        registry.start();
        assertThat(registry.isMaster()).isTrue();

        // A store call that hangs (rather than throws) must not leave this
        // node acting as master past its local lease deadline.
        hang.set(true);
        await().atMost(Duration.ofSeconds(2)).until(() -> !registry.isMaster());
    }
}
