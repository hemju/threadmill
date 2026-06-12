package com.hemju.threadmill.soak.harness;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.engine.ProcessingNode;

/**
 * Periodic close-and-replace of one {@link ProcessingNode}, composable with
 * any scenario via {@code -PnodeChurn=<interval>}.
 *
 * <p>Steady nodes for hours prove little more than steady nodes for minutes;
 * the cluster behaviour that hour-scale runs are meant to expose lives in the
 * <em>transitions</em>: maintenance-lease handover and master re-election,
 * node-registry heartbeat cleanup, in-flight handlers interrupted past the
 * shutdown grace and retried on surviving nodes, and queue-family rediscovery
 * on the replacement node. Each cycle closes the oldest node (graceful
 * {@code close()} — the in-JVM analogue; hard process kills are the
 * worker-churn simulation's job) and starts a fresh replacement, so the node
 * count is preserved and at least one node is always alive.
 *
 * <p>Trace vocabulary: {@code node_churn_stop} before the close, then the
 * usual {@code node_stopped} / {@code node_started} pair with
 * {@code reason="churn"} / {@code reason="churn-replacement"}.
 */
final class NodeChurner {

    private static final Logger LOG = LoggerFactory.getLogger(NodeChurner.class);

    private final Thread thread;
    private volatile boolean stopped;

    private NodeChurner(
            Duration interval,
            List<ProcessingNode> nodes,
            SoakTraceWriter trace,
            AtomicBoolean abortRequested,
            Supplier<ProcessingNode> replacementStarter) {
        this.thread = Thread.ofVirtual().name("soak-node-churner").unstarted(() -> {
            while (!stopped) {
                try {
                    Thread.sleep(interval.toMillis());
                } catch (InterruptedException e) {
                    return;
                }
                if (stopped || abortRequested.get()) return;
                try {
                    churnOnce(nodes, trace, replacementStarter);
                } catch (RuntimeException e) {
                    LOG.warn("node churn cycle failed: {}", e.toString());
                }
            }
        });
    }

    static NodeChurner start(
            Duration interval,
            List<ProcessingNode> nodes,
            SoakTraceWriter trace,
            AtomicBoolean abortRequested,
            Supplier<ProcessingNode> replacementStarter) {
        Objects.requireNonNull(interval, "interval");
        var churner = new NodeChurner(interval, nodes, trace, abortRequested, replacementStarter);
        churner.thread.start();
        return churner;
    }

    void stop() {
        stopped = true;
        thread.interrupt();
        try {
            // The thread may be mid-close() on a node draining its grace
            // period; give it a moment, but never hold up the run teardown.
            thread.join(Duration.ofSeconds(30));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void churnOnce(
            List<ProcessingNode> nodes, SoakTraceWriter trace, Supplier<ProcessingNode> replacementStarter) {
        ProcessingNode victim;
        synchronized (nodes) {
            if (nodes.size() < 2) return; // never churn the last node
            victim = nodes.removeFirst();
        }
        trace.emit("node_churn_stop", Map.of("nodeId", victim.nodeId().toString()));
        victim.close();
        trace.emit("node_stopped", Map.of("nodeId", victim.nodeId().toString(), "reason", "churn"));
        replacementStarter.get();
    }
}
