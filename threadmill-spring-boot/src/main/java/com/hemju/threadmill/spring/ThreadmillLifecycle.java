package com.hemju.threadmill.spring;

import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.engine.QueueLane;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.core.store.JobStoreCapabilities;

/**
 * {@link SmartLifecycle} wrapper around a Threadmill {@link ProcessingNode}. The
 * engine starts in Spring's latest lifecycle phase and stops in the earliest shutdown
 * phase. Remote-wake subscriptions, when present, are owned by this lifecycle so their
 * ordering relative to the node is deterministic.
 *
 * <h2>Phase choice</h2>
 *
 * <p>Spring starts lower phases first and stops higher phases first. The default
 * {@link SmartLifecycle} phase ({@link Integer#MAX_VALUE}) therefore gives Threadmill
 * the desired edge: start after lower-phase application infrastructure and stop before
 * it. Store beans and their connections are constructed before lifecycle startup.
 */
public final class ThreadmillLifecycle implements SmartLifecycle {

    /**
     * Phase value used by the auto-configured engine lifecycle. Spring starts lower
     * phases first and stops higher phases first, so the default maximum phase starts
     * Threadmill as late as possible and stops it as early as possible.
     */
    public static final int PHASE = SmartLifecycle.DEFAULT_PHASE;

    private static final Logger LOG = LoggerFactory.getLogger(ThreadmillLifecycle.class);

    private final ProcessingNode node;
    private final ThreadmillRemoteWakeChannels remoteWakeChannels;
    private volatile boolean running = false;

    public ThreadmillLifecycle(ProcessingNode node) {
        this(node, null);
    }

    ThreadmillLifecycle(ProcessingNode node, ThreadmillRemoteWakeChannels remoteWakeChannels) {
        this.node = Objects.requireNonNull(node, "node");
        this.remoteWakeChannels = remoteWakeChannels;
    }

    @Override
    public void start() {
        if (running) return;
        // A ProcessingNode is not restartable: once close()d, start() is a silent
        // no-op. Re-running this lifecycle after a stop (e.g. actuator /pause then
        // /resume, which does context stop()/start()) would log a "started"
        // banner over a permanently dead engine — a silent total processing
        // outage. Fail loudly instead.
        if (node.isStopped()) {
            throw new IllegalStateException("Threadmill ProcessingNode " + node.nodeId()
                    + " has been stopped and cannot be restarted in place. A SmartLifecycle stop()/start()"
                    + " cycle (e.g. actuator /pause then /resume) is not supported; restart the application"
                    + " context instead.");
        }
        node.start();
        if (remoteWakeChannels != null) {
            remoteWakeChannels.start(node::wake);
        }
        running = true;
        LOG.info("\n{}", renderBanner(node));
    }

    @Override
    public void stop() {
        if (!running) return;
        try {
            if (remoteWakeChannels != null) {
                remoteWakeChannels.close();
            }
            node.close();
        } finally {
            running = false;
            LOG.info("Threadmill: ProcessingNode {} stopped", node.nodeId());
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }

    /** Expose the wrapped node so other auto-configured beans (Actuator, etc.) can read it. */
    public ProcessingNode node() {
        return node;
    }

    /**
     * Build the Quartz-inspired startup banner. Package-private so tests can
     * assert against the exact formatted string without spinning up SLF4J
     * capture every time.
     */
    static String renderBanner(ProcessingNode node) {
        JobStore store = node.store();
        ProcessingNodeConfig config = node.config();
        JobStoreCapabilities caps = store.capabilities();
        int totalWorkers = node.lanes().stream().mapToInt(QueueLane::workers).sum();
        String lanesLine = node.lanes().stream()
                .map(lane -> lane.queue() + " x" + lane.workers())
                .collect(Collectors.joining("   "));

        var b = new StringBuilder(512);
        b.append("┌─ Threadmill engine started ───────────────────────────────────────────\n");
        b.append("│  Node id      : ")
                .append(node.nodeId())
                .append("   (lifecycle phase ")
                .append(PHASE)
                .append(")\n");
        b.append("│  Store        : ").append(store.describe()).append('\n');
        b.append("│                 capabilities: maxJob=")
                .append(caps.maxSerializedJobBytes())
                .append("B, maxLog=")
                .append(caps.maxJobLogBytes())
                .append("B, exactCounts=")
                .append(caps.supportsExactCounts())
                .append(", concurrencyGroups=")
                .append(caps.supportsConcurrencyGroups())
                .append('\n');
        b.append("│  Workers      : ")
                .append(totalWorkers)
                .append(" across ")
                .append(node.lanes().size())
                .append(" lane(s)\n");
        b.append("│                 ").append(lanesLine).append('\n');
        b.append("│  Polling      : poll=")
                .append(config.pollInterval())
                .append(", claimBatch=")
                .append(config.claimBatchSize())
                .append(", jobTimeout=")
                .append(config.jobTimeout())
                .append('\n');
        b.append("│  Maintenance  : poll=")
                .append(config.maintenancePollInterval())
                .append(", heartbeat=")
                .append(config.claimHeartbeat())
                .append(", retention=")
                .append(config.retentionInterval())
                .append('\n');
        b.append("│  Master lease : ")
                .append(config.maintenanceLeaseDuration())
                .append("  (this node is master-eligible)\n");
        b.append("└───────────────────────────────────────────────────────────────────────");
        return b.toString();
    }
}
