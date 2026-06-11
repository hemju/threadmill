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
 * {@link SmartLifecycle} wrapper around a Threadmill {@link ProcessingNode} so the
 * engine starts after the application's DataSource / RedisConnectionFactory beans are
 * fully ready, and stops <em>before</em> those beans are torn down on shutdown.
 *
 * <h2>Phase choice</h2>
 *
 * <p>Spring's {@code DataSourceAutoConfiguration} registers its lifecycle objects at
 * {@code Integer.MAX_VALUE} (the default). To stop the engine before the data source
 * is closed during graceful shutdown, the engine's phase must be lower than that
 * value (Spring stops higher phases first). We use {@code Integer.MAX_VALUE / 2}
 * — high enough to start after any "infrastructure" bean that opted into the
 * default phase, low enough that the data source still wins the stop race.
 */
public final class ThreadmillLifecycle implements SmartLifecycle {

    /**
     * Phase value used by the auto-configured engine lifecycle. Picked deliberately
     * so the engine starts after the DataSource / Redis beans (which sit at the
     * default high phase) and stops before them on graceful shutdown.
     */
    public static final int PHASE = Integer.MAX_VALUE / 2;

    private static final Logger LOG = LoggerFactory.getLogger(ThreadmillLifecycle.class);

    private final ProcessingNode node;
    private volatile boolean running = false;

    public ThreadmillLifecycle(ProcessingNode node) {
        this.node = Objects.requireNonNull(node, "node");
    }

    @Override
    public void start() {
        if (running) return;
        // A ProcessingNode is not restartable: once close()d, start() is a silent
        // no-op. Re-running this lifecycle after a stop (e.g. actuator /pause then
        // /resume, which does context stop()/start()) would log a "started"
        // banner over a permanently dead engine — a silent total processing
        // outage. Fail loudly instead. Because this lifecycle's phase is below
        // the remote-wake lifecycle's, throwing here also prevents that bean from
        // re-subscribing (and leaking) its listener on the same broken restart.
        if (node.isStopped()) {
            throw new IllegalStateException("Threadmill ProcessingNode " + node.nodeId()
                    + " has been stopped and cannot be restarted in place. A SmartLifecycle stop()/start()"
                    + " cycle (e.g. actuator /pause then /resume) is not supported; restart the application"
                    + " context instead.");
        }
        node.start();
        running = true;
        LOG.info("\n{}", renderBanner(node));
    }

    @Override
    public void stop() {
        if (!running) return;
        try {
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
