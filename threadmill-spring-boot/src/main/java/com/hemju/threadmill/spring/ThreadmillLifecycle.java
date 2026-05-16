package com.hemju.threadmill.spring;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import com.hemju.threadmill.core.engine.ProcessingNode;

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
        node.start();
        running = true;
        LOG.info("Threadmill: ProcessingNode {} started (lifecycle phase {})", node.nodeId(), PHASE);
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
}
