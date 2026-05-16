package com.hemju.threadmill.soak.harness.scenario;

import java.util.List;

import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.soak.harness.LoadGenerator;
import com.hemju.threadmill.soak.harness.invariant.SoakInvariant;

/**
 * One operator-runnable workload-shape.
 *
 * <p>A scenario decides three things: how the {@link ProcessingNode}s are
 * configured (lanes, tags, timeouts), what work to produce while the run is
 * underway, and which invariants the post-run verifier must check.
 *
 * <p>Scenarios are stateless; one instance is reused for the entire run. Any
 * mutable state lives in the {@link SoakRunContext}.
 */
public interface SoakScenario {

    /** The scenario id used in {@code -Pscenario=…}. */
    String name();

    /** Short human description; goes into {@code summary.md}. */
    String description();

    /** Invariants the post-run verifier must check. */
    List<SoakInvariant> invariants();

    /**
     * Customise the {@link ProcessingNodeConfig} before nodes are built.
     * Most scenarios use the runner's defaults; long-running and retry-storm
     * scenarios override timeouts.
     */
    default ProcessingNodeConfig.Builder tuneConfig(ProcessingNodeConfig.Builder b) {
        return b;
    }

    /**
     * Customise each {@link ProcessingNode} builder before {@code build()}.
     * Used to add queue-family lanes, tags, or explicit per-queue lanes.
     */
    default void configureNode(ProcessingNode.Builder b) {}

    /**
     * Produce work for the duration of the run. The scenario may call into
     * the {@link SoakRunContext}'s pause/resume helpers for scenarios that
     * exercise those primitives.
     */
    void runWorkload(LoadGenerator gen, SoakRunContext ctx) throws Exception;

    /**
     * Maximum wait after the producer stops before the harness assumes drain
     * is impossible. Scenarios with long handlers extend this.
     */
    default java.time.Duration drainBudget() {
        return java.time.Duration.ofSeconds(45);
    }
}
