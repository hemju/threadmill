package com.hemju.threadmill.soak.harness.scenario;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.QueueWeights;
import com.hemju.threadmill.soak.harness.LoadGenerator;
import com.hemju.threadmill.soak.harness.invariant.InvariantChecks;
import com.hemju.threadmill.soak.harness.invariant.SoakInvariant;

/**
 * 20 simulated resources, 10% EXCLUSIVE "import"-shaped (longer) and
 * 90% SHARED "export"-shaped (shorter), all under one queue-family lane
 * with uniform weights. The default scenario — exercises the whole
 * concurrency primitive end to end under load.
 */
public final class MixedWorkloadScenario implements SoakScenario {

    private static final int RESOURCE_COUNT = 20;

    @Override
    public String name() {
        return "mixed-workload";
    }

    @Override
    public String description() {
        return "20 resources × EXCLUSIVE imports (10%) + SHARED exports (90%) over a queue-family lane";
    }

    @Override
    public List<SoakInvariant> invariants() {
        return List.of(
                InvariantChecks.atLeastOnce(),
                InvariantChecks.exclusivityHeld(),
                InvariantChecks.strictInGroupOrder(),
                InvariantChecks.noLockLeaks(),
                InvariantChecks.retryBudgetRespected(5));
    }

    @Override
    public void configureNode(ProcessingNode.Builder b) {
        b.lane("project:*", 8, QueueWeights.uniform());
    }

    @Override
    public java.time.Duration drainBudget() {
        // Multi-key + EXCLUSIVE imports can serialize chunks of work; 60s
        // covers any duration the harness supports.
        return java.time.Duration.ofSeconds(60);
    }

    @Override
    public void runWorkload(LoadGenerator gen, SoakRunContext ctx) throws InterruptedException {
        Instant runStart = ctx.runStart();
        long n = 0;
        while (Instant.now().isBefore(ctx.runDeadline())) {
            gen.pace(gen.deadlineFor(runStart, n));
            int resource = ThreadLocalRandom.current().nextInt(RESOURCE_COUNT);
            String queue = "project:" + resource;
            String key = "project:" + resource;
            boolean isImport = ThreadLocalRandom.current().nextDouble() < 0.10;
            if (isImport) {
                gen.enqueue(
                        new SoakPayloads.FixedWork((int) n, 30, 0.0),
                        SoakPayloads.FixedWorkHandler.class,
                        queue,
                        0,
                        key,
                        ConcurrencyMode.EXCLUSIVE);
            } else {
                gen.enqueue(
                        new SoakPayloads.FixedWork((int) n, 8, 0.0),
                        SoakPayloads.FixedWorkHandler.class,
                        queue,
                        0,
                        key,
                        ConcurrencyMode.SHARED);
            }
            n++;
        }
    }
}
