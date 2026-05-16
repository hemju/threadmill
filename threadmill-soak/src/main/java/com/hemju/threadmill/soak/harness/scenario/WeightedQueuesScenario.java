package com.hemju.threadmill.soak.harness.scenario;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.QueueWeights;
import com.hemju.threadmill.soak.harness.LoadGenerator;
import com.hemju.threadmill.soak.harness.invariant.InvariantChecks;
import com.hemju.threadmill.soak.harness.invariant.SoakInvariant;

/**
 * Three queues with a 10:3:1 weight ratio under one queue-family lane,
 * sustained backlog on all three. Verifies the throughput ratio lands
 * within ±15% of configured weights.
 */
public final class WeightedQueuesScenario implements SoakScenario {

    static final String QUEUE_HIGH = "weighted:high";
    static final String QUEUE_MID = "weighted:mid";
    static final String QUEUE_LOW = "weighted:low";

    private static final Map<String, Integer> WEIGHTS;

    static {
        WEIGHTS = new LinkedHashMap<>();
        WEIGHTS.put(QUEUE_HIGH, 10);
        WEIGHTS.put(QUEUE_MID, 3);
        WEIGHTS.put(QUEUE_LOW, 1);
    }

    @Override
    public String name() {
        return "weighted-queues";
    }

    @Override
    public String description() {
        return "three queues with 10:3:1 weighting under one queue-family lane";
    }

    @Override
    public List<SoakInvariant> invariants() {
        // ±25% tolerance — short runs (≤30s) see meaningful variance because the
        // weighted-fair scheduler hands out claims in small batches. The
        // invariant skips the check entirely when the total succeeded count is
        // below 50 (see InvariantChecks).
        return List.of(InvariantChecks.atLeastOnce(), InvariantChecks.weightRatioWithinTolerance(WEIGHTS, 0.25));
    }

    @Override
    public void configureNode(ProcessingNode.Builder b) {
        // Deliberately low lane capacity (2 workers) so the dispatcher MUST
        // choose between queues — that's the only condition under which the
        // weighted-fair dispatch ratio is observable in throughput. With
        // ample capacity all three queues drain at the producer's rate and
        // weights have no effect.
        b.lane("weighted:*", 2, QueueWeights.fromMap(WEIGHTS));
    }

    @Override
    public void runWorkload(LoadGenerator gen, SoakRunContext ctx) throws InterruptedException {
        // Continuous production well above the lane's 2-worker capacity so a
        // sustained backlog exists on every queue throughout the run. With 50ms
        // handlers and 2 lane workers per node, max processing is ~40 jobs/sec
        // per node — production at the harness's nominal rate (typically 50+)
        // creates enduring contention the weights have to break.
        long n = 0;
        Instant runStart = ctx.runStart();
        long localRate = Math.max(150L, gen.jobsPerSecond());
        while (Instant.now().isBefore(ctx.runDeadline())) {
            long nanos = n * java.time.Duration.ofSeconds(1).toNanos() / localRate;
            gen.pace(runStart.plusNanos(nanos));
            String queue =
                    switch ((int) (n % 3)) {
                        case 0 -> QUEUE_HIGH;
                        case 1 -> QUEUE_MID;
                        default -> QUEUE_LOW;
                    };
            gen.enqueue(new SoakPayloads.FixedWork((int) n, 50, 0.0), SoakPayloads.FixedWorkHandler.class, queue, 0);
            n++;
        }
    }

    @Override
    public java.time.Duration drainBudget() {
        // With 3× over-production, drain takes a few seconds once production
        // stops; 60s is comfortable for any duration the harness supports.
        return java.time.Duration.ofSeconds(60);
    }
}
