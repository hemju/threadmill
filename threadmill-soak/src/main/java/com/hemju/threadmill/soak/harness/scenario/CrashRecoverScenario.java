package com.hemju.threadmill.soak.harness.scenario;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.soak.harness.LoadGenerator;
import com.hemju.threadmill.soak.harness.invariant.InvariantChecks;
import com.hemju.threadmill.soak.harness.invariant.SoakInvariant;

/**
 * Calls {@link ProcessingNode#close()} on one of the running nodes mid-run.
 * The remaining nodes pick up in-flight jobs through orphan reclaim. The
 * brief specifies graceful {@code close()} (not a hard interrupt or process
 * kill) — that's the path the orphan-recovery code is designed for.
 */
public final class CrashRecoverScenario implements SoakScenario {

    @Override
    public String name() {
        return "crash-recover";
    }

    @Override
    public String description() {
        return "close one ProcessingNode mid-run — orphan reclaim picks up its work";
    }

    @Override
    public List<SoakInvariant> invariants() {
        return List.of(InvariantChecks.atLeastOnce(), InvariantChecks.noLockLeaks());
    }

    @Override
    public boolean supportsConcurrentProducers() {
        // Each workload invocation closes one node mid-run; N producers would
        // close N nodes and change what the scenario tests.
        return false;
    }

    @Override
    public void runWorkload(LoadGenerator gen, SoakRunContext ctx) throws InterruptedException {
        Instant runStart = ctx.runStart();
        Duration total = ctx.duration();
        Instant closeAt = runStart.plus(total.dividedBy(3));
        long n = 0;
        boolean closed = false;
        while (Instant.now().isBefore(ctx.runDeadline())) {
            gen.pace(gen.deadlineFor(runStart, n));
            if (!closed && Instant.now().isAfter(closeAt)) {
                List<ProcessingNode> nodes = ctx.nodes();
                if (nodes.size() >= 2) {
                    ProcessingNode victim = nodes.get(nodes.size() - 1);
                    Map<String, Object> fields = new LinkedHashMap<>();
                    fields.put("nodeId", victim.nodeId().toString());
                    fields.put("reason", "crash-recover scenario close()");
                    ctx.trace().emit("node_closing_mid_run", fields);
                    // Block while the worker pool drains gracefully — the engine's
                    // close() awaits its own shutdownGracePeriod.
                    victim.close();
                    closed = true;
                }
            }
            gen.enqueue(
                    new SoakPayloads.FixedWork((int) n, 12, 0.0), SoakPayloads.FixedWorkHandler.class, "default", 0);
            n++;
        }
    }

    @Override
    public Duration drainBudget() {
        return Duration.ofSeconds(90);
    }
}
