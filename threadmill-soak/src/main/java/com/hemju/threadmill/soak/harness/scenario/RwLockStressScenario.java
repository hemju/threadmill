package com.hemju.threadmill.soak.harness.scenario;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.soak.harness.LoadGenerator;
import com.hemju.threadmill.soak.harness.invariant.InvariantChecks;
import com.hemju.threadmill.soak.harness.invariant.SoakInvariant;

/**
 * Single concurrency key — every job contends. 95% SHARED, 5% EXCLUSIVE.
 * Verifies strict in-group order (EXCLUSIVE never overlaps; SHARED never
 * leapfrogs an earlier-enqueued EXCLUSIVE) and that every EXCLUSIVE runs.
 */
public final class RwLockStressScenario implements SoakScenario {

    private static final String QUEUE = "default";
    private static final String KEY = "soak:rw-lock-stress";

    @Override
    public String name() {
        return "rw-lock-stress";
    }

    @Override
    public String description() {
        return "single concurrency key, 95% SHARED + 5% EXCLUSIVE — verifies strict in-group order";
    }

    @Override
    public List<SoakInvariant> invariants() {
        return List.of(
                InvariantChecks.atLeastOnce(),
                InvariantChecks.exclusivityHeld(),
                InvariantChecks.strictInGroupOrder(),
                InvariantChecks.noLockLeaks());
    }

    @Override
    public void runWorkload(LoadGenerator gen, SoakRunContext ctx) throws InterruptedException {
        Instant runStart = ctx.runStart();
        long n = 0;
        // Production rate is gated by single-key SHARED throughput AND by the
        // serialised EXCLUSIVE jobs. Cap conservatively so the engine doesn't
        // fall behind on a 4-worker node — under aggressive production with
        // 5% EXCLUSIVE jobs, the in-memory store's concurrency-group tracking
        // can fall behind enough that backlog never drains. Operators with
        // larger nodes can override via the scenario's local rate (the cap is
        // the minimum of this constant and the configured rate).
        long localRate = Math.min(25L, gen.jobsPerSecond());
        while (Instant.now().isBefore(ctx.runDeadline())) {
            long nanos = n * Duration.ofSeconds(1).toNanos() / localRate;
            gen.pace(runStart.plusNanos(nanos));
            boolean isExclusive = ThreadLocalRandom.current().nextDouble() < 0.05;
            ConcurrencyMode mode = isExclusive ? ConcurrencyMode.EXCLUSIVE : ConcurrencyMode.SHARED;
            gen.enqueue(
                    new SoakPayloads.FixedWork((int) n, isExclusive ? 25 : 8, 0.0),
                    SoakPayloads.FixedWorkHandler.class,
                    QUEUE,
                    0,
                    KEY,
                    mode);
            n++;
        }
    }

    @Override
    public Duration drainBudget() {
        // Single-key contention serializes EXCLUSIVE jobs; drain can run long
        // on a busy run. 90s is comfortably above worst-case for any
        // duration the harness supports.
        return Duration.ofSeconds(90);
    }
}
