package com.hemju.threadmill.soak.harness.scenario;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.soak.harness.LoadGenerator;
import com.hemju.threadmill.soak.harness.invariant.InvariantChecks;
import com.hemju.threadmill.soak.harness.invariant.SoakInvariant;

/**
 * Mixes long-running handlers that check in regularly with stalled ones that
 * never check in. Verifies the engine's no-progress-timeout kills the
 * stalled jobs while the checked-in jobs run to completion.
 *
 * <p>The configured {@code noProgressTimeout} is short enough to fire well
 * inside the run budget; {@code jobTimeout} is long enough that the
 * wall-clock path doesn't fire instead.
 */
public final class LongRunningScenario implements SoakScenario {

    private static final Duration NO_PROGRESS = Duration.ofSeconds(2);
    private static final Duration JOB_TIMEOUT = Duration.ofSeconds(60);
    private static final long CHECKIN_INTERVAL_MS = 600;
    private static final long CHECKIN_RUN_MS = 3_500;
    private static final long STALL_MS = 8_000;

    @Override
    public String name() {
        return "long-running";
    }

    @Override
    public String description() {
        return "long handlers with periodic check-ins; some stalled — no-progress timeout kill check";
    }

    @Override
    public List<SoakInvariant> invariants() {
        return List.of(
                InvariantChecks.atLeastOnce(), InvariantChecks.noLockLeaks(), InvariantChecks.noProgressTimeoutKills());
    }

    @Override
    public ProcessingNodeConfig.Builder tuneConfig(ProcessingNodeConfig.Builder b) {
        return b.jobTimeout(JOB_TIMEOUT).noProgressTimeout(NO_PROGRESS).defaultMaxAttempts(1);
    }

    @Override
    public void runWorkload(LoadGenerator gen, SoakRunContext ctx) throws InterruptedException {
        Instant runStart = ctx.runStart();
        long n = 0;
        // Lower the production rate — long-running handlers fill workers fast.
        // The drain budget compensates for this.
        long localRate = Math.max(1L, gen.jobsPerSecond() / 20L);
        while (Instant.now().isBefore(ctx.runDeadline())) {
            long nanos = n * Duration.ofSeconds(1).toNanos() / localRate;
            Instant deadline = runStart.plusNanos(nanos);
            gen.pace(deadline);
            boolean stalled = (n % 4) == 0;
            if (stalled) {
                gen.enqueue(
                        new SoakPayloads.StalledWork((int) n, STALL_MS),
                        SoakPayloads.StalledWorkHandler.class,
                        "default",
                        0);
            } else {
                gen.enqueue(
                        new SoakPayloads.CheckingInWork((int) n, CHECKIN_RUN_MS, CHECKIN_INTERVAL_MS),
                        SoakPayloads.CheckingInWorkHandler.class,
                        "default",
                        0);
            }
            n++;
        }
    }

    @Override
    public Duration drainBudget() {
        // Long-running handlers + stalled-job timeout reclaim need a wide budget.
        return Duration.ofSeconds(90);
    }
}
