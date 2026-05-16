package com.hemju.threadmill.soak.harness.scenario;

import java.time.Instant;
import java.util.List;

import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.soak.harness.LoadGenerator;
import com.hemju.threadmill.soak.harness.invariant.InvariantChecks;
import com.hemju.threadmill.soak.harness.invariant.SoakInvariant;

/**
 * 5% baseline failures, 1% wall-clock timeouts. Verifies retry counts
 * respect {@code maxAttempts} and the retry path doesn't run away.
 *
 * <p>Quarantine isn't induced here (requires unresolved-handler injection
 * which is awkward in a single-JVM run); the dedicated quarantine path is
 * exercised by {@code AbstractJobStoreContractTest}.
 */
public final class RetryStormScenario implements SoakScenario {

    private static final int MAX_ATTEMPTS = 5;

    @Override
    public String name() {
        return "retry-storm";
    }

    @Override
    public String description() {
        return "5% baseline failures + 1% wall-clock timeouts — retry-loop runaway check";
    }

    @Override
    public List<SoakInvariant> invariants() {
        return List.of(
                InvariantChecks.atLeastOnce(),
                InvariantChecks.retryBudgetRespected(MAX_ATTEMPTS),
                InvariantChecks.noLockLeaks());
    }

    @Override
    public ProcessingNodeConfig.Builder tuneConfig(ProcessingNodeConfig.Builder b) {
        return b.defaultMaxAttempts(MAX_ATTEMPTS).jobTimeout(java.time.Duration.ofSeconds(2));
    }

    @Override
    public void runWorkload(LoadGenerator gen, SoakRunContext ctx) throws InterruptedException {
        Instant runStart = ctx.runStart();
        long n = 0;
        while (Instant.now().isBefore(ctx.runDeadline())) {
            gen.pace(gen.deadlineFor(runStart, n));
            gen.enqueue(
                    new SoakPayloads.FlakyWork((int) n, 5, 0.05, 0.01, 4_000),
                    SoakPayloads.FlakyWorkHandler.class,
                    "default",
                    0);
            n++;
        }
    }

    @Override
    public java.time.Duration drainBudget() {
        // Retries add wall-clock; give the engine longer to settle.
        return java.time.Duration.ofSeconds(90);
    }
}
