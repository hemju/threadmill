package com.hemju.threadmill.soak.harness.invariant;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Feeds every trace event into a scenario's invariant checks <em>as the run
 * happens</em>, instead of replaying the file afterwards.
 *
 * <p>This is what makes an hours-long endurance run supervisable: a definite
 * violation (one provable the moment its event arrives — see
 * {@link StreamingInvariantCheck}) fires {@code onFirstDefiniteViolation}
 * exactly once, which the harness uses for fail-fast; the progress reporter
 * polls {@link #snapshotResults()} so an operator — or an AI agent — can read
 * the current verdict from {@code progress.json} without waiting for the run
 * to end.
 *
 * <p>A check that throws is quarantined: it receives no further events and is
 * reported as failed, naming the exception — one broken checker must not
 * take down the run or mask the other checkers' findings.
 *
 * <p>Events arrive from the trace writer's emit path (already serialized by
 * its lock); {@code snapshotResults()} is called concurrently from the
 * progress thread. Each check synchronizes internally.
 */
public final class LiveInvariantVerifier {

    private final List<StreamingInvariantCheck> checks;
    private final RuntimeException[] checkFailures;
    private final Runnable onFirstDefiniteViolation;
    private final AtomicBoolean violationSignalled = new AtomicBoolean();

    public LiveInvariantVerifier(List<SoakInvariant> invariants, Runnable onFirstDefiniteViolation) {
        Objects.requireNonNull(invariants, "invariants");
        this.onFirstDefiniteViolation = Objects.requireNonNull(onFirstDefiniteViolation, "onFirstDefiniteViolation");
        this.checks = invariants.stream().map(SoakInvariant::newCheck).toList();
        this.checkFailures = new RuntimeException[checks.size()];
    }

    /** Consume the next trace event; called once per emitted line, in file order. */
    public synchronized void onEvent(TraceEvent event) {
        boolean anyViolation = false;
        for (int i = 0; i < checks.size(); i++) {
            if (checkFailures[i] != null) continue;
            StreamingInvariantCheck check = checks.get(i);
            try {
                check.onEvent(event);
            } catch (RuntimeException e) {
                checkFailures[i] = e;
                continue;
            }
            anyViolation |= check.violationCount() > 0;
        }
        // onFinish() has not run yet, so any counted violation is definite.
        if (anyViolation && violationSignalled.compareAndSet(false, true)) {
            onFirstDefiniteViolation.run();
        }
    }

    /** True once any check has counted a definite violation. */
    public boolean hasDefiniteViolation() {
        return violationSignalled.get();
    }

    /** Mid-run results: definite violations only, no completeness evaluation. */
    public List<InvariantResult> snapshotResults() {
        return results(false);
    }

    /** Final results, including completeness violations. Idempotent. */
    public List<InvariantResult> finishResults() {
        return results(true);
    }

    private synchronized List<InvariantResult> results(boolean finish) {
        var results = new ArrayList<InvariantResult>(checks.size());
        for (int i = 0; i < checks.size(); i++) {
            StreamingInvariantCheck check = checks.get(i);
            RuntimeException failure = checkFailures[i];
            if (failure != null) {
                results.add(InvariantResult.fail(
                        check.name(),
                        List.of("invariant raised " + failure.getClass().getSimpleName() + ": " + failure.getMessage()),
                        List.of()));
            } else {
                results.add(finish ? check.finish() : check.snapshot());
            }
        }
        return results;
    }
}
