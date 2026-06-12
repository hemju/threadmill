package com.hemju.threadmill.soak.harness.invariant;

/**
 * One named correctness check a scenario registers for its runs.
 *
 * <p>The invariant itself is a stateless definition; {@link #newCheck()}
 * produces the stateful {@link StreamingInvariantCheck} that consumes one
 * run's trace events. The harness verifies <em>live</em> — every event is fed
 * to the checks as it is written — so checks must hold bounded state (see the
 * {@link StreamingInvariantCheck} contract) and may be snapshotted mid-run for
 * progress reporting and fail-fast.
 */
public interface SoakInvariant {

    /** Stable id, used in {@code summary.json}. */
    String name();

    /** Short human description; surfaced in {@code summary.md}. */
    String description();

    /** A fresh stateful check for one run's event stream. */
    StreamingInvariantCheck newCheck();
}
