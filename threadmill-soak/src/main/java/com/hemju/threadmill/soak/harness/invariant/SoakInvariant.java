package com.hemju.threadmill.soak.harness.invariant;

/**
 * One named, post-run correctness check.
 *
 * <p>Invariants run after the load generator has stopped and the engine has
 * drained (or hit the drain budget). They read the {@link TraceCorpus} and
 * produce an {@link InvariantResult} naming any violations and a handful of
 * sample event chains that illustrate the failure.
 */
public interface SoakInvariant {

    /** Stable id, used in {@code summary.json}. */
    String name();

    /** Short human description; surfaced in {@code summary.md}. */
    String description();

    /** Check the invariant against the loaded trace. */
    InvariantResult check(TraceCorpus corpus);
}
