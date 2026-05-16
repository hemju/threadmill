package com.hemju.threadmill.soak.harness.invariant;

import java.util.List;

/**
 * Outcome of one invariant check. {@code violations} is a flat list of
 * human-readable messages; {@code sampleChains} (capped at five) holds the
 * raw JSON events that proved the violation, so an AI agent can trace the
 * exact lifecycle that broke it.
 */
public record InvariantResult(String name, boolean passed, List<String> violations, List<List<String>> sampleChains) {

    public InvariantResult {
        violations = List.copyOf(violations);
        sampleChains = List.copyOf(sampleChains);
    }

    public static InvariantResult pass(String name) {
        return new InvariantResult(name, true, List.of(), List.of());
    }

    public static InvariantResult fail(String name, List<String> violations, List<List<String>> sampleChains) {
        return new InvariantResult(name, false, violations, sampleChains);
    }
}
