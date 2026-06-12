package com.hemju.threadmill.soak.harness.invariant;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * One stateful, streaming instance of a named invariant for a single run.
 *
 * <p>Checks consume the trace one event at a time and must keep their state
 * <em>bounded by the amount of in-flight work</em>, never by the length of the
 * run — an endurance run feeds tens of millions of events through each check.
 *
 * <p>Two kinds of violation exist:
 *
 * <ul>
 *   <li><b>Definite</b> violations are provable the moment the offending event
 *       arrives (an EXCLUSIVE lock overlapping, a claim on a paused queue).
 *       Subclasses report these via {@link #recordViolation} from
 *       {@link #onEvent}; they are visible in {@link #snapshot()} mid-run and
 *       drive the harness's fail-fast abort.</li>
 *   <li><b>Completeness</b> violations only exist at end of run (a job that
 *       never reached a terminal event, a lock never released). Subclasses
 *       report these by overriding {@link #onFinish()}; they appear only in
 *       {@link #finish()}.</li>
 * </ul>
 *
 * <p>Recorded violation messages are capped at {@value #MAX_RECORDED_VIOLATIONS}
 * (with a trailing {@code (+N more …)} marker) and sample chains at
 * {@value #MAX_SAMPLE_CHAINS}, so a pathological run cannot exhaust memory by
 * violating an invariant millions of times.
 *
 * <p>All methods are synchronized: the live verifier feeds {@link #onEvent}
 * from the trace-writer thread while the progress reporter calls
 * {@link #snapshot()} concurrently.
 */
public abstract class StreamingInvariantCheck {

    static final int MAX_RECORDED_VIOLATIONS = 100;
    static final int MAX_SAMPLE_CHAINS = 5;

    private final String name;
    private final List<String> recordedViolations = new ArrayList<>();
    private final List<List<String>> sampleChains = new ArrayList<>();
    private long violationCount;
    private boolean finished;

    protected StreamingInvariantCheck(String name) {
        this.name = Objects.requireNonNull(name, "name");
    }

    public final String name() {
        return name;
    }

    /** Consume the next trace event. Events arrive in trace-file order. */
    public final synchronized void onEvent(TraceEvent event) {
        if (finished) return;
        observe(event);
    }

    /** Total violations seen so far, including ones past the recording cap. */
    public final synchronized long violationCount() {
        return violationCount;
    }

    /** Definite violations observed so far; safe to call mid-run. */
    public final synchronized InvariantResult snapshot() {
        return toResult();
    }

    /**
     * End-of-run result: definite violations plus completeness violations
     * contributed by {@link #onFinish()}. Idempotent — the first call runs
     * {@code onFinish()}, later calls return the same outcome.
     */
    public final synchronized InvariantResult finish() {
        if (!finished) {
            finished = true;
            onFinish();
        }
        return toResult();
    }

    /** Per-event state transition; report definite violations via {@link #recordViolation}. */
    protected abstract void observe(TraceEvent event);

    /** Hook for completeness checks; runs once, before the final result is built. */
    protected void onFinish() {}

    /**
     * Record one violation. {@code sampleChain} holds the raw trace lines that
     * prove it (may be empty); only the first {@value #MAX_SAMPLE_CHAINS}
     * chains and {@value #MAX_RECORDED_VIOLATIONS} messages are retained.
     */
    protected final void recordViolation(String message, List<String> sampleChain) {
        violationCount++;
        if (recordedViolations.size() < MAX_RECORDED_VIOLATIONS) {
            recordedViolations.add(message);
        }
        if (!sampleChain.isEmpty() && sampleChains.size() < MAX_SAMPLE_CHAINS) {
            sampleChains.add(List.copyOf(sampleChain));
        }
    }

    private InvariantResult toResult() {
        if (violationCount == 0) return InvariantResult.pass(name);
        var messages = new ArrayList<>(recordedViolations);
        long unrecorded = violationCount - recordedViolations.size();
        if (unrecorded > 0) {
            messages.add("(+" + unrecorded + " more violations not recorded — cap is " + MAX_RECORDED_VIOLATIONS + ")");
        }
        return InvariantResult.fail(name, messages, sampleChains);
    }
}
