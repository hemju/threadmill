package com.hemju.threadmill.soak.harness.scenario;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.soak.harness.SoakHarnessConfig;
import com.hemju.threadmill.soak.harness.SoakTraceWriter;

/**
 * Per-run context handed to a {@link SoakScenario}'s
 * {@code runWorkload(...)} method.
 *
 * <p>Exposes the run's effective duration, the trace writer (for scenarios
 * that emit their own events such as crash-recover), the live store, and a
 * read-only handle on the active processing nodes (so the crash-recover
 * scenario can close one of them mid-run).
 *
 * <p>The harness can abort a run early (fail-fast on a definite invariant
 * violation). Abort is expressed through {@link #runDeadline()} so every
 * scenario's {@code while (Instant.now().isBefore(ctx.runDeadline()))}
 * producer loop exits on its next iteration without scenario changes.
 */
public final class SoakRunContext {

    private final SoakHarnessConfig config;
    private final JobStore store;
    private final SoakTraceWriter trace;
    private final Instant runStart;
    private final Supplier<List<ProcessingNode>> nodesSupplier;
    private final AtomicBoolean abortRequested;

    public SoakRunContext(
            SoakHarnessConfig config,
            JobStore store,
            SoakTraceWriter trace,
            Instant runStart,
            Supplier<List<ProcessingNode>> nodesSupplier) {
        this(config, store, trace, runStart, nodesSupplier, new AtomicBoolean());
    }

    public SoakRunContext(
            SoakHarnessConfig config,
            JobStore store,
            SoakTraceWriter trace,
            Instant runStart,
            Supplier<List<ProcessingNode>> nodesSupplier,
            AtomicBoolean abortRequested) {
        this.config = Objects.requireNonNull(config, "config");
        this.store = Objects.requireNonNull(store, "store");
        this.trace = Objects.requireNonNull(trace, "trace");
        this.runStart = Objects.requireNonNull(runStart, "runStart");
        this.nodesSupplier = Objects.requireNonNull(nodesSupplier, "nodesSupplier");
        this.abortRequested = Objects.requireNonNull(abortRequested, "abortRequested");
    }

    public SoakHarnessConfig config() {
        return config;
    }

    public JobStore store() {
        return store;
    }

    public SoakTraceWriter trace() {
        return trace;
    }

    public Duration duration() {
        return config.duration();
    }

    public Instant runStart() {
        return runStart;
    }

    /**
     * The instant the producer loop should stop. Once an abort is requested
     * this returns {@link Instant#MIN}, so the loop's next
     * {@code Instant.now().isBefore(...)} check exits immediately.
     */
    public Instant runDeadline() {
        if (abortRequested.get()) return Instant.MIN;
        return runStart.plus(config.duration());
    }

    /** True once the harness has requested an early abort (fail-fast). */
    public boolean aborted() {
        return abortRequested.get();
    }

    public List<ProcessingNode> nodes() {
        return nodesSupplier.get();
    }

    /** Pause a queue at the store level and emit a {@code queue_paused} trace event. */
    public void pauseQueue(String queue, String reason) {
        store.pauseQueue(queue, reason);
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("queue", queue);
        fields.put("reason", reason);
        trace.emit("queue_paused", fields);
    }

    /** Resume a queue at the store level and emit a {@code queue_resumed} trace event. */
    public void resumeQueue(String queue) {
        store.resumeQueue(queue);
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("queue", queue);
        trace.emit("queue_resumed", fields);
    }
}
