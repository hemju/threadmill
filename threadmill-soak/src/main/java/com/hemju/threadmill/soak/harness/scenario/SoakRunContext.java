package com.hemju.threadmill.soak.harness.scenario;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
 */
public final class SoakRunContext {

    private final SoakHarnessConfig config;
    private final JobStore store;
    private final SoakTraceWriter trace;
    private final Instant runStart;
    private final Supplier<List<ProcessingNode>> nodesSupplier;

    public SoakRunContext(
            SoakHarnessConfig config,
            JobStore store,
            SoakTraceWriter trace,
            Instant runStart,
            Supplier<List<ProcessingNode>> nodesSupplier) {
        this.config = Objects.requireNonNull(config, "config");
        this.store = Objects.requireNonNull(store, "store");
        this.trace = Objects.requireNonNull(trace, "trace");
        this.runStart = Objects.requireNonNull(runStart, "runStart");
        this.nodesSupplier = Objects.requireNonNull(nodesSupplier, "nodesSupplier");
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

    public Instant runDeadline() {
        return runStart.plus(config.duration());
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
