package com.hemju.threadmill.soak.harness;

import java.util.LinkedHashMap;
import java.util.Objects;

import com.hemju.threadmill.core.handler.JobExecutionContext;

/**
 * Static bridge that lets the soak handlers emit {@code exec_started} /
 * {@code exec_finished} events from <em>inside</em> {@code run(...)}.
 *
 * <p>Why these events exist: every {@link com.hemju.threadmill.core.engine.JobInterceptor}
 * hook fires only <em>after</em> the store transition it describes has
 * committed, so interceptor-emitted lock events lag store truth by scheduling
 * jitter. A legal per-key handoff between two workers can therefore appear in
 * the trace as a microsecond overlap — the exact false positive that aborted
 * the first real endurance run. Events emitted synchronously inside the
 * handler have the opposite property: they are written <em>while the handler
 * is executing</em>, and the trace writer's lock totally orders them, so an
 * observed bracket overlap is a real execution overlap by construction. The
 * execution-level invariants ({@code exclusivityHeld},
 * {@code strictInGroupOrder}) judge these brackets; the interceptor lock
 * events remain for {@code lock-events.jsonl} and contention statistics.
 *
 * <p>The sink is installed by the runner for the duration of one run and
 * cleared afterwards; handlers are resolved reflectively, so a static holder
 * is the only injection point. Runs are one-per-JVM (the endurance
 * orchestrator gives each backend its own process), and the harness's JUnit
 * smokes run sequentially. When no sink is installed the emitters are no-ops,
 * so handlers stay usable outside a harness run.
 */
public final class SoakExecutionTrace {

    private static volatile SoakTraceWriter writer;

    private SoakExecutionTrace() {}

    /** Install the run's trace writer; called by the runner before nodes start. */
    public static void install(SoakTraceWriter traceWriter) {
        writer = Objects.requireNonNull(traceWriter, "traceWriter");
    }

    /** Remove the sink; called by the runner after the run's trace is closed. */
    public static void clear() {
        writer = null;
    }

    /** First statement of every soak handler's {@code run(...)}. */
    public static void started(JobExecutionContext ctx) {
        emit("exec_started", ctx);
    }

    /** Emitted in a {@code finally} so a throwing or interrupted handler still closes its bracket. */
    public static void finished(JobExecutionContext ctx) {
        emit("exec_finished", ctx);
    }

    private static void emit(String event, JobExecutionContext ctx) {
        SoakTraceWriter w = writer;
        if (w == null || ctx == null) return;
        var fields = new LinkedHashMap<String, Object>();
        fields.put("jobId", ctx.jobId().toString());
        fields.put("attempt", ctx.attempt());
        w.emit(event, fields);
    }
}
