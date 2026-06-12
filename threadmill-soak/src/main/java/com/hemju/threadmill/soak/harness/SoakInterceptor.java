package com.hemju.threadmill.soak.harness;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.engine.JobInterceptor;
import com.hemju.threadmill.core.handler.JobExecutionContext;

/**
 * Lifecycle interceptor for the soak harness.
 *
 * <p>The engine fires {@code onProcessingStarting} once per attempt, just
 * before the handler runs — this is the only signal we get that a claim
 * succeeded. Claim and start are emitted from the same hook (the engine has
 * no separate "claimed" interceptor callback; claim happens in the
 * dispatcher, not the runner). Latency-tracker hooks record the timestamp
 * for both stages from the same moment.
 *
 * <p>Failures funnel through one path:
 *
 * <ul>
 *   <li>{@code FailureCause.TIMEOUT} → {@code timed_out}</li>
 *   <li>{@code FailureCause.QUARANTINE} → {@code quarantined}</li>
 *   <li>Anything else → {@code failed}</li>
 * </ul>
 *
 * <p>Lock released is emitted on every terminal hook (success or failure of
 * any cause) so the lock-pairing invariant holds.
 */
public final class SoakInterceptor implements JobInterceptor {

    private final SoakTraceWriter trace;
    private final LatencyTracker latencyTracker;
    private final ConcurrentHashMap<String, Integer> attemptsByJob = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> succeededByQueue = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> succeededByHandler = new ConcurrentHashMap<>();
    private final AtomicLong succeededCount = new AtomicLong();
    private final AtomicLong failedCount = new AtomicLong();
    private final AtomicLong timedOutCount = new AtomicLong();
    private final AtomicLong quarantinedCount = new AtomicLong();
    private final AtomicLong retriedCount = new AtomicLong();

    public SoakInterceptor(SoakTraceWriter trace, LatencyTracker latencyTracker) {
        this.trace = Objects.requireNonNull(trace, "trace");
        this.latencyTracker = Objects.requireNonNull(latencyTracker, "latencyTracker");
    }

    @Override
    public void onProcessingStarting(Job job, JobExecutionContext ctx) {
        int attempt = attemptsByJob.merge(job.id().toString(), 1, Integer::sum);
        // Claim and start are the engine's "starting an attempt" event — we
        // emit both names so AI agents grep for either term and so the JSON
        // shape carries the lifecycle vocabulary the brief documents.
        var claimedFields = new LinkedHashMap<String, Object>();
        claimedFields.put("jobId", job.id().toString());
        claimedFields.put("queue", job.queue());
        claimedFields.put("attempt", attempt);
        trace.emit("claimed", claimedFields);
        latencyTracker.recordClaimed(job.id());
        var startedFields = new LinkedHashMap<>(claimedFields);
        trace.emit("started", startedFields);
        latencyTracker.recordStarted(job.id());
        job.concurrencyKey().ifPresent(key -> {
            var lockFields = new LinkedHashMap<String, Object>();
            lockFields.put("jobId", job.id().toString());
            lockFields.put("lockKey", key);
            lockFields.put("lockMode", job.concurrencyMode().map(Enum::name).orElse(""));
            trace.emit("lock_acquired", lockFields);
        });
    }

    @Override
    public void onProcessingSucceeded(Job job, JobExecutionContext ctx) {
        succeededCount.incrementAndGet();
        succeededByQueue.computeIfAbsent(job.queue(), q -> new LongAdder()).increment();
        succeededByHandler
                .computeIfAbsent(handlerSimpleName(job.spec().handlerType()), h -> new LongAdder())
                .increment();
        // Terminal: drop the per-job entry so the map stays bounded by
        // in-flight work across an hours-long run.
        Integer tracked = attemptsByJob.remove(job.id().toString());
        int attempts = tracked == null ? 0 : tracked;
        var fields = new LinkedHashMap<String, Object>();
        fields.put("jobId", job.id().toString());
        fields.put("queue", job.queue());
        fields.put("attempts", attempts);
        fields.put("final", true);
        trace.emit("succeeded", fields);
        emitLockReleased(job, attempts);
        latencyTracker.recordCompleted(job.id(), attempts, "SUCCEEDED");
    }

    @Override
    public void onProcessingFailed(Job job, JobExecutionContext ctx, Throwable cause, FailureCause causeKind) {
        boolean terminal = job.currentState() != JobState.SCHEDULED;
        // A non-final failure keeps its entry — the retry attempt needs the
        // running count; a terminal one is dropped to keep the map bounded.
        Integer tracked = terminal
                ? attemptsByJob.remove(job.id().toString())
                : attemptsByJob.get(job.id().toString());
        int attempts = tracked == null ? 0 : tracked;
        var fields = new LinkedHashMap<String, Object>();
        fields.put("jobId", job.id().toString());
        fields.put("queue", job.queue());
        fields.put("attempts", attempts);
        fields.put("causeKind", causeKind.name());
        fields.put("causeMessage", cause == null ? null : truncate(cause.getMessage()));
        fields.put("final", terminal);
        String event =
                switch (causeKind) {
                    case TIMEOUT -> "timed_out";
                    case QUARANTINE -> "quarantined";
                    default -> "failed";
                };
        switch (event) {
            case "timed_out" -> timedOutCount.incrementAndGet();
            case "quarantined" -> quarantinedCount.incrementAndGet();
            default -> failedCount.incrementAndGet();
        }
        trace.emit(event, fields);
        emitLockReleased(job, attempts);
        if (!terminal) {
            // A non-final failure means RetryInterceptor will schedule another attempt.
            retriedCount.incrementAndGet();
            var retryFields = new LinkedHashMap<String, Object>();
            retryFields.put("jobId", job.id().toString());
            retryFields.put("queue", job.queue());
            retryFields.put("attempts", attempts);
            trace.emit("retried", retryFields);
        } else {
            latencyTracker.recordCompleted(
                    job.id(), attempts, causeKind == FailureCause.QUARANTINE ? "QUARANTINED" : "FAILED");
        }
    }

    private void emitLockReleased(Job job, int attempts) {
        // A claim whose handler never started (node churned away between the
        // store-level claim and onProcessingStarting, then orphan-reclaimed;
        // or a quarantine at claim time) traced no lock_acquired — emitting a
        // release would record a bracket that never opened. attempts is only
        // incremented by onProcessingStarting, which precedes every
        // lock_acquired, so attempts == 0 means exactly that shape.
        if (attempts == 0) return;
        job.concurrencyKey().ifPresent(key -> {
            var lockFields = new LinkedHashMap<String, Object>();
            lockFields.put("jobId", job.id().toString());
            lockFields.put("lockKey", key);
            lockFields.put("lockMode", job.concurrencyMode().map(Enum::name).orElse(""));
            trace.emit("lock_released", lockFields);
        });
    }

    private static String truncate(String s) {
        if (s == null) return null;
        return s.length() > 160 ? s.substring(0, 160) + "…" : s;
    }

    /** Matches the simple name the load generator records on {@code enqueued} events. */
    private static String handlerSimpleName(String handlerType) {
        int cut = Math.max(handlerType.lastIndexOf('.'), handlerType.lastIndexOf('$'));
        return cut < 0 ? handlerType : handlerType.substring(cut + 1);
    }

    /** Succeeded counts per queue; snapshot for the run summary. */
    public Map<String, Long> succeededByQueue() {
        var snapshot = new LinkedHashMap<String, Long>();
        succeededByQueue.forEach((queue, count) -> snapshot.put(queue, count.sum()));
        return snapshot;
    }

    /** Succeeded counts per handler simple name; snapshot for the run summary. */
    public Map<String, Long> succeededByHandler() {
        var snapshot = new LinkedHashMap<String, Long>();
        succeededByHandler.forEach((handler, count) -> snapshot.put(handler, count.sum()));
        return snapshot;
    }

    public long succeeded() {
        return succeededCount.get();
    }

    public long failed() {
        return failedCount.get();
    }

    public long timedOut() {
        return timedOutCount.get();
    }

    public long quarantined() {
        return quarantinedCount.get();
    }

    public long retried() {
        return retriedCount.get();
    }
}
