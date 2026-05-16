package com.hemju.threadmill.simulation;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.engine.JobInterceptor;
import com.hemju.threadmill.core.handler.JobExecutionContext;

/**
 * Engine interceptor that emits trace events the verifier later reads.
 *
 * <p>Maps lifecycle hooks to events:
 *
 * <ul>
 *   <li>{@code onProcessingStarting} → {@code claimed} + {@code lock_acquired}
 *       (if the job carries a concurrency key)</li>
 *   <li>{@code onProcessingSucceeded} → {@code succeeded} + {@code lock_released}</li>
 *   <li>{@code onProcessingFailed} → {@code failed} (or {@code timed_out} /
 *       {@code quarantined} based on {@code FailureCause}) + {@code lock_released}</li>
 * </ul>
 */
public final class SimulationInterceptor implements JobInterceptor {

    private final TraceWriter trace;
    private final ConcurrentHashMap<String, Integer> attemptsByJob = new ConcurrentHashMap<>();
    private final AtomicLong succeededCount = new AtomicLong();
    private final AtomicLong failedCount = new AtomicLong();
    private final AtomicLong timedOutCount = new AtomicLong();
    private final AtomicLong quarantinedCount = new AtomicLong();

    public SimulationInterceptor(TraceWriter trace) {
        this.trace = Objects.requireNonNull(trace, "trace");
    }

    @Override
    public void onProcessingStarting(Job job, JobExecutionContext ctx) {
        int attempt = attemptsByJob.merge(job.id().toString(), 1, Integer::sum);
        var fields = new LinkedHashMap<String, Object>();
        fields.put("jobId", job.id().toString());
        fields.put("queue", job.queue());
        fields.put("attempt", attempt);
        trace.emit("claimed", fields);
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
        var fields = new LinkedHashMap<String, Object>();
        fields.put("jobId", job.id().toString());
        fields.put("queue", job.queue());
        fields.put("attempts", attemptsByJob.getOrDefault(job.id().toString(), 0));
        fields.put("final", true);
        trace.emit("succeeded", fields);
        emitLockReleased(job);
    }

    @Override
    public void onProcessingFailed(Job job, JobExecutionContext ctx, Throwable cause, FailureCause causeKind) {
        var fields = new LinkedHashMap<String, Object>();
        fields.put("jobId", job.id().toString());
        fields.put("queue", job.queue());
        fields.put("attempts", attemptsByJob.getOrDefault(job.id().toString(), 0));
        fields.put("causeKind", causeKind.name());
        fields.put("causeMessage", cause == null ? null : truncate(cause.getMessage()));
        boolean finalState = job.currentState() != JobState.SCHEDULED;
        fields.put("final", finalState);
        String event =
                switch (causeKind) {
                    case TIMEOUT -> "timed_out";
                    case QUARANTINE -> "quarantined";
                    default -> "failed";
                };
        if (event.equals("timed_out")) timedOutCount.incrementAndGet();
        else if (event.equals("quarantined")) quarantinedCount.incrementAndGet();
        else failedCount.incrementAndGet();
        trace.emit(event, fields);
        // The lock release is what makes the next-in-line claim possible — emit
        // it whether the failure is final or will retry.
        emitLockReleased(job);
    }

    @Override
    public void onStateChange(Job job, JobState from, JobState to) {
        // Captured indirectly through claimed / succeeded / failed events; emitting
        // every transition would dwarf the signal we need.
    }

    private void emitLockReleased(Job job) {
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

    /** For the summary report. */
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

    public long startedAt() {
        return Instant.now().toEpochMilli();
    }
}
