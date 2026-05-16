package com.hemju.threadmill.soak.harness;

import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

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
        int attempts = attemptsByJob.getOrDefault(job.id().toString(), 0);
        var fields = new LinkedHashMap<String, Object>();
        fields.put("jobId", job.id().toString());
        fields.put("queue", job.queue());
        fields.put("attempts", attempts);
        fields.put("final", true);
        trace.emit("succeeded", fields);
        emitLockReleased(job);
        latencyTracker.recordCompleted(job.id(), attempts, "SUCCEEDED");
    }

    @Override
    public void onProcessingFailed(Job job, JobExecutionContext ctx, Throwable cause, FailureCause causeKind) {
        int attempts = attemptsByJob.getOrDefault(job.id().toString(), 0);
        var fields = new LinkedHashMap<String, Object>();
        fields.put("jobId", job.id().toString());
        fields.put("queue", job.queue());
        fields.put("attempts", attempts);
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
        switch (event) {
            case "timed_out" -> timedOutCount.incrementAndGet();
            case "quarantined" -> quarantinedCount.incrementAndGet();
            default -> failedCount.incrementAndGet();
        }
        trace.emit(event, fields);
        emitLockReleased(job);
        if (!finalState) {
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
