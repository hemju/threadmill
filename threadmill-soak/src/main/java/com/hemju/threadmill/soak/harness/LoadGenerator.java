package com.hemju.threadmill.soak.harness;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.schedule.Scheduler;

/**
 * Rate-paced producer that scenarios call to enqueue work.
 *
 * <p>Emits {@code enqueued} trace events at the time of the actual store
 * insert. The engine's {@link com.hemju.threadmill.core.engine.JobInterceptor}
 * has no {@code onEnqueued} hook (insertion happens outside the engine), so
 * this is the only correct place to record those events.
 *
 * <p>Pacing is intentionally simple: a deadline-based sleep loop, no
 * sliding-window jitter. Burstiness from natural Gradle / JVM jitter is
 * exactly what soak runs want to expose.
 *
 * <p>With {@code -Pproducers=N} the runner builds one generator per producer
 * thread, each pacing at the split rate but all sharing one enqueued counter
 * so the status line and {@code progress.json} report run totals.
 */
public final class LoadGenerator {

    private final Scheduler scheduler;
    private final SoakTraceWriter trace;
    private final LatencyTracker latencyTracker;
    private final AtomicLong enqueuedCount;
    private final int jobsPerSecond;

    public LoadGenerator(Scheduler scheduler, SoakTraceWriter trace, LatencyTracker latencyTracker, int jobsPerSecond) {
        this(scheduler, trace, latencyTracker, jobsPerSecond, new AtomicLong());
    }

    public LoadGenerator(
            Scheduler scheduler,
            SoakTraceWriter trace,
            LatencyTracker latencyTracker,
            int jobsPerSecond,
            AtomicLong sharedEnqueuedCount) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.trace = Objects.requireNonNull(trace, "trace");
        this.latencyTracker = Objects.requireNonNull(latencyTracker, "latencyTracker");
        if (jobsPerSecond <= 0) throw new IllegalArgumentException("jobsPerSecond must be positive");
        this.jobsPerSecond = jobsPerSecond;
        this.enqueuedCount = Objects.requireNonNull(sharedEnqueuedCount, "sharedEnqueuedCount");
    }

    public int jobsPerSecond() {
        return jobsPerSecond;
    }

    public AtomicLong enqueuedCount() {
        return enqueuedCount;
    }

    /** Compute the deadline (relative to a start instant) for the {@code n}-th job. */
    public Instant deadlineFor(Instant runStart, long n) {
        long nanos = Math.max(0L, n) * Duration.ofSeconds(1).toNanos() / jobsPerSecond;
        return runStart.plusNanos(nanos);
    }

    /**
     * Sleep until {@code deadline}. Returns immediately if already past it.
     * The pacing-deadline strategy means producing slower than the target
     * rate at the start does not "burn" budget — the next call catches up.
     */
    public void pace(Instant deadline) throws InterruptedException {
        long sleepMs = Duration.between(Instant.now(), deadline).toMillis();
        if (sleepMs > 0) Thread.sleep(Math.min(sleepMs, 50));
    }

    public <P extends JobPayload> JobId enqueue(
            P payload, Class<? extends JobHandler<P>> handler, String queue, int priority) {
        return enqueue(payload, handler, queue, priority, null, null);
    }

    public <P extends JobPayload> JobId enqueue(
            P payload,
            Class<? extends JobHandler<P>> handler,
            String queue,
            int priority,
            String concurrencyKey,
            ConcurrencyMode concurrencyMode) {
        JobId id = scheduler.enqueue(payload, handler, queue, priority, concurrencyKey, concurrencyMode);
        latencyTracker.recordEnqueued(id);
        enqueuedCount.incrementAndGet();
        emitEnqueued(id, queue, handler, concurrencyKey, concurrencyMode);
        return id;
    }

    public <P extends JobPayload> List<JobId> enqueueAll(
            List<P> payloads, Class<? extends JobHandler<P>> handler, String queue, int priority) {
        List<JobId> ids = scheduler.enqueueAll(payloads, handler, queue, priority);
        for (JobId id : ids) {
            latencyTracker.recordEnqueued(id);
            enqueuedCount.incrementAndGet();
            emitEnqueued(id, queue, handler, null, null);
        }
        return ids;
    }

    private void emitEnqueued(
            JobId id,
            String queue,
            Class<? extends JobHandler<?>> handler,
            String concurrencyKey,
            ConcurrencyMode concurrencyMode) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("jobId", id.toString());
        fields.put("queue", queue);
        fields.put("handler", handler.getSimpleName());
        fields.put("lockKey", concurrencyKey);
        fields.put("lockMode", concurrencyMode == null ? null : concurrencyMode.name());
        trace.emit("enqueued", fields);
    }
}
