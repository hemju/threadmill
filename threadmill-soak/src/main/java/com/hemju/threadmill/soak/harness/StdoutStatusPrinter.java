package com.hemju.threadmill.soak.harness;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.hemju.threadmill.core.JobState;

/**
 * Emits one stdout line per second so a human watching the terminal knows
 * the run is progressing. Format:
 *
 * <pre>
 * [soakPostgres mixed-workload run-2026-05-16T12-30-01Z] t=30s enq=6010 succ=5921 fail=24 q=89 inflight=24 wait_p99=320ms
 * </pre>
 *
 * <p>Reads from the same {@link MetricsSampler} snapshot the structured
 * metrics file uses, so the live line never disagrees with the artifacts.
 */
public final class StdoutStatusPrinter implements AutoCloseable {

    private final String taskLabel;
    private final String scenario;
    private final String runId;
    private final MetricsSampler sampler;
    private final SoakInterceptor interceptor;
    private final AtomicLong enqueuedSoFar;
    private final ScheduledExecutorService scheduler;

    public StdoutStatusPrinter(
            String taskLabel,
            String scenario,
            String runId,
            MetricsSampler sampler,
            SoakInterceptor interceptor,
            AtomicLong enqueuedSoFar) {
        this.taskLabel = Objects.requireNonNull(taskLabel, "taskLabel");
        this.scenario = Objects.requireNonNull(scenario, "scenario");
        this.runId = Objects.requireNonNull(runId, "runId");
        this.sampler = Objects.requireNonNull(sampler, "sampler");
        this.interceptor = Objects.requireNonNull(interceptor, "interceptor");
        this.enqueuedSoFar = Objects.requireNonNull(enqueuedSoFar, "enqueuedSoFar");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "soak-stdout-printer");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::print, 1, 1, TimeUnit.SECONDS);
    }

    private void print() {
        try {
            MetricsSampler.Snapshot s = sampler.lastSnapshot();
            Map<JobState, Long> states = s.states();
            long queued = states.getOrDefault(JobState.ENQUEUED, 0L) + states.getOrDefault(JobState.SCHEDULED, 0L);
            long succ = interceptor.succeeded();
            long fail = interceptor.failed() + interceptor.timedOut() + interceptor.quarantined();
            System.out.printf(
                    "[%s %s %s] t=%ds enq=%d succ=%d fail=%d q=%d inflight=%d wait_p99=%dms%n",
                    taskLabel,
                    scenario,
                    runId,
                    s.elapsedSeconds(),
                    enqueuedSoFar.get(),
                    succ,
                    fail,
                    queued,
                    s.inflight(),
                    s.endToEndP99Ms());
        } catch (RuntimeException ignore) {
            // Live status is best-effort; structured artifacts are the source of truth.
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
