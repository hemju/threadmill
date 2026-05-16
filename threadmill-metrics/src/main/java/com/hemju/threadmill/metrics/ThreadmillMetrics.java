package com.hemju.threadmill.metrics;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.engine.JobInterceptor;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.store.JobStore;

/**
 * Micrometer integration for Threadmill.
 *
 * <p>Exposes:
 * <ul>
 *   <li>{@code threadmill.jobs.count{state="..."}} — point-in-time count per
 *       state, sourced from {@link JobStore#countsByState()} (cheap — built
 *       from the incrementally-maintained counter, never a full scan).</li>
 *   <li>{@code threadmill.jobs.processed} (counter) — successful completions.</li>
 *   <li>{@code threadmill.jobs.failed} (counter) — failures by cause label.</li>
 *   <li>{@code threadmill.jobs.processing.time} (timer) — handler runtime
 *       from claim to terminal transition.</li>
 * </ul>
 *
 * <p>Register the {@link #asInterceptor()} return value with the
 * {@code ProcessingNode.Builder} so per-job events feed the meters; the
 * gauge values are pulled on Micrometer's own cadence.
 */
public final class ThreadmillMetrics {

    private final MeterRegistry registry;
    private final JobStore store;
    private final Map<JobState, AtomicLong> stateGauges = new EnumMap<>(JobState.class);
    private final Map<String, AtomicLong> queueDepthGauges = new ConcurrentHashMap<>();
    private final AtomicLong oldestProcessingHeartbeatAgeMillis = new AtomicLong(0);
    private final Counter processedCounter;
    private final Counter refreshErrors;
    private final Map<String, Counter> failedCounters = new ConcurrentHashMap<>();
    private final Timer processingTime;
    private final Timer claimLatency;
    private final ConcurrentHashMap<String, Instant> inFlightStart = new ConcurrentHashMap<>();

    public ThreadmillMetrics(MeterRegistry registry, JobStore store) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.store = Objects.requireNonNull(store, "store");
        for (JobState s : JobState.values()) {
            var v = new AtomicLong();
            stateGauges.put(s, v);
            Gauge.builder("threadmill.jobs.count", v, AtomicLong::doubleValue)
                    .tag("state", s.name())
                    .description("Number of Threadmill jobs in this state")
                    .register(registry);
        }
        this.processedCounter = Counter.builder("threadmill.jobs.processed")
                .description("Total successfully-processed Threadmill jobs since startup")
                .register(registry);
        this.refreshErrors = Counter.builder("threadmill.metrics.refresh.errors")
                .description("Errors while refreshing Threadmill metrics from the store")
                .register(registry);
        this.processingTime = Timer.builder("threadmill.jobs.processing.time")
                .description("Wall-clock time from claim to terminal transition")
                .register(registry);
        this.claimLatency = Timer.builder("threadmill.claim.latency")
                .description("Wall-clock time spent in JobStore claimReady calls when recorded by the host")
                .register(registry);
        Gauge.builder(
                        "threadmill.processing.oldest.heartbeat.age",
                        oldestProcessingHeartbeatAgeMillis,
                        AtomicLong::doubleValue)
                .description("Age in milliseconds of the oldest processing heartbeat")
                .register(registry);
        refresh();
    }

    /** Refresh the per-state gauges from the store. */
    public void refresh() {
        try {
            Map<JobState, Long> counts = store.countsByState();
            for (JobState s : JobState.values()) {
                stateGauges.get(s).set(counts.getOrDefault(s, 0L));
            }
            for (var e : store.queueDepths().entrySet()) {
                queueGauge(e.getKey()).set(e.getValue());
            }
            long age = store.oldestProcessingHeartbeat()
                    .map(at -> Math.max(0L, Duration.between(at, Instant.now()).toMillis()))
                    .orElse(0L);
            oldestProcessingHeartbeatAgeMillis.set(age);
        } catch (RuntimeException e) {
            refreshErrors.increment();
        }
    }

    /** Record an externally-observed claim latency. */
    public void recordClaimLatency(Duration duration) {
        claimLatency.record(duration);
    }

    /** A JobInterceptor that drives the success / failure counters and the timer. */
    public JobInterceptor asInterceptor() {
        return new JobInterceptor() {
            @Override
            public void onProcessingStarting(Job job, JobExecutionContext ctx) {
                inFlightStart.put(job.id().toString(), Instant.now());
            }

            @Override
            public void onProcessingSucceeded(Job job, JobExecutionContext ctx) {
                processedCounter.increment();
                recordElapsed(job);
                refresh();
            }

            @Override
            public void onProcessingFailed(Job job, JobExecutionContext ctx, Throwable cause, FailureCause kind) {
                failedCounters
                        .computeIfAbsent(
                                kind.name(),
                                k -> Counter.builder("threadmill.jobs.failed")
                                        .tag("cause", k)
                                        .register(registry))
                        .increment();
                recordElapsed(job);
                refresh();
            }
        };
    }

    private void recordElapsed(Job job) {
        Instant started = inFlightStart.remove(job.id().toString());
        if (started != null) {
            processingTime.record(Duration.between(started, Instant.now()));
        }
    }

    private AtomicLong queueGauge(String queue) {
        return queueDepthGauges.computeIfAbsent(queue, q -> {
            var v = new AtomicLong();
            Gauge.builder("threadmill.queue.depth", v, AtomicLong::doubleValue)
                    .tag("queue", q)
                    .description("Number of ENQUEUED Threadmill jobs in this queue")
                    .register(registry);
            Gauge.builder("threadmill.queue.oldest.enqueued.age", v, ignored -> queueAgeMillis(q))
                    .tag("queue", q)
                    .description("Age in milliseconds of the oldest ENQUEUED job in this queue")
                    .register(registry);
            return v;
        });
    }

    private double queueAgeMillis(String queue) {
        try {
            return store.oldestEnqueuedAt(queue)
                    .map(at -> (double)
                            Math.max(0L, Duration.between(at, Instant.now()).toMillis()))
                    .orElse(0d);
        } catch (RuntimeException e) {
            refreshErrors.increment();
            return 0d;
        }
    }
}
