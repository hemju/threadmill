package com.hemju.threadmill.soak.harness;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.soak.harness.invariant.InvariantResult;
import com.hemju.threadmill.soak.harness.invariant.LiveInvariantVerifier;

/**
 * Periodically rewrites {@code progress.json} so a still-running soak can be
 * inspected from outside the process — by an operator tailing the artifact
 * directory, or by the endurance orchestrator combining both backends' status
 * lines. The final state of the file after the run is superseded by
 * {@code summary.json}.
 *
 * <p>Each write goes to a temp file and is atomically moved into place, so a
 * reader never observes a torn JSON document.
 */
public final class ProgressReporter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ProgressReporter.class);

    private final Path progressJson;
    private final SoakHarnessConfig config;
    private final Instant runStart;
    private final MetricsSampler metrics;
    private final SoakInterceptor interceptor;
    private final AtomicLong enqueuedCount;
    private final LiveInvariantVerifier verifier;
    private final BooleanSupplier aborted;
    private final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private final ScheduledExecutorService scheduler;
    private volatile String phase = "running";

    public ProgressReporter(
            Path progressJson,
            SoakHarnessConfig config,
            Instant runStart,
            MetricsSampler metrics,
            SoakInterceptor interceptor,
            AtomicLong enqueuedCount,
            LiveInvariantVerifier verifier,
            BooleanSupplier aborted) {
        this.progressJson = Objects.requireNonNull(progressJson, "progressJson");
        this.config = Objects.requireNonNull(config, "config");
        this.runStart = Objects.requireNonNull(runStart, "runStart");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.interceptor = Objects.requireNonNull(interceptor, "interceptor");
        this.enqueuedCount = Objects.requireNonNull(enqueuedCount, "enqueuedCount");
        this.verifier = Objects.requireNonNull(verifier, "verifier");
        this.aborted = Objects.requireNonNull(aborted, "aborted");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "soak-progress-reporter");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        long periodMs = Math.max(1L, config.progressInterval().toMillis());
        scheduler.scheduleAtFixedRate(this::writeSafely, 0, periodMs, TimeUnit.MILLISECONDS);
    }

    /** Lifecycle marker surfaced in the file: {@code running}, {@code draining}, {@code finished}. */
    public void phase(String phase) {
        this.phase = Objects.requireNonNull(phase, "phase");
        writeSafely();
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        phase = "finished";
        writeSafely();
    }

    private void writeSafely() {
        try {
            writeOnce();
        } catch (RuntimeException | IOException e) {
            // Progress is a convenience view; never let it interfere with the run.
            LOG.warn("progress.json write failed: {}", e.toString());
        }
    }

    private void writeOnce() throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("timestamp", Instant.now().toString());
        doc.put("runId", config.runId());
        doc.put("backend", config.backend());
        doc.put("scenario", config.scenario());
        doc.put("phase", phase);
        doc.put("aborted", aborted.getAsBoolean());
        doc.put("elapsedMs", Duration.between(runStart, Instant.now()).toMillis());
        doc.put("targetDurationMs", config.duration().toMillis());

        Map<String, Object> counts = new LinkedHashMap<>();
        counts.put("enqueued", enqueuedCount.get());
        counts.put("succeeded", interceptor.succeeded());
        counts.put("failed", interceptor.failed());
        counts.put("timedOut", interceptor.timedOut());
        counts.put("quarantined", interceptor.quarantined());
        counts.put("retried", interceptor.retried());
        doc.put("counts", counts);

        MetricsSampler.Snapshot snapshot = metrics.lastSnapshot();
        Map<String, Long> states = new LinkedHashMap<>();
        snapshot.states().forEach((state, count) -> states.put(state.name(), count));
        doc.put("storeCountsByState", states);
        doc.put("queueDepths", snapshot.queueDepths());
        doc.put("inflight", snapshot.inflight());
        doc.put("endToEndP99Ms", snapshot.endToEndP99Ms());

        var invariants = new ArrayList<Map<String, Object>>();
        for (InvariantResult result : verifier.snapshotResults()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("name", result.name());
            entry.put("passed", result.passed());
            entry.put("violationCount", result.violations().size());
            entry.put(
                    "firstViolations",
                    result.violations()
                            .subList(0, Math.min(3, result.violations().size())));
            invariants.add(entry);
        }
        doc.put("invariants", invariants);

        Path tmp = progressJson.resolveSibling(progressJson.getFileName() + ".tmp");
        Files.writeString(tmp, mapper.writeValueAsString(doc), StandardCharsets.UTF_8);
        Files.move(tmp, progressJson, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
}
