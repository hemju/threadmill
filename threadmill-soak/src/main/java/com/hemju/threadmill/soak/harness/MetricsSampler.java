package com.hemju.threadmill.soak.harness;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.store.JobStore;

/**
 * Once-per-second snapshot of engine state: per-state counts, per-queue
 * depths, in-flight count. Lines are appended to {@code metrics.jsonl} for
 * post-run analysis; the live-status printer reads the most recent snapshot.
 *
 * <p>Store queries are not always cheap, so the sampler runs on its own
 * single-threaded scheduler and never blocks the load generator.
 */
public final class MetricsSampler implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsSampler.class);

    private final JobStore store;
    private final LatencyTracker latencyTracker;
    private final BufferedWriter writer;
    private final ObjectMapper mapper;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final ScheduledExecutorService scheduler;
    private final Instant startedAt;
    private volatile Snapshot lastSnapshot = Snapshot.empty();

    public MetricsSampler(Path file, JobStore store, LatencyTracker latencyTracker) throws IOException {
        Objects.requireNonNull(file, "file");
        this.store = Objects.requireNonNull(store, "store");
        this.latencyTracker = Objects.requireNonNull(latencyTracker, "latencyTracker");
        if (file.getParent() != null) Files.createDirectories(file.getParent());
        this.writer = Files.newBufferedWriter(
                file,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "soak-metrics-sampler");
            t.setDaemon(true);
            return t;
        });
        this.startedAt = Instant.now();
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::sampleAndWrite, 0, 1, TimeUnit.SECONDS);
    }

    public Snapshot lastSnapshot() {
        return lastSnapshot;
    }

    private void sampleAndWrite() {
        try {
            Map<JobState, Long> counts = store.countsByState();
            Map<String, Long> queueDepths = store.queueDepths();
            int inflight = latencyTracker.inflight();
            long p99 = latencyTracker.currentP99EndToEndMs();
            long elapsedSeconds =
                    Math.max(0, Duration.between(startedAt, Instant.now()).toSeconds());
            Snapshot snapshot = new Snapshot(elapsedSeconds, counts, queueDepths, inflight, p99);
            lastSnapshot = snapshot;
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("timestamp", Instant.now().toString());
            row.put("elapsedSeconds", elapsedSeconds);
            Map<String, Long> stateCounts = new LinkedHashMap<>();
            for (JobState s : JobState.values()) stateCounts.put(s.name(), counts.getOrDefault(s, 0L));
            row.put("states", stateCounts);
            row.put("queueDepths", queueDepths);
            row.put("inflight", inflight);
            row.put("endToEndP99Ms", p99);
            writeLine(row);
        } catch (RuntimeException e) {
            // The store outage circuit breaker pauses the cluster, not the harness;
            // log and keep sampling. The same outage will be visible in trace.jsonl.
            LOG.warn("metrics sample failed: {}", e.toString());
        }
    }

    private void writeLine(Map<String, Object> row) {
        writeLock.lock();
        try {
            writer.write(mapper.writeValueAsString(row));
            writer.write('\n');
            writer.flush();
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("metrics row not serialisable", e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        writeLock.lock();
        try {
            writer.flush();
            writer.close();
        } finally {
            writeLock.unlock();
        }
    }

    public record Snapshot(
            long elapsedSeconds,
            Map<JobState, Long> states,
            Map<String, Long> queueDepths,
            int inflight,
            long endToEndP99Ms) {

        public static Snapshot empty() {
            return new Snapshot(0L, Map.of(), Map.of(), 0, 0L);
        }
    }
}
