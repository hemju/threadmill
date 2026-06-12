package com.hemju.threadmill.soak.harness;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hemju.threadmill.core.JobId;

/**
 * Tracks per-job lifecycle timings: when it was enqueued, claimed, started,
 * and completed. Each completion writes one JSON-lines record to
 * {@code latencies.jsonl}; in-memory aggregates support the run summary
 * percentiles and the {@code inflight} count surfaced in the live-status line.
 */
public final class LatencyTracker implements AutoCloseable {

    private final BufferedWriter writer;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ReentrantLock writeLock = new ReentrantLock();
    private final ConcurrentHashMap<String, Stages> stages = new ConcurrentHashMap<>();
    private final RecentWindow endToEnd = new RecentWindow(65_536);

    public LatencyTracker(Path file) throws IOException {
        Objects.requireNonNull(file, "file");
        if (file.getParent() != null) Files.createDirectories(file.getParent());
        this.writer = Files.newBufferedWriter(
                file,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
    }

    public void recordEnqueued(JobId id) {
        stages.put(id.toString(), new Stages(System.nanoTime()));
    }

    public void recordClaimed(JobId id) {
        Stages s = stages.get(id.toString());
        if (s != null && s.claimedAt == 0L) s.claimedAt = System.nanoTime();
    }

    public void recordStarted(JobId id) {
        Stages s = stages.get(id.toString());
        if (s != null && s.startedAt == 0L) s.startedAt = System.nanoTime();
    }

    public void recordCompleted(JobId id, int attempts, String finalState) {
        Stages s = stages.remove(id.toString());
        if (s == null) return;
        long completedAt = System.nanoTime();
        long enqueuedToClaimMs = msBetween(s.enqueuedAt, s.claimedAt);
        long claimToStartMs = msBetween(s.claimedAt, s.startedAt);
        long startToCompleteMs = msBetween(s.startedAt, completedAt);
        long endToEndMs = msBetween(s.enqueuedAt, completedAt);
        endToEnd.add(endToEndMs);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("jobId", id.toString());
        row.put("attempts", attempts);
        row.put("finalState", finalState);
        row.put("enqueueToClaimMs", s.claimedAt == 0L ? null : enqueuedToClaimMs);
        row.put("claimToStartMs", s.claimedAt == 0L || s.startedAt == 0L ? null : claimToStartMs);
        row.put("startToCompleteMs", s.startedAt == 0L ? null : startToCompleteMs);
        row.put("endToEndMs", endToEndMs);
        writeLine(row);
    }

    public int inflight() {
        return stages.size();
    }

    /**
     * Snapshot of the current p99 end-to-end latency, in ms, over the most
     * recent window of completions; used by the live-status printer. The
     * full-run percentiles in the summary are computed from
     * {@code latencies.jsonl}, which holds every sample.
     */
    public long currentP99EndToEndMs() {
        return endToEnd.snapshotPercentile(0.99);
    }

    private void writeLine(Map<String, Object> row) {
        writeLock.lock();
        try {
            writer.write(mapper.writeValueAsString(row));
            writer.write('\n');
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("latency row not serialisable", e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            writeLock.unlock();
        }
    }

    private static long msBetween(long fromNano, long toNano) {
        if (fromNano == 0L || toNano == 0L) return 0L;
        return Duration.ofNanos(Math.max(0L, toNano - fromNano)).toMillis();
    }

    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            writer.flush();
            writer.close();
        } finally {
            writeLock.unlock();
        }
    }

    private static final class Stages {
        final long enqueuedAt;
        volatile long claimedAt;
        volatile long startedAt;

        Stages(long enqueuedAt) {
            this.enqueuedAt = enqueuedAt;
        }
    }

    /**
     * Fixed-capacity ring of the most recent samples. The live-status p99 only
     * needs a recent window; keeping every sample (an endurance run completes
     * millions of jobs) would grow memory without bound and make each status
     * refresh re-sort an ever-larger array.
     */
    static final class RecentWindow {
        private final long[] values;
        private int next;
        private int size;

        RecentWindow(int capacity) {
            if (capacity <= 0) throw new IllegalArgumentException("capacity must be positive");
            this.values = new long[capacity];
        }

        synchronized void add(long v) {
            values[next] = v;
            next = (next + 1) % values.length;
            size = Math.min(size + 1, values.length);
        }

        synchronized long snapshotPercentile(double p) {
            return Percentiles.percentile(Arrays.copyOf(values, size), p);
        }
    }
}
