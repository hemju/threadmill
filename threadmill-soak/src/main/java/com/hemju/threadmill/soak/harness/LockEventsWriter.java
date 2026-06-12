package com.hemju.threadmill.soak.harness;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Derives {@code lock-events.jsonl} from {@code trace.jsonl}: one row per
 * (jobId, lockKey) pairing the {@code lock_acquired} and {@code lock_released}
 * events with hold duration. Cheaper for an AI agent to scan than the full
 * trace when the question is purely about lock semantics.
 *
 * <p>Streams the trace file line by line — in-memory state is the open-acquire
 * map (bounded by concurrently held locks) plus per-key hold-duration samples
 * for the contention summary.
 */
public final class LockEventsWriter {

    private LockEventsWriter() {}

    public static SummaryReport.LockContention write(Path traceJsonl, OutputDir dir) throws IOException {
        var mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        Map<String, Acquire> openAcquires = new HashMap<>();
        Map<String, KeyStats> byKey = new LinkedHashMap<>();
        Map<String, Integer> activeShared = new HashMap<>();
        Map<String, GrowableLongArray> holdsByKey = new HashMap<>();

        try (BufferedWriter writer = Files.newBufferedWriter(dir.lockEventsJsonl(), StandardCharsets.UTF_8);
                var lines = Files.lines(traceJsonl)) {
            lines.forEach(line -> {
                if (line.isBlank()) return;
                JsonNode e;
                try {
                    e = mapper.readTree(line);
                } catch (IOException ex) {
                    throw new UncheckedIOException("malformed trace line: " + line, ex);
                }
                String event = e.path("event").asText();
                String key = e.path("lockKey").asText("");
                if (key.isEmpty()) return;
                String jobId = e.path("jobId").asText("");
                String mode = e.path("lockMode").asText("");
                Instant ts = Instant.parse(e.get("timestamp").asText());
                if ("lock_acquired".equals(event)) {
                    String openKey = jobId + "::" + key;
                    openAcquires.put(openKey, new Acquire(ts, mode));
                    KeyStats stats = byKey.computeIfAbsent(key, k -> new KeyStats());
                    stats.acquires++;
                    if ("EXCLUSIVE".equals(mode)) stats.exclusiveCount++;
                    if ("SHARED".equals(mode)) {
                        int after = activeShared.merge(key, 1, Integer::sum);
                        stats.maxConcurrentShared = Math.max(stats.maxConcurrentShared, after);
                    }
                } else if ("lock_released".equals(event)) {
                    String openKey = jobId + "::" + key;
                    Acquire open = openAcquires.remove(openKey);
                    if (open != null) {
                        long heldMs = Duration.between(open.at, ts).toMillis();
                        Map<String, Object> row = new LinkedHashMap<>();
                        row.put("jobId", jobId);
                        row.put("lockKey", key);
                        row.put("lockMode", open.mode);
                        row.put("acquiredAt", open.at.toString());
                        row.put("releasedAt", ts.toString());
                        row.put("heldMillis", heldMs);
                        try {
                            writer.write(mapper.writeValueAsString(row));
                            writer.write('\n');
                        } catch (IOException ex) {
                            throw new UncheckedIOException(ex);
                        }
                        holdsByKey
                                .computeIfAbsent(key, k -> new GrowableLongArray())
                                .add(heldMs);
                        if ("SHARED".equals(open.mode)) {
                            activeShared.merge(key, -1, Integer::sum);
                        }
                    }
                }
            });
        }
        // Build the summary contention map.
        Map<String, SummaryReport.LockStats> stats = new LinkedHashMap<>();
        for (var entry : byKey.entrySet()) {
            String key = entry.getKey();
            KeyStats s = entry.getValue();
            GrowableLongArray holds = holdsByKey.get(key);
            long[] arr = holds == null ? new long[0] : holds.toArray();
            long sum = 0L;
            for (long v : arr) sum += v;
            long avg = arr.length == 0 ? 0L : sum / arr.length;
            long p99 = Percentiles.percentile(arr, 0.99);
            stats.put(key, new SummaryReport.LockStats(s.acquires, s.maxConcurrentShared, s.exclusiveCount, avg, p99));
        }
        return new SummaryReport.LockContention(stats);
    }

    private record Acquire(Instant at, String mode) {}

    private static final class KeyStats {
        long acquires;
        int maxConcurrentShared;
        long exclusiveCount;
    }
}
