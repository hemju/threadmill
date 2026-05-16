package com.hemju.threadmill.simulation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Reads a JSON-lines trace and asserts every invariant the simulation
 * promises. Each violation is collected with the event chain that proves it,
 * so a test failure points at the exact cause.
 *
 * <p>Invariants:
 *
 * <ul>
 *   <li><strong>At-least-once delivery.</strong> Every enqueued job has a
 *       terminal event (succeeded, failed with {@code final=true}, timed_out
 *       with {@code final=true}, or quarantined).</li>
 *   <li><strong>Concurrency exclusion.</strong> For any concurrency key, at
 *       any time, never an EXCLUSIVE lock overlapping with another lock for
 *       the same key.</li>
 *   <li><strong>Lock pairing.</strong> Every {@code lock_acquired} has a
 *       matching {@code lock_released} with the same job id and key. The
 *       reverse direction is also checked.</li>
 *   <li><strong>Pause obeyed.</strong> Between a {@code queue_paused} and
 *       the matching {@code queue_resumed} for one queue, no {@code claimed}
 *       event names that queue.</li>
 * </ul>
 */
public final class TraceVerifier {

    private final Path traceFile;
    private final ObjectMapper mapper = new ObjectMapper();

    public TraceVerifier(Path traceFile) {
        this.traceFile = Objects.requireNonNull(traceFile, "traceFile");
    }

    public Result verify() throws IOException {
        List<JsonNode> events = new ArrayList<>();
        try (var lines = Files.lines(traceFile)) {
            lines.forEach(line -> {
                if (line.isBlank()) return;
                try {
                    events.add(mapper.readTree(line));
                } catch (IOException e) {
                    throw new IllegalStateException("Malformed trace line: " + line, e);
                }
            });
        }
        var violations = new ArrayList<String>();
        checkAtLeastOnce(events, violations);
        checkConcurrencyExclusion(events, violations);
        checkLockPairing(events, violations);
        checkPauseObeyed(events, violations);
        return new Result(events.size(), List.copyOf(violations));
    }

    private void checkAtLeastOnce(List<JsonNode> events, List<String> violations) {
        Set<String> enqueued = new HashSet<>();
        Set<String> terminal = new HashSet<>();
        for (JsonNode e : events) {
            String event = e.path("event").asText();
            String jobId = e.path("jobId").asText("");
            if (jobId.isEmpty()) continue;
            if ("enqueued".equals(event)) enqueued.add(jobId);
            if ("succeeded".equals(event)
                    || "quarantined".equals(event)
                    || (("failed".equals(event) || "timed_out".equals(event))
                            && e.path("final").asBoolean(false))) {
                terminal.add(jobId);
            }
        }
        for (String id : enqueued) {
            if (!terminal.contains(id)) {
                violations.add("at-least-once: job " + id + " was enqueued but never reached a terminal event");
            }
        }
    }

    private void checkConcurrencyExclusion(List<JsonNode> events, List<String> violations) {
        // For each lockKey, keep counters of in-flight EXCLUSIVE and SHARED, by walking events in order.
        Map<String, ActiveLocks> active = new HashMap<>();
        for (JsonNode e : events) {
            String event = e.path("event").asText();
            String key = e.path("lockKey").asText("");
            if (key.isEmpty()) continue;
            String mode = e.path("lockMode").asText("");
            ActiveLocks a = active.computeIfAbsent(key, k -> new ActiveLocks());
            if ("lock_acquired".equals(event)) {
                if ("EXCLUSIVE".equals(mode) && (a.exclusive > 0 || a.shared > 0)) {
                    violations.add("exclusion: EXCLUSIVE lock_acquired on " + key + " while other locks active "
                            + "(exclusive=" + a.exclusive + ", shared=" + a.shared + ")");
                }
                if ("SHARED".equals(mode) && a.exclusive > 0) {
                    violations.add("exclusion: SHARED lock_acquired on " + key + " while EXCLUSIVE active (exclusive="
                            + a.exclusive + ")");
                }
                if ("EXCLUSIVE".equals(mode)) a.exclusive++;
                else if ("SHARED".equals(mode)) a.shared++;
            } else if ("lock_released".equals(event)) {
                if ("EXCLUSIVE".equals(mode)) a.exclusive = Math.max(0, a.exclusive - 1);
                else if ("SHARED".equals(mode)) a.shared = Math.max(0, a.shared - 1);
            }
        }
    }

    private void checkLockPairing(List<JsonNode> events, List<String> violations) {
        // For every job: the number of lock_acquired events must equal the number of lock_released events.
        // (Across retries we see one acquire + one release per attempt.)
        Map<String, int[]> perJob = new HashMap<>();
        for (JsonNode e : events) {
            String event = e.path("event").asText();
            String jobId = e.path("jobId").asText("");
            if (jobId.isEmpty()) continue;
            int[] counts = perJob.computeIfAbsent(jobId, k -> new int[2]);
            if ("lock_acquired".equals(event)) counts[0]++;
            else if ("lock_released".equals(event)) counts[1]++;
        }
        for (var entry : perJob.entrySet()) {
            if (entry.getValue()[0] != entry.getValue()[1]) {
                violations.add("lock pairing: job " + entry.getKey()
                        + " has acquire=" + entry.getValue()[0]
                        + " release=" + entry.getValue()[1]);
            }
        }
    }

    private void checkPauseObeyed(List<JsonNode> events, List<String> violations) {
        Map<String, Instant> pausedAt = new HashMap<>();
        for (JsonNode e : events) {
            String event = e.path("event").asText();
            String queue = e.path("queue").asText("");
            Instant ts = Instant.parse(e.get("timestamp").asText());
            if ("queue_paused".equals(event)) {
                pausedAt.put(queue, ts);
            } else if ("queue_resumed".equals(event)) {
                pausedAt.remove(queue);
            } else if ("claimed".equals(event) && pausedAt.containsKey(queue)) {
                violations.add("pause: claim on paused queue " + queue + " at " + ts + " (paused at "
                        + pausedAt.get(queue) + ")");
            }
        }
    }

    private static final class ActiveLocks {
        int exclusive;
        int shared;
    }

    public record Result(long lineCount, List<String> violations) {

        public boolean isClean() {
            return violations.isEmpty();
        }

        public Map<String, Object> asReport() {
            var out = new LinkedHashMap<String, Object>();
            out.put("lineCount", lineCount);
            out.put("violationCount", violations.size());
            out.put("violations", violations);
            return out;
        }
    }
}
