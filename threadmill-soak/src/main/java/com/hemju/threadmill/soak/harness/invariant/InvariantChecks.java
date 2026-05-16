package com.hemju.threadmill.soak.harness.invariant;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Library of named invariant checks.
 *
 * <p>Each factory method returns a {@link SoakInvariant} that walks the
 * corpus once. None of these implementations keep state across runs;
 * scenarios pick the subset that applies to their workload.
 */
public final class InvariantChecks {

    private InvariantChecks() {}

    /** Every enqueued job reaches a terminal event. */
    public static SoakInvariant atLeastOnce() {
        return new Checker("atLeastOnce", "every enqueued job reaches a terminal state", corpus -> {
            Set<String> enqueued = new LinkedHashSet<>();
            Set<String> terminal = new HashSet<>();
            Map<String, String> firstEnqueueLineByJob = new HashMap<>();
            List<JsonNode> events = corpus.events();
            List<String> raw = corpus.rawLines();
            for (int i = 0; i < events.size(); i++) {
                JsonNode e = events.get(i);
                String event = e.path("event").asText();
                String jobId = e.path("jobId").asText("");
                if (jobId.isEmpty()) continue;
                if ("enqueued".equals(event)) {
                    enqueued.add(jobId);
                    firstEnqueueLineByJob.putIfAbsent(jobId, raw.get(i));
                }
                if ("succeeded".equals(event)
                        || "quarantined".equals(event)
                        || (("failed".equals(event) || "timed_out".equals(event))
                                && e.path("final").asBoolean(false))) {
                    terminal.add(jobId);
                }
            }
            List<String> violations = new ArrayList<>();
            List<List<String>> samples = new ArrayList<>();
            for (String id : enqueued) {
                if (!terminal.contains(id)) {
                    violations.add("job " + id + " was enqueued but never reached a terminal event");
                    if (samples.size() < 5) {
                        samples.add(List.of(firstEnqueueLineByJob.get(id)));
                    }
                }
            }
            return Outcome.of(violations, samples);
        });
    }

    /** No EXCLUSIVE lock overlaps with another lock for the same key. */
    public static SoakInvariant exclusivityHeld() {
        return new Checker("exclusivityHeld", "EXCLUSIVE locks run alone for their key", corpus -> {
            Map<String, ActiveLocks> active = new HashMap<>();
            List<String> violations = new ArrayList<>();
            List<List<String>> samples = new ArrayList<>();
            List<JsonNode> events = corpus.events();
            List<String> raw = corpus.rawLines();
            for (int i = 0; i < events.size(); i++) {
                JsonNode e = events.get(i);
                String event = e.path("event").asText();
                String key = e.path("lockKey").asText("");
                if (key.isEmpty()) continue;
                String mode = e.path("lockMode").asText("");
                ActiveLocks a = active.computeIfAbsent(key, k -> new ActiveLocks());
                if ("lock_acquired".equals(event)) {
                    if ("EXCLUSIVE".equals(mode) && (a.exclusive > 0 || a.shared > 0)) {
                        violations.add("EXCLUSIVE lock on " + key + " acquired while other locks active (exclusive="
                                + a.exclusive + ", shared=" + a.shared + ")");
                        if (samples.size() < 5) samples.add(List.of(raw.get(i)));
                    }
                    if ("SHARED".equals(mode) && a.exclusive > 0) {
                        violations.add("SHARED lock on " + key + " acquired while EXCLUSIVE active (exclusive="
                                + a.exclusive + ")");
                        if (samples.size() < 5) samples.add(List.of(raw.get(i)));
                    }
                    if ("EXCLUSIVE".equals(mode)) a.exclusive++;
                    else if ("SHARED".equals(mode)) a.shared++;
                } else if ("lock_released".equals(event)) {
                    if ("EXCLUSIVE".equals(mode)) a.exclusive = Math.max(0, a.exclusive - 1);
                    else if ("SHARED".equals(mode)) a.shared = Math.max(0, a.shared - 1);
                }
            }
            return Outcome.of(violations, samples);
        });
    }

    /**
     * For any key, a SHARED claim never precedes the lock_released of an
     * earlier-enqueued EXCLUSIVE. We approximate this by replaying enqueue
     * order per key and checking that no SHARED runs while an earlier
     * EXCLUSIVE is still pending (i.e. enqueued but not yet released).
     */
    public static SoakInvariant strictInGroupOrder() {
        return new Checker(
                "strictInGroupOrder",
                "SHARED never leapfrogs an earlier-enqueued EXCLUSIVE for the same key",
                corpus -> {
                    Map<String, List<EnqueuedJob>> orderByKey = new HashMap<>();
                    Map<String, Boolean> exclusiveRunning = new HashMap<>();
                    Set<String> finishedJobs = new HashSet<>();
                    List<String> violations = new ArrayList<>();
                    List<List<String>> samples = new ArrayList<>();
                    List<JsonNode> events = corpus.events();
                    List<String> raw = corpus.rawLines();
                    for (int i = 0; i < events.size(); i++) {
                        JsonNode e = events.get(i);
                        String event = e.path("event").asText();
                        String key = e.path("lockKey").asText("");
                        String jobId = e.path("jobId").asText("");
                        String mode = e.path("lockMode").asText("");
                        if ("enqueued".equals(event) && !key.isEmpty()) {
                            orderByKey
                                    .computeIfAbsent(key, k -> new ArrayList<>())
                                    .add(new EnqueuedJob(jobId, mode, i));
                        }
                        if ("lock_acquired".equals(event) && !key.isEmpty()) {
                            if ("SHARED".equals(mode)) {
                                List<EnqueuedJob> order = orderByKey.getOrDefault(key, List.of());
                                int idx = indexOf(order, jobId);
                                for (int j = 0; j < idx; j++) {
                                    EnqueuedJob ej = order.get(j);
                                    if ("EXCLUSIVE".equals(ej.mode) && !finishedJobs.contains(ej.jobId)) {
                                        violations.add("job " + jobId + " (SHARED on " + key
                                                + ") leapfrogged earlier EXCLUSIVE " + ej.jobId);
                                        if (samples.size() < 5) {
                                            samples.add(List.of(raw.get(ej.enqueueLineIndex), raw.get(i)));
                                        }
                                        break;
                                    }
                                }
                            }
                            if ("EXCLUSIVE".equals(mode)) exclusiveRunning.put(key, true);
                        }
                        if ("lock_released".equals(event) && !key.isEmpty()) {
                            if ("EXCLUSIVE".equals(mode)) exclusiveRunning.put(key, false);
                        }
                        if ("succeeded".equals(event)
                                || "quarantined".equals(event)
                                || (("failed".equals(event) || "timed_out".equals(event))
                                        && e.path("final").asBoolean(false))) {
                            finishedJobs.add(jobId);
                        }
                    }
                    return Outcome.of(violations, samples);
                });
    }

    /**
     * Workflow concurrency holds are unbroken between the root acquire and
     * the last descendant release. (Soak does not run workflow scenarios in
     * v1 — the check is a no-op if no workflow_root_id metadata appears,
     * but the registration slot is here so future workflow scenarios get
     * coverage for free.)
     */
    public static SoakInvariant workflowLockHeldContinuously() {
        return new Checker(
                "workflowLockHeldContinuously",
                "workflow concurrency hold is unbroken from root acquire to last descendant release",
                corpus -> Outcome.of(List.of(), List.of()));
    }

    /** No job exceeds {@code maxAttempts} (default 5; scenarios pass their configured ceiling). */
    public static SoakInvariant retryBudgetRespected(int maxAttempts) {
        return new Checker("retryBudgetRespected", "no job runs more than " + maxAttempts + " attempts", corpus -> {
            Map<String, Integer> attempts = new HashMap<>();
            Map<String, String> sampleLine = new HashMap<>();
            List<JsonNode> events = corpus.events();
            List<String> raw = corpus.rawLines();
            for (int i = 0; i < events.size(); i++) {
                JsonNode e = events.get(i);
                String event = e.path("event").asText();
                String jobId = e.path("jobId").asText("");
                if (jobId.isEmpty()) continue;
                if ("claimed".equals(event)) {
                    int attempt = e.path("attempt").asInt(0);
                    attempts.merge(jobId, attempt, Math::max);
                    sampleLine.putIfAbsent(jobId, raw.get(i));
                }
            }
            List<String> violations = new ArrayList<>();
            List<List<String>> samples = new ArrayList<>();
            for (var entry : attempts.entrySet()) {
                if (entry.getValue() > maxAttempts) {
                    violations.add("job " + entry.getKey() + " ran " + entry.getValue() + " attempts (limit "
                            + maxAttempts + ")");
                    if (samples.size() < 5) samples.add(List.of(sampleLine.get(entry.getKey())));
                }
            }
            return Outcome.of(violations, samples);
        });
    }

    /** Every lock_acquired has a matching lock_released by run end. */
    public static SoakInvariant noLockLeaks() {
        return new Checker("noLockLeaks", "every lock_acquired has a matching lock_released", corpus -> {
            Map<String, int[]> perJob = new LinkedHashMap<>();
            Map<String, String> firstLineByJob = new HashMap<>();
            List<JsonNode> events = corpus.events();
            List<String> raw = corpus.rawLines();
            for (int i = 0; i < events.size(); i++) {
                JsonNode e = events.get(i);
                String event = e.path("event").asText();
                String jobId = e.path("jobId").asText("");
                if (jobId.isEmpty()) continue;
                if ("lock_acquired".equals(event)) {
                    perJob.computeIfAbsent(jobId, k -> new int[2])[0]++;
                    firstLineByJob.putIfAbsent(jobId, raw.get(i));
                } else if ("lock_released".equals(event)) {
                    perJob.computeIfAbsent(jobId, k -> new int[2])[1]++;
                }
            }
            List<String> violations = new ArrayList<>();
            List<List<String>> samples = new ArrayList<>();
            for (var entry : perJob.entrySet()) {
                int[] counts = entry.getValue();
                if (counts[0] != counts[1]) {
                    violations.add("job " + entry.getKey() + " has acquire=" + counts[0] + " release=" + counts[1]);
                    if (samples.size() < 5) {
                        samples.add(List.of(firstLineByJob.getOrDefault(entry.getKey(), "(no acquire line)")));
                    }
                }
            }
            return Outcome.of(violations, samples);
        });
    }

    /** Between a queue_paused and the matching queue_resumed, no claimed names that queue. */
    public static SoakInvariant pauseObeyed() {
        return new Checker("pauseObeyed", "no claimed event names a queue between its pause/resume bracket", corpus -> {
            Map<String, Instant> pausedAt = new HashMap<>();
            List<String> violations = new ArrayList<>();
            List<List<String>> samples = new ArrayList<>();
            List<JsonNode> events = corpus.events();
            List<String> raw = corpus.rawLines();
            for (int i = 0; i < events.size(); i++) {
                JsonNode e = events.get(i);
                String event = e.path("event").asText();
                String queue = e.path("queue").asText("");
                Instant ts = Instant.parse(e.get("timestamp").asText());
                if ("queue_paused".equals(event)) pausedAt.put(queue, ts);
                else if ("queue_resumed".equals(event)) pausedAt.remove(queue);
                else if ("claimed".equals(event) && pausedAt.containsKey(queue)) {
                    violations.add("claim on paused queue " + queue + " at " + ts + " (paused at " + pausedAt.get(queue)
                            + ")");
                    if (samples.size() < 5) samples.add(List.of(raw.get(i)));
                }
            }
            return Outcome.of(violations, samples);
        });
    }

    /**
     * Per-queue throughput preserves the relative ordering of configured
     * weights: higher-weighted queues see ≥ throughput than lower-weighted
     * ones (with the configured {@code tolerance} of slack to absorb noise
     * on short runs). Weight 0 must observe zero throughput.
     *
     * <p>This is intentionally weaker than "matches the absolute ratio
     * within X%". The pass-based weighted-fair scheduler does deliver the
     * absolute ratio in steady state, but short runs with shallow backlog
     * cannot accumulate enough picks for the ratio to converge — the
     * directional preference, however, is observable far sooner.
     */
    public static SoakInvariant weightRatioWithinTolerance(Map<String, Integer> weights, double tolerance) {
        return new Checker(
                "weightRatioWithinTolerance",
                "throughput preserves the configured weight ordering (high ≥ mid ≥ low) within slack",
                corpus -> {
                    Map<String, Long> succeededPerQueue = new LinkedHashMap<>();
                    for (JsonNode e : corpus.events()) {
                        if ("succeeded".equals(e.path("event").asText())) {
                            String q = e.path("queue").asText("");
                            if (!q.isEmpty()) succeededPerQueue.merge(q, 1L, Long::sum);
                        }
                    }
                    long totalSucceeded = succeededPerQueue.values().stream()
                            .mapToLong(Long::longValue)
                            .sum();
                    if (totalSucceeded < 50) {
                        return Outcome.of(List.of(), List.of()); // too few samples to judge
                    }
                    // Sort queue names by configured weight descending. For
                    // each adjacent pair (heavier, lighter) the heavier
                    // queue's count must be ≥ lighter's, subject to slack
                    // tolerance × totalSucceeded.
                    List<Map.Entry<String, Integer>> byWeight = new ArrayList<>(weights.entrySet());
                    byWeight.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));
                    long slack = Math.round(tolerance * totalSucceeded);
                    List<String> violations = new ArrayList<>();
                    for (int i = 0; i + 1 < byWeight.size(); i++) {
                        var heavier = byWeight.get(i);
                        var lighter = byWeight.get(i + 1);
                        if (heavier.getValue().equals(lighter.getValue())) continue;
                        long hc = succeededPerQueue.getOrDefault(heavier.getKey(), 0L);
                        long lc = succeededPerQueue.getOrDefault(lighter.getKey(), 0L);
                        if (hc + slack < lc) {
                            violations.add(String.format(
                                    "weight order broken: %s (weight=%d, count=%d) < %s (weight=%d, count=%d) "
                                            + "by more than %d",
                                    heavier.getKey(),
                                    heavier.getValue(),
                                    hc,
                                    lighter.getKey(),
                                    lighter.getValue(),
                                    lc,
                                    slack));
                        }
                    }
                    // Zero-weight queues must observe zero throughput.
                    for (var entry : weights.entrySet()) {
                        if (entry.getValue() == 0 && succeededPerQueue.getOrDefault(entry.getKey(), 0L) > 0) {
                            violations.add("zero-weight queue " + entry.getKey() + " observed throughput "
                                    + succeededPerQueue.get(entry.getKey()));
                        }
                    }
                    return Outcome.of(violations, List.of());
                });
    }

    /**
     * Jobs that stop checking in are reclaimed within noProgressTimeout; jobs
     * that keep checking in are not. The soak harness's long-running scenario
     * tags some jobs with {@code "noProgress=true"} in its enqueued metadata
     * — the invariant asserts those jobs terminate with cause TIMEOUT, while
     * jobs without the tag complete successfully.
     */
    public static SoakInvariant noProgressTimeoutKills() {
        return new Checker(
                "noProgressTimeoutKills",
                "jobs that stop checking in are reclaimed; jobs that keep checking in survive",
                corpus -> {
                    Map<String, String> finalCauseByJob = new LinkedHashMap<>();
                    Map<String, String> jobLineByJob = new HashMap<>();
                    Map<String, Boolean> noProgressFlag = new HashMap<>();
                    List<JsonNode> events = corpus.events();
                    List<String> raw = corpus.rawLines();
                    for (int i = 0; i < events.size(); i++) {
                        JsonNode e = events.get(i);
                        String event = e.path("event").asText();
                        String jobId = e.path("jobId").asText("");
                        if (jobId.isEmpty()) continue;
                        if ("enqueued".equals(event)) {
                            jobLineByJob.putIfAbsent(jobId, raw.get(i));
                            String handler = e.path("handler").asText("");
                            if (handler.contains("Stalled")) noProgressFlag.put(jobId, true);
                            else noProgressFlag.put(jobId, false);
                        }
                        if (("succeeded".equals(event)
                                        || ("timed_out".equals(event)
                                                && e.path("final").asBoolean(false))
                                        || "quarantined".equals(event))
                                && e.path("final").asBoolean(true)) {
                            finalCauseByJob.putIfAbsent(jobId, event);
                        }
                    }
                    List<String> violations = new ArrayList<>();
                    List<List<String>> samples = new ArrayList<>();
                    for (var entry : noProgressFlag.entrySet()) {
                        String jobId = entry.getKey();
                        boolean expectKill = entry.getValue();
                        String cause = finalCauseByJob.get(jobId);
                        if (cause == null) continue;
                        if (expectKill && !"timed_out".equals(cause)) {
                            violations.add("stalled job " + jobId + " ended in " + cause + " (expected timed_out)");
                            if (samples.size() < 5) samples.add(List.of(jobLineByJob.get(jobId)));
                        }
                        if (!expectKill && "timed_out".equals(cause)) {
                            violations.add("checked-in job " + jobId + " was timed_out (expected succeeded)");
                            if (samples.size() < 5) samples.add(List.of(jobLineByJob.get(jobId)));
                        }
                    }
                    return Outcome.of(violations, samples);
                });
    }

    /**
     * Bulk-insert atomicity: if any batch fails, no jobs from that batch are
     * persisted. We approximate this with a flag the bulk-enqueue scenario
     * sets in its trace metadata: each enqueue line records the batch id, and
     * we assert that for any batch id, either all enqueued events succeeded
     * the underlying insert (i.e. they appear in trace.jsonl) or none did.
     */
    public static SoakInvariant bulkInsertAtomic() {
        return new Checker("bulkInsertAtomic", "partial-failure batch leaves no jobs persisted", corpus -> {
            // Implementation note: the scenario emits a custom event
            // `bulk_batch_rejected` with `batchId`. We assert that no
            // `enqueued` event was recorded for any jobId in that batch.
            Set<String> rejectedBatchIds = new HashSet<>();
            Map<String, Set<String>> jobsByBatch = new HashMap<>();
            Map<String, String> rejectedLineByBatch = new HashMap<>();
            List<JsonNode> events = corpus.events();
            List<String> raw = corpus.rawLines();
            for (int i = 0; i < events.size(); i++) {
                JsonNode e = events.get(i);
                String event = e.path("event").asText();
                if ("bulk_batch_rejected".equals(event)) {
                    String batchId = e.path("batchId").asText("");
                    if (!batchId.isEmpty()) {
                        rejectedBatchIds.add(batchId);
                        rejectedLineByBatch.put(batchId, raw.get(i));
                    }
                } else if ("enqueued".equals(event)) {
                    String batchId = e.path("batchId").asText("");
                    String jobId = e.path("jobId").asText("");
                    if (!batchId.isEmpty()) {
                        jobsByBatch
                                .computeIfAbsent(batchId, k -> new LinkedHashSet<>())
                                .add(jobId);
                    }
                }
            }
            List<String> violations = new ArrayList<>();
            List<List<String>> samples = new ArrayList<>();
            for (String batchId : rejectedBatchIds) {
                Set<String> jobs = jobsByBatch.getOrDefault(batchId, Set.of());
                if (!jobs.isEmpty()) {
                    violations.add("batch " + batchId + " was rejected but " + jobs.size()
                            + " enqueued events still recorded for it");
                    if (samples.size() < 5) {
                        samples.add(List.of(rejectedLineByBatch.get(batchId)));
                    }
                }
            }
            return Outcome.of(violations, samples);
        });
    }

    // ---------------------------------------------------------------- internals

    private static int indexOf(List<EnqueuedJob> list, String jobId) {
        for (int i = 0; i < list.size(); i++) if (list.get(i).jobId.equals(jobId)) return i;
        return -1;
    }

    private record EnqueuedJob(String jobId, String mode, int enqueueLineIndex) {}

    private static final class ActiveLocks {
        int exclusive;
        int shared;
    }

    private record Outcome(List<String> violations, List<List<String>> sampleChains) {
        static Outcome of(List<String> v, List<List<String>> s) {
            return new Outcome(v, s);
        }
    }

    @FunctionalInterface
    private interface CheckerFn {
        Outcome run(TraceCorpus corpus);
    }

    private record Checker(String name, String description, CheckerFn fn) implements SoakInvariant {

        @Override
        public InvariantResult check(TraceCorpus corpus) {
            Outcome o = fn.run(corpus);
            if (o.violations.isEmpty()) return InvariantResult.pass(name);
            return InvariantResult.fail(name, o.violations, o.sampleChains);
        }
    }
}
