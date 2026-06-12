package com.hemju.threadmill.soak.harness.invariant;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Library of named invariant checks.
 *
 * <p>Every factory method returns a {@link SoakInvariant} whose
 * {@link SoakInvariant#newCheck() checks} consume the trace as a stream with
 * state bounded by in-flight work, never by run length — the same definitions
 * verify a five-second smoke run and an eight-hour endurance run. Scenarios
 * pick the subset that applies to their workload.
 */
public final class InvariantChecks {

    private InvariantChecks() {}

    /**
     * Every enqueued job reaches a terminal event. A completeness check: jobs
     * still open mid-run are not violations, only jobs left open at end of run.
     */
    public static SoakInvariant atLeastOnce() {
        return new Def(
                "atLeastOnce",
                "every enqueued job reaches a terminal state",
                () -> new StreamingInvariantCheck("atLeastOnce") {
                    private final Map<String, String> enqueueLineByOpenJob = new LinkedHashMap<>();

                    @Override
                    protected void observe(TraceEvent e) {
                        String jobId = e.text("jobId");
                        if (jobId.isEmpty()) return;
                        if ("enqueued".equals(e.event())) {
                            enqueueLineByOpenJob.putIfAbsent(jobId, e.rawLine());
                        } else if (isTerminal(e)) {
                            enqueueLineByOpenJob.remove(jobId);
                        }
                    }

                    @Override
                    protected void onFinish() {
                        for (var entry : enqueueLineByOpenJob.entrySet()) {
                            recordViolation(
                                    "job " + entry.getKey() + " was enqueued but never reached a terminal event",
                                    List.of(entry.getValue()));
                        }
                    }
                });
    }

    /** No EXCLUSIVE lock overlaps with another lock for the same key. Violations are definite. */
    public static SoakInvariant exclusivityHeld() {
        return new Def(
                "exclusivityHeld",
                "EXCLUSIVE locks run alone for their key",
                () -> new StreamingInvariantCheck("exclusivityHeld") {
                    private final Map<String, ActiveLocks> activeByKey = new HashMap<>();

                    @Override
                    protected void observe(TraceEvent e) {
                        String key = e.text("lockKey");
                        if (key.isEmpty()) return;
                        String event = e.event();
                        String mode = e.text("lockMode");
                        if ("lock_acquired".equals(event)) {
                            ActiveLocks a = activeByKey.computeIfAbsent(key, k -> new ActiveLocks());
                            if ("EXCLUSIVE".equals(mode) && (a.exclusive > 0 || a.shared > 0)) {
                                recordViolation(
                                        "EXCLUSIVE lock on " + key + " acquired while other locks active (exclusive="
                                                + a.exclusive + ", shared=" + a.shared + ")",
                                        List.of(e.rawLine()));
                            }
                            if ("SHARED".equals(mode) && a.exclusive > 0) {
                                recordViolation(
                                        "SHARED lock on " + key + " acquired while EXCLUSIVE active (exclusive="
                                                + a.exclusive + ")",
                                        List.of(e.rawLine()));
                            }
                            if ("EXCLUSIVE".equals(mode)) a.exclusive++;
                            else if ("SHARED".equals(mode)) a.shared++;
                        } else if ("lock_released".equals(event)) {
                            ActiveLocks a = activeByKey.get(key);
                            if (a == null) return;
                            if ("EXCLUSIVE".equals(mode)) a.exclusive = Math.max(0, a.exclusive - 1);
                            else if ("SHARED".equals(mode)) a.shared = Math.max(0, a.shared - 1);
                            if (a.exclusive == 0 && a.shared == 0) activeByKey.remove(key);
                        }
                    }
                });
    }

    /**
     * For any key, a SHARED claim never precedes the completion of an
     * earlier-enqueued EXCLUSIVE. The check keeps, per key, the enqueue-ordered
     * set of jobs that have not yet finished; a SHARED acquire walks the
     * pending order ahead of itself and flags any EXCLUSIVE it leapfrogged.
     * Finished jobs are pruned immediately, so state is bounded by backlog.
     *
     * <p>A <em>retried</em> job leaves the order book entirely: the engine's
     * in-key pending order is {@code (current_state_at, id)}, so a retry
     * legitimately re-times the job — first at the SCHEDULED transition, then
     * again at its ENQUEUED promotion — and neither instant is observable
     * from the trace. Asserting order against a retried job would assert the
     * harness's guess, not the engine's contract; {@code exclusivityHeld}
     * still guarantees retried EXCLUSIVE jobs run alone.
     */
    public static SoakInvariant strictInGroupOrder() {
        return new Def(
                "strictInGroupOrder",
                "SHARED never leapfrogs an earlier-enqueued EXCLUSIVE for the same key",
                () -> new StreamingInvariantCheck("strictInGroupOrder") {
                    private final Map<String, LinkedHashMap<String, PendingJob>> pendingByKey = new HashMap<>();
                    private final Map<String, String> keyByJob = new HashMap<>();

                    @Override
                    protected void observe(TraceEvent e) {
                        String event = e.event();
                        String jobId = e.text("jobId");
                        if ("enqueued".equals(event)) {
                            String key = e.text("lockKey");
                            if (key.isEmpty() || jobId.isEmpty()) return;
                            pendingByKey
                                    .computeIfAbsent(key, k -> new LinkedHashMap<>())
                                    .putIfAbsent(jobId, new PendingJob(e.text("lockMode"), e.rawLine()));
                            keyByJob.putIfAbsent(jobId, key);
                        } else if ("lock_acquired".equals(event) && "SHARED".equals(e.text("lockMode"))) {
                            String key = e.text("lockKey");
                            var pending = pendingByKey.get(key);
                            if (pending == null || !pending.containsKey(jobId)) return;
                            for (var entry : pending.entrySet()) {
                                if (entry.getKey().equals(jobId)) break;
                                PendingJob earlier = entry.getValue();
                                if ("EXCLUSIVE".equals(earlier.mode)) {
                                    recordViolation(
                                            "job " + jobId + " (SHARED on " + key + ") leapfrogged earlier EXCLUSIVE "
                                                    + entry.getKey(),
                                            List.of(earlier.enqueueLine, e.rawLine()));
                                    break;
                                }
                            }
                        } else if ("retried".equals(event) || isTerminal(e)) {
                            String key = keyByJob.remove(jobId);
                            if (key == null) return;
                            var pending = pendingByKey.get(key);
                            if (pending == null) return;
                            pending.remove(jobId);
                            if (pending.isEmpty()) pendingByKey.remove(key);
                        }
                    }
                });
    }

    /**
     * Workflow concurrency holds are unbroken between the root acquire and
     * the last descendant release. (Soak does not run workflow scenarios in
     * v1 — the check is a no-op if no workflow_root_id metadata appears, but
     * the registration slot is here so future workflow scenarios get coverage
     * for free.)
     */
    public static SoakInvariant workflowLockHeldContinuously() {
        return new Def(
                "workflowLockHeldContinuously",
                "workflow concurrency hold is unbroken from root acquire to last descendant release",
                () -> new StreamingInvariantCheck("workflowLockHeldContinuously") {
                    @Override
                    protected void observe(TraceEvent e) {}
                });
    }

    /**
     * No job exceeds {@code maxAttempts} (scenarios pass their configured
     * ceiling). A definite violation, flagged the moment an over-budget claim
     * appears; healthy runs hold no state at all.
     */
    public static SoakInvariant retryBudgetRespected(int maxAttempts) {
        return new Def(
                "retryBudgetRespected",
                "no job runs more than " + maxAttempts + " attempts",
                () -> new StreamingInvariantCheck("retryBudgetRespected") {
                    private final Set<String> flagged = new HashSet<>();

                    @Override
                    protected void observe(TraceEvent e) {
                        if (!"claimed".equals(e.event())) return;
                        String jobId = e.text("jobId");
                        if (jobId.isEmpty()) return;
                        int attempt = e.intField("attempt", 0);
                        if (attempt > maxAttempts && flagged.add(jobId)) {
                            recordViolation(
                                    "job " + jobId + " ran " + attempt + " attempts (limit " + maxAttempts + ")",
                                    List.of(e.rawLine()));
                        }
                    }
                });
    }

    /**
     * Every lock_acquired has a matching lock_released by run end. A
     * completeness check: locks held mid-run are expected; only jobs whose
     * acquire/release counts stay unbalanced at end of run are violations.
     */
    public static SoakInvariant noLockLeaks() {
        return new Def(
                "noLockLeaks",
                "every lock_acquired has a matching lock_released",
                () -> new StreamingInvariantCheck("noLockLeaks") {
                    private final Map<String, LockBalance> openByJob = new LinkedHashMap<>();

                    @Override
                    protected void observe(TraceEvent e) {
                        String event = e.event();
                        String jobId = e.text("jobId");
                        if (jobId.isEmpty()) return;
                        if ("lock_acquired".equals(event)) {
                            LockBalance b = openByJob.computeIfAbsent(jobId, k -> new LockBalance(e.rawLine()));
                            b.acquires++;
                        } else if ("lock_released".equals(event)) {
                            LockBalance b = openByJob.computeIfAbsent(jobId, k -> new LockBalance(e.rawLine()));
                            b.releases++;
                            // Balanced means this attempt's bracket closed cleanly;
                            // drop the entry so state stays bounded by in-flight work.
                            if (b.releases == b.acquires) openByJob.remove(jobId);
                        }
                    }

                    @Override
                    protected void onFinish() {
                        for (var entry : openByJob.entrySet()) {
                            LockBalance b = entry.getValue();
                            recordViolation(
                                    "job " + entry.getKey() + " has acquire=" + b.acquires + " release=" + b.releases,
                                    List.of(b.firstLine));
                        }
                    }
                });
    }

    /** Between a queue_paused and the matching queue_resumed, no claimed names that queue. Definite. */
    public static SoakInvariant pauseObeyed() {
        return new Def(
                "pauseObeyed",
                "no claimed event names a queue between its pause/resume bracket",
                () -> new StreamingInvariantCheck("pauseObeyed") {
                    private final Map<String, Instant> pausedAt = new HashMap<>();

                    @Override
                    protected void observe(TraceEvent e) {
                        String event = e.event();
                        String queue = e.text("queue");
                        if ("queue_paused".equals(event)) {
                            pausedAt.put(queue, Instant.parse(e.text("timestamp")));
                        } else if ("queue_resumed".equals(event)) {
                            pausedAt.remove(queue);
                        } else if ("claimed".equals(event) && pausedAt.containsKey(queue)) {
                            recordViolation(
                                    "claim on paused queue " + queue + " at " + e.text("timestamp") + " (paused at "
                                            + pausedAt.get(queue) + ")",
                                    List.of(e.rawLine()));
                        }
                    }
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
     * directional preference, however, is observable far sooner. Evaluated
     * only at end of run; cumulative counts mid-run are still converging.
     */
    public static SoakInvariant weightRatioWithinTolerance(Map<String, Integer> weights, double tolerance) {
        Map<String, Integer> weightsCopy = Map.copyOf(weights);
        return new Def(
                "weightRatioWithinTolerance",
                "throughput preserves the configured weight ordering (high ≥ mid ≥ low) within slack",
                () -> new StreamingInvariantCheck("weightRatioWithinTolerance") {
                    private final Map<String, Long> succeededPerQueue = new LinkedHashMap<>();

                    @Override
                    protected void observe(TraceEvent e) {
                        if (!"succeeded".equals(e.event())) return;
                        String queue = e.text("queue");
                        if (!queue.isEmpty()) succeededPerQueue.merge(queue, 1L, Long::sum);
                    }

                    @Override
                    protected void onFinish() {
                        long totalSucceeded = succeededPerQueue.values().stream()
                                .mapToLong(Long::longValue)
                                .sum();
                        if (totalSucceeded < 50) return; // too few samples to judge
                        List<Map.Entry<String, Integer>> byWeight = new ArrayList<>(weightsCopy.entrySet());
                        byWeight.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));
                        long slack = Math.round(tolerance * totalSucceeded);
                        for (int i = 0; i + 1 < byWeight.size(); i++) {
                            var heavier = byWeight.get(i);
                            var lighter = byWeight.get(i + 1);
                            if (heavier.getValue().equals(lighter.getValue())) continue;
                            long hc = succeededPerQueue.getOrDefault(heavier.getKey(), 0L);
                            long lc = succeededPerQueue.getOrDefault(lighter.getKey(), 0L);
                            if (hc + slack < lc) {
                                recordViolation(
                                        String.format(
                                                "weight order broken: %s (weight=%d, count=%d) < %s (weight=%d, "
                                                        + "count=%d) by more than %d",
                                                heavier.getKey(),
                                                heavier.getValue(),
                                                hc,
                                                lighter.getKey(),
                                                lighter.getValue(),
                                                lc,
                                                slack),
                                        List.of());
                            }
                        }
                        for (var entry : weightsCopy.entrySet()) {
                            if (entry.getValue() == 0 && succeededPerQueue.getOrDefault(entry.getKey(), 0L) > 0) {
                                recordViolation(
                                        "zero-weight queue " + entry.getKey() + " observed throughput "
                                                + succeededPerQueue.get(entry.getKey()),
                                        List.of());
                            }
                        }
                    }
                });
    }

    /**
     * Jobs that stop checking in are reclaimed within noProgressTimeout; jobs
     * that keep checking in are not. The soak harness's long-running scenario
     * uses a {@code Stalled}-named handler for the former — each job is judged
     * the moment its final event arrives, then its state is dropped.
     */
    public static SoakInvariant noProgressTimeoutKills() {
        return new Def(
                "noProgressTimeoutKills",
                "jobs that stop checking in are reclaimed; jobs that keep checking in survive",
                () -> new StreamingInvariantCheck("noProgressTimeoutKills") {
                    private final Map<String, Expectation> openByJob = new HashMap<>();

                    @Override
                    protected void observe(TraceEvent e) {
                        String event = e.event();
                        String jobId = e.text("jobId");
                        if (jobId.isEmpty()) return;
                        if ("enqueued".equals(event)) {
                            boolean expectKill = e.text("handler").contains("Stalled");
                            openByJob.putIfAbsent(jobId, new Expectation(expectKill, e.rawLine()));
                            return;
                        }
                        boolean isFinalCause = ("succeeded".equals(event)
                                        || "quarantined".equals(event)
                                        || ("timed_out".equals(event) && e.boolField("final", false)))
                                && e.boolField("final", true);
                        if (!isFinalCause) return;
                        Expectation expectation = openByJob.remove(jobId);
                        if (expectation == null) return;
                        if (expectation.expectKill && !"timed_out".equals(event)) {
                            recordViolation(
                                    "stalled job " + jobId + " ended in " + event + " (expected timed_out)",
                                    List.of(expectation.enqueueLine));
                        }
                        if (!expectation.expectKill && "timed_out".equals(event)) {
                            recordViolation(
                                    "checked-in job " + jobId + " was timed_out (expected succeeded)",
                                    List.of(expectation.enqueueLine));
                        }
                    }
                });
    }

    /**
     * Bulk-insert atomicity: a rejected batch leaves no jobs persisted. The
     * bulk-enqueue scenario brackets each batch with
     * {@code bulk_batch_committed} / {@code bulk_batch_rejected} events;
     * committed batches are dropped from state immediately, so memory is
     * bounded by in-flight batches.
     */
    public static SoakInvariant bulkInsertAtomic() {
        return new Def(
                "bulkInsertAtomic",
                "partial-failure batch leaves no jobs persisted",
                () -> new StreamingInvariantCheck("bulkInsertAtomic") {
                    private final Map<String, Long> enqueuedByOpenBatch = new HashMap<>();
                    private final Set<String> rejectedBatchIds = new HashSet<>();

                    @Override
                    protected void observe(TraceEvent e) {
                        String event = e.event();
                        String batchId = e.text("batchId");
                        if (batchId.isEmpty()) return;
                        switch (event) {
                            case "enqueued" -> {
                                if (rejectedBatchIds.contains(batchId)) {
                                    recordViolation(
                                            "batch " + batchId
                                                    + " was rejected but an enqueued event was still recorded for it",
                                            List.of(e.rawLine()));
                                } else {
                                    enqueuedByOpenBatch.merge(batchId, 1L, Long::sum);
                                }
                            }
                            case "bulk_batch_rejected" -> {
                                rejectedBatchIds.add(batchId);
                                Long enqueued = enqueuedByOpenBatch.remove(batchId);
                                if (enqueued != null && enqueued > 0) {
                                    recordViolation(
                                            "batch " + batchId + " was rejected but " + enqueued
                                                    + " enqueued events still recorded for it",
                                            List.of(e.rawLine()));
                                }
                            }
                            case "bulk_batch_committed" -> enqueuedByOpenBatch.remove(batchId);
                            default -> {
                                // other events carry no batch bookkeeping
                            }
                        }
                    }
                });
    }

    // ---------------------------------------------------------------- internals

    /**
     * A terminal lifecycle event: {@code succeeded}, {@code quarantined}, or a
     * {@code failed} / {@code timed_out} marked {@code final} (a non-final
     * failure means the retry interceptor scheduled another attempt).
     */
    private static boolean isTerminal(TraceEvent e) {
        String event = e.event();
        return "succeeded".equals(event)
                || "quarantined".equals(event)
                || (("failed".equals(event) || "timed_out".equals(event)) && e.boolField("final", false));
    }

    private record PendingJob(String mode, String enqueueLine) {}

    private record Expectation(boolean expectKill, String enqueueLine) {}

    private static final class ActiveLocks {
        int exclusive;
        int shared;
    }

    private static final class LockBalance {
        final String firstLine;
        int acquires;
        int releases;

        LockBalance(String firstLine) {
            this.firstLine = firstLine;
        }
    }

    private record Def(String name, String description, Supplier<StreamingInvariantCheck> factory)
            implements SoakInvariant {

        private Def {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(description, "description");
            Objects.requireNonNull(factory, "factory");
        }

        @Override
        public StreamingInvariantCheck newCheck() {
            return factory.get();
        }
    }
}
