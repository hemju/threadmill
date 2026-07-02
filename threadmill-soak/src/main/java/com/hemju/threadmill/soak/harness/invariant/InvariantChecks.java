package com.hemju.threadmill.soak.harness.invariant;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
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

    /**
     * No EXCLUSIVE handler <em>executes</em> concurrently with another handler
     * for the same key. Judged on the handler-emitted {@code exec_started} /
     * {@code exec_finished} brackets, never on the interceptor lock events:
     * interceptor hooks fire only after the store transition committed, so a
     * legal per-key handoff between workers can appear as a microsecond
     * lock-event overlap (the false positive that aborted the first real
     * endurance run). Bracket events are written while the handler runs and
     * the trace writer totally orders them, so an observed bracket overlap is
     * a real execution overlap by construction. Violations are definite.
     */
    public static SoakInvariant exclusivityHeld() {
        return new Def(
                "exclusivityHeld",
                "EXCLUSIVE handlers execute alone for their key",
                () -> new StreamingInvariantCheck("exclusivityHeld") {
                    private final Map<String, KeyedJob> keyedJobById = new HashMap<>();
                    private final Map<String, RunningExec> runningByJob = new HashMap<>();
                    private final Map<String, ActiveLocks> runningByKey = new HashMap<>();
                    private final Map<String, String> lastStartLineByKey = new HashMap<>();

                    @Override
                    protected void observe(TraceEvent e) {
                        String event = e.event();
                        String jobId = e.text("jobId");
                        if (jobId.isEmpty()) return;
                        switch (event) {
                            case "enqueued" -> {
                                String key = e.text("lockKey");
                                if (!key.isEmpty()) {
                                    keyedJobById.putIfAbsent(jobId, new KeyedJob(key, e.text("lockMode")));
                                }
                            }
                            case "exec_started" -> {
                                KeyedJob keyed = keyedJobById.get(jobId);
                                if (keyed == null) return; // unkeyed job — nothing to enforce
                                ActiveLocks a = runningByKey.computeIfAbsent(keyed.key, k -> new ActiveLocks());
                                if ("EXCLUSIVE".equals(keyed.mode) && (a.exclusive > 0 || a.shared > 0)) {
                                    recordViolation(
                                            "EXCLUSIVE handler for " + keyed.key
                                                    + " started while other handlers execute (exclusive=" + a.exclusive
                                                    + ", shared=" + a.shared + ")",
                                            sampleWith(keyed.key, e));
                                }
                                if ("SHARED".equals(keyed.mode) && a.exclusive > 0) {
                                    recordViolation(
                                            "SHARED handler for " + keyed.key
                                                    + " started while an EXCLUSIVE executes (exclusive=" + a.exclusive
                                                    + ")",
                                            sampleWith(keyed.key, e));
                                }
                                if ("EXCLUSIVE".equals(keyed.mode)) a.exclusive++;
                                else if ("SHARED".equals(keyed.mode)) a.shared++;
                                runningByJob.put(jobId, new RunningExec(keyed.key, keyed.mode));
                                lastStartLineByKey.put(keyed.key, e.rawLine());
                            }
                            case "exec_finished" -> {
                                RunningExec running = runningByJob.remove(jobId);
                                if (running == null) return;
                                ActiveLocks a = runningByKey.get(running.key);
                                if (a == null) return;
                                if ("EXCLUSIVE".equals(running.mode)) a.exclusive = Math.max(0, a.exclusive - 1);
                                else if ("SHARED".equals(running.mode)) a.shared = Math.max(0, a.shared - 1);
                                if (a.exclusive == 0 && a.shared == 0) {
                                    runningByKey.remove(running.key);
                                    lastStartLineByKey.remove(running.key);
                                }
                            }
                            default -> {
                                if (isTerminal(e)) keyedJobById.remove(jobId);
                            }
                        }
                    }

                    private List<String> sampleWith(String key, TraceEvent e) {
                        String earlier = lastStartLineByKey.get(key);
                        return earlier == null ? List.of(e.rawLine()) : List.of(earlier, e.rawLine());
                    }
                });
    }

    /**
     * For any key, a SHARED handler never <em>starts executing</em> before an
     * earlier-<em>created</em> EXCLUSIVE has finished executing. The check
     * keeps, per key, the set of jobs that have not yet finished, ordered by
     * <em>job id</em> — UUIDv7 ids embed the creation millisecond, so their
     * canonical strings sort chronologically. A SHARED {@code exec_started}
     * walks the ids ahead of itself and flags any not-yet-executed EXCLUSIVE
     * it leapfrogged. State is bounded by backlog.
     *
     * <p>The book deliberately does NOT use trace emission order: the
     * {@code enqueued} event is emitted after the insert commits, and with
     * concurrent producers ({@code -Pproducers}) insert latencies of seconds
     * decouple emission order from creation order entirely — the first
     * multi-producer stress run false-aborted on SHAREDs that were provably
     * (by id) created before the EXCLUSIVE they "leapfrogged".
     *
     * <p>Three shapes are excused, all grounded in the engine's contract, not
     * in timing heuristics:
     *
     * <ul>
     *   <li>An EXCLUSIVE whose id-embedded creation millisecond is within
     *       {@value #SAME_INSTANT_MARGIN_MS}ms of the SHARED's: the engine
     *       orders by {@code (current_state_at, id)} at microsecond
     *       precision, which the trace cannot observe inside that band — and
     *       a real leapfrog leaves the EXCLUSIVE pending for far longer.</li>
     *   <li>An EXCLUSIVE that already emitted {@code exec_finished} is done
     *       for ordering purposes — its terminal save (which actually frees
     *       the key) strictly follows that emission, so a SHARED admitted
     *       after the save always observes the bracket closed. This is what
     *       makes the check immune to the interceptor-hook emission lag.</li>
     *   <li>A <em>retried</em> job leaves the order book: a retry
     *       legitimately re-times the job in the pending order. Because the
     *       retry hook's {@code retried} emission races the next claimant's
     *       events (both follow the same reclaim save), a suspected leapfrog
     *       is held as provisional for {@code RETRY_EXCUSE_WINDOW} and
     *       cancelled if the leapfrogged EXCLUSIVE's {@code retried} event
     *       arrives inside it; otherwise it is promoted to a definite
     *       violation.</li>
     * </ul>
     */
    public static SoakInvariant strictInGroupOrder() {
        return new Def(
                "strictInGroupOrder",
                "SHARED never executes before an earlier-enqueued EXCLUSIVE for the same key",
                () -> new StreamingInvariantCheck("strictInGroupOrder") {
                    private final Map<String, TreeMap<String, PendingJob>> pendingByKey = new HashMap<>();
                    private final Map<String, String> keyByJob = new HashMap<>();
                    private final List<ProvisionalLeapfrog> provisional = new ArrayList<>();

                    @Override
                    protected void observe(TraceEvent e) {
                        String event = e.event();
                        String jobId = e.text("jobId");
                        switch (event) {
                            case "enqueued" -> {
                                String key = e.text("lockKey");
                                if (key.isEmpty() || jobId.isEmpty()) return;
                                pendingByKey
                                        .computeIfAbsent(key, k -> new TreeMap<>())
                                        .putIfAbsent(jobId, new PendingJob(e.text("lockMode"), e.rawLine()));
                                keyByJob.putIfAbsent(jobId, key);
                            }
                            case "exec_started" -> {
                                String key = keyByJob.get(jobId);
                                if (key == null) return;
                                var pending = pendingByKey.get(key);
                                if (pending == null || !pending.containsKey(jobId)) return;
                                PendingJob self = pending.get(jobId);
                                if (!"SHARED".equals(self.mode)) return;
                                long sharedCreatedMs = uuidV7Millis(jobId);
                                for (var entry : pending.headMap(jobId).entrySet()) {
                                    PendingJob earlier = entry.getValue();
                                    if (!"EXCLUSIVE".equals(earlier.mode) || earlier.executionDone) continue;
                                    if (!createdClearlyBefore(uuidV7Millis(entry.getKey()), sharedCreatedMs)) continue;
                                    provisional.add(new ProvisionalLeapfrog(
                                            entry.getKey(),
                                            Instant.parse(e.text("timestamp")),
                                            "job " + jobId + " (SHARED on " + key
                                                    + ") executed before earlier EXCLUSIVE " + entry.getKey(),
                                            List.of(earlier.enqueueLine, e.rawLine())));
                                    break;
                                }
                            }
                            case "exec_finished" -> {
                                String key = keyByJob.get(jobId);
                                if (key == null) return;
                                var pending = pendingByKey.get(key);
                                if (pending == null) return;
                                PendingJob job = pending.get(jobId);
                                if (job != null) job.executionDone = true;
                            }
                            case "retried" -> {
                                // Excuse suspicions about this job raised inside the
                                // race window — the reclaim save that re-timed it also
                                // admitted the "leapfrogging" SHARED.
                                Instant ts = Instant.parse(e.text("timestamp"));
                                provisional.removeIf(p -> p.exclusiveJobId.equals(jobId)
                                        && !ts.isAfter(p.observedAt.plus(RETRY_EXCUSE_WINDOW)));
                                removeFromBook(jobId);
                            }
                            default -> {
                                if (isTerminal(e)) removeFromBook(jobId);
                            }
                        }
                        promoteExpired(e);
                    }

                    @Override
                    protected void onFinish() {
                        for (ProvisionalLeapfrog p : provisional) {
                            recordViolation(p.message, p.sampleChain);
                        }
                        provisional.clear();
                    }

                    private void removeFromBook(String jobId) {
                        String key = keyByJob.remove(jobId);
                        if (key == null) return;
                        var pending = pendingByKey.get(key);
                        if (pending == null) return;
                        pending.remove(jobId);
                        if (pending.isEmpty()) pendingByKey.remove(key);
                    }

                    /** A suspicion not excused within the window is a definite violation. */
                    private void promoteExpired(TraceEvent e) {
                        if (provisional.isEmpty()) return;
                        Instant now;
                        try {
                            now = Instant.parse(e.text("timestamp"));
                        } catch (RuntimeException malformed) {
                            return;
                        }
                        var it = provisional.iterator();
                        while (it.hasNext()) {
                            ProvisionalLeapfrog p = it.next();
                            if (now.isAfter(p.observedAt.plus(RETRY_EXCUSE_WINDOW))) {
                                recordViolation(p.message, p.sampleChain);
                                it.remove();
                            }
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
     * How long a suspected leapfrog stays provisional, waiting for the
     * leapfrogged EXCLUSIVE's {@code retried} event to excuse it. The race it
     * absorbs is emission-scheduling jitter between two threads that both
     * already committed against the store — microseconds normally, GC pauses
     * worst case. A real leapfrog leaves the EXCLUSIVE pending far longer.
     */
    private static final Duration RETRY_EXCUSE_WINDOW = Duration.ofSeconds(1);

    /**
     * Two jobs created within this many milliseconds of each other have no
     * trace-observable order: the engine ranks them by microsecond-precision
     * {@code current_state_at} (id tie-break), the trace only sees the id's
     * millisecond prefix. A real leapfrog leaves the EXCLUSIVE pending far
     * longer than this.
     */
    private static final long SAME_INSTANT_MARGIN_MS = 2;

    /**
     * The 48-bit creation-millis prefix of a canonical UUIDv7 string, or -1
     * when the id has another shape (hand-written ids in tests).
     */
    private static long uuidV7Millis(String jobId) {
        if (jobId.length() < 13 || jobId.charAt(8) != '-') return -1;
        try {
            long high = Long.parseLong(jobId.substring(0, 8), 16);
            long low = Long.parseLong(jobId.substring(9, 13), 16);
            return (high << 16) | low;
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /**
     * True when the EXCLUSIVE was created clearly before the SHARED — beyond
     * the same-instant margin the trace cannot resolve. Ids without a UUIDv7
     * timestamp compare pessimistically (book order alone decides), which
     * keeps hand-written test traces strict.
     */
    private static boolean createdClearlyBefore(long exclusiveCreatedMs, long sharedCreatedMs) {
        if (exclusiveCreatedMs < 0 || sharedCreatedMs < 0) return true;
        return exclusiveCreatedMs + SAME_INSTANT_MARGIN_MS <= sharedCreatedMs;
    }

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

    private static final class PendingJob {
        final String mode;
        final String enqueueLine;
        boolean executionDone;

        PendingJob(String mode, String enqueueLine) {
            this.mode = mode;
            this.enqueueLine = enqueueLine;
        }
    }

    private record KeyedJob(String key, String mode) {}

    private record RunningExec(String key, String mode) {}

    private record ProvisionalLeapfrog(
            String exclusiveJobId, Instant observedAt, String message, List<String> sampleChain) {}

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
