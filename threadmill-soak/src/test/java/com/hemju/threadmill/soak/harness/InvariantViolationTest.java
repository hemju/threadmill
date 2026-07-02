package com.hemju.threadmill.soak.harness;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.hemju.threadmill.soak.harness.invariant.InvariantChecks;
import com.hemju.threadmill.soak.harness.invariant.InvariantResult;
import com.hemju.threadmill.soak.harness.invariant.SoakInvariant;
import com.hemju.threadmill.soak.harness.invariant.StreamingInvariantCheck;
import com.hemju.threadmill.soak.harness.invariant.TraceEvent;
import com.hemju.threadmill.soak.harness.invariant.TraceReplay;

/**
 * Confirms the invariant checker actually fires on a deliberately bad trace.
 *
 * <p>If we only ever exercise the green path, a regression that silently
 * disables a checker (e.g. early-returns before a violation is recorded)
 * would pass every other test. This is the red-path coverage, exercised
 * end-to-end through the on-disk trace format and the streaming replay the
 * harness itself uses.
 */
final class InvariantViolationTest {

    @Test
    void noLockLeaksFiresWhenAcquireHasNoMatchingRelease(@TempDir Path tempDir) throws Exception {
        Path traceFile = tempDir.resolve("trace.jsonl");
        try (SoakTraceWriter writer = new SoakTraceWriter(traceFile)) {
            writer.emit(
                    "enqueued", Map.of("jobId", "job-1", "queue", "default", "lockKey", "k1", "lockMode", "SHARED"));
            writer.emit("claimed", Map.of("jobId", "job-1", "queue", "default", "attempt", 1));
            writer.emit("started", Map.of("jobId", "job-1", "queue", "default", "attempt", 1));
            writer.emit("lock_acquired", Map.of("jobId", "job-1", "lockKey", "k1", "lockMode", "SHARED"));
            // … no lock_released for job-1 — the leak this test asserts against.
        }
        InvariantResult result = verify(traceFile, InvariantChecks.noLockLeaks());
        assertThat(result.passed()).isFalse();
        assertThat(result.violations()).anyMatch(v -> v.contains("job-1"));
    }

    @Test
    void noLockLeaksPassesWhenRetryRebalancesAcrossAttempts(@TempDir Path tempDir) throws Exception {
        Path traceFile = tempDir.resolve("trace.jsonl");
        try (SoakTraceWriter writer = new SoakTraceWriter(traceFile)) {
            writer.emit("lock_acquired", Map.of("jobId", "job-1", "lockKey", "k1", "lockMode", "EXCLUSIVE"));
            writer.emit("lock_released", Map.of("jobId", "job-1", "lockKey", "k1", "lockMode", "EXCLUSIVE"));
            // Second attempt of the same job re-acquires and re-releases.
            writer.emit("lock_acquired", Map.of("jobId", "job-1", "lockKey", "k1", "lockMode", "EXCLUSIVE"));
            writer.emit("lock_released", Map.of("jobId", "job-1", "lockKey", "k1", "lockMode", "EXCLUSIVE"));
        }
        assertThat(verify(traceFile, InvariantChecks.noLockLeaks()).passed()).isTrue();
    }

    @Test
    void exclusivityHeldFiresWhenHandlersExecuteConcurrently(@TempDir Path tempDir) throws Exception {
        Path traceFile = tempDir.resolve("trace.jsonl");
        try (SoakTraceWriter writer = new SoakTraceWriter(traceFile)) {
            writer.emit("enqueued", Map.of("jobId", "job-1", "queue", "q", "lockKey", "k", "lockMode", "SHARED"));
            writer.emit("enqueued", Map.of("jobId", "job-2", "queue", "q", "lockKey", "k", "lockMode", "EXCLUSIVE"));
            writer.emit("exec_started", Map.of("jobId", "job-1", "attempt", 1));
            // EXCLUSIVE handler starts while the SHARED bracket is still open —
            // a real concurrent execution, the thing this invariant exists for.
            writer.emit("exec_started", Map.of("jobId", "job-2", "attempt", 1));
            writer.emit("exec_finished", Map.of("jobId", "job-2", "attempt", 1));
            writer.emit("exec_finished", Map.of("jobId", "job-1", "attempt", 1));
        }
        InvariantResult result = verify(traceFile, InvariantChecks.exclusivityHeld());
        assertThat(result.passed()).isFalse();
        assertThat(result.violations()).isNotEmpty();
    }

    @Test
    void exclusivityHeldIgnoresInterceptorLockEventHandoffOverlap(@TempDir Path tempDir) throws Exception {
        // Regression for the aborted endurance run: interceptor hooks fire
        // after the store transition commits, so a legal per-key handoff can
        // trace the outgoing SHARED's lock_released a few microseconds AFTER
        // the incoming EXCLUSIVE's lock_acquired. The exec brackets are
        // sequential — no execution overlap — and the check must judge those.
        Path traceFile = tempDir.resolve("trace.jsonl");
        try (SoakTraceWriter writer = new SoakTraceWriter(traceFile)) {
            writer.emit("enqueued", Map.of("jobId", "job-s", "queue", "q", "lockKey", "k", "lockMode", "SHARED"));
            writer.emit("enqueued", Map.of("jobId", "job-e", "queue", "q", "lockKey", "k", "lockMode", "EXCLUSIVE"));
            writer.emit("lock_acquired", Map.of("jobId", "job-s", "lockKey", "k", "lockMode", "SHARED"));
            writer.emit("exec_started", Map.of("jobId", "job-s", "attempt", 1));
            writer.emit("exec_finished", Map.of("jobId", "job-s", "attempt", 1));
            // Store handoff happened here; the two threads' trailing emissions interleave.
            writer.emit("lock_acquired", Map.of("jobId", "job-e", "lockKey", "k", "lockMode", "EXCLUSIVE"));
            writer.emit("succeeded", Map.of("jobId", "job-s", "queue", "q", "attempts", 1, "final", true));
            writer.emit("lock_released", Map.of("jobId", "job-s", "lockKey", "k", "lockMode", "SHARED"));
            writer.emit("exec_started", Map.of("jobId", "job-e", "attempt", 1));
            writer.emit("exec_finished", Map.of("jobId", "job-e", "attempt", 1));
            writer.emit("succeeded", Map.of("jobId", "job-e", "queue", "q", "attempts", 1, "final", true));
            writer.emit("lock_released", Map.of("jobId", "job-e", "lockKey", "k", "lockMode", "EXCLUSIVE"));
        }
        assertThat(verify(traceFile, InvariantChecks.exclusivityHeld()).passed())
                .isTrue();
    }

    @Test
    void atLeastOnceFiresOnAJobThatNeverReachesATerminalEvent(@TempDir Path tempDir) throws Exception {
        Path traceFile = tempDir.resolve("trace.jsonl");
        try (SoakTraceWriter writer = new SoakTraceWriter(traceFile)) {
            writer.emit("enqueued", Map.of("jobId", "job-orphan", "queue", "default"));
            // No subsequent claimed/succeeded — the job stayed enqueued forever.
            writer.emit("enqueued", Map.of("jobId", "job-completed", "queue", "default"));
            writer.emit(
                    "succeeded", Map.of("jobId", "job-completed", "queue", "default", "attempts", 1, "final", true));
        }
        InvariantResult result = verify(traceFile, InvariantChecks.atLeastOnce());
        assertThat(result.passed()).isFalse();
        assertThat(result.violations()).anyMatch(v -> v.contains("job-orphan"));
    }

    @Test
    void strictInGroupOrderFiresWhenSharedLeapfrogsEarlierExclusive(@TempDir Path tempDir) throws Exception {
        Path traceFile = tempDir.resolve("trace.jsonl");
        try (SoakTraceWriter writer = new SoakTraceWriter(traceFile)) {
            writer.emit("enqueued", Map.of("jobId", "job-excl", "queue", "q", "lockKey", "k", "lockMode", "EXCLUSIVE"));
            writer.emit("enqueued", Map.of("jobId", "job-shared", "queue", "q", "lockKey", "k", "lockMode", "SHARED"));
            // The later-enqueued SHARED executes while the earlier EXCLUSIVE is
            // still pending and never re-timed — promoted to a violation at finish().
            writer.emit("exec_started", Map.of("jobId", "job-shared", "attempt", 1));
        }
        InvariantResult result = verify(traceFile, InvariantChecks.strictInGroupOrder());
        assertThat(result.passed()).isFalse();
        assertThat(result.violations()).anyMatch(v -> v.contains("job-shared") && v.contains("job-excl"));
    }

    @Test
    void strictInGroupOrderPassesWhenTheEarlierExclusiveAlreadyExecuted(@TempDir Path tempDir) throws Exception {
        Path traceFile = tempDir.resolve("trace.jsonl");
        try (SoakTraceWriter writer = new SoakTraceWriter(traceFile)) {
            writer.emit("enqueued", Map.of("jobId", "job-excl", "queue", "q", "lockKey", "k", "lockMode", "EXCLUSIVE"));
            writer.emit("enqueued", Map.of("jobId", "job-shared", "queue", "q", "lockKey", "k", "lockMode", "SHARED"));
            writer.emit("exec_started", Map.of("jobId", "job-excl", "attempt", 1));
            writer.emit("exec_finished", Map.of("jobId", "job-excl", "attempt", 1));
            // The EXCLUSIVE's succeeded event may still be in flight (it lags
            // the terminal save) — the closed bracket alone must excuse this.
            writer.emit("exec_started", Map.of("jobId", "job-shared", "attempt", 1));
            writer.emit("exec_finished", Map.of("jobId", "job-shared", "attempt", 1));
            writer.emit("succeeded", Map.of("jobId", "job-excl", "queue", "q", "attempts", 1, "final", true));
            writer.emit("succeeded", Map.of("jobId", "job-shared", "queue", "q", "attempts", 1, "final", true));
        }
        assertThat(verify(traceFile, InvariantChecks.strictInGroupOrder()).passed())
                .isTrue();
    }

    @Test
    void strictInGroupOrderExcusesARetriedExclusive(@TempDir Path tempDir) throws Exception {
        // The endurance validations flagged both orderings of this shape: an
        // EXCLUSIVE is orphan-reclaimed and retried, re-timing its position in
        // the engine's (current_state_at, id) pending order. The reclaim
        // thread's failed/retried emissions race the admitted SHARED's
        // exec_started, so both interleavings must pass.
        Path retriedFirst = tempDir.resolve("retried-first.jsonl");
        try (SoakTraceWriter writer = new SoakTraceWriter(retriedFirst)) {
            writer.emit("enqueued", Map.of("jobId", "job-excl", "queue", "q", "lockKey", "k", "lockMode", "EXCLUSIVE"));
            writer.emit("enqueued", Map.of("jobId", "job-shared", "queue", "q", "lockKey", "k", "lockMode", "SHARED"));
            emitOrphanReclaimFailure(writer, "job-excl");
            writer.emit("retried", Map.of("jobId", "job-excl", "queue", "q", "attempts", 0));
            writer.emit("exec_started", Map.of("jobId", "job-shared", "attempt", 1));
        }
        assertThat(verify(retriedFirst, InvariantChecks.strictInGroupOrder()).passed())
                .isTrue();

        Path execFirst = tempDir.resolve("exec-first.jsonl");
        try (SoakTraceWriter writer = new SoakTraceWriter(execFirst)) {
            writer.emit("enqueued", Map.of("jobId", "job-excl", "queue", "q", "lockKey", "k", "lockMode", "EXCLUSIVE"));
            writer.emit("enqueued", Map.of("jobId", "job-shared", "queue", "q", "lockKey", "k", "lockMode", "SHARED"));
            // SHARED's exec_started wins the emission race against the reclaim
            // thread's failed/retried pair — µs apart in reality. The retried
            // event inside the excuse window cancels the provisional violation.
            writer.emit("exec_started", Map.of("jobId", "job-shared", "attempt", 1));
            emitOrphanReclaimFailure(writer, "job-excl");
            writer.emit("retried", Map.of("jobId", "job-excl", "queue", "q", "attempts", 0));
        }
        assertThat(verify(execFirst, InvariantChecks.strictInGroupOrder()).passed())
                .isTrue();
    }

    private static void emitOrphanReclaimFailure(SoakTraceWriter writer, String jobId) {
        writer.emit(
                "failed",
                Map.of("jobId", jobId, "queue", "q", "attempts", 0, "causeKind", "ORPHAN_RECLAIM", "final", false));
    }

    @Test
    void strictInGroupOrderUsesCreationOrderNotEmissionOrder(@TempDir Path tempDir) throws Exception {
        // Regression from the first multi-producer stress run: with 16
        // concurrent producers, insert latencies of seconds decouple the
        // enqueued-event emission order from creation order. Here the
        // EXCLUSIVE's enqueued line lands FIRST in the trace, but its UUIDv7
        // id is 20ms NEWER than the SHARED's — by the engine's
        // (current_state_at, id) order the SHARED is earlier, so running it
        // first is legal and must not be flagged.
        String sharedId = "019f21a0-f000-7aaa-8aaa-aaaaaaaaaaaa";
        String exclusiveId = "019f21a0-f014-7bbb-8bbb-bbbbbbbbbbbb";
        Path traceFile = tempDir.resolve("trace.jsonl");
        try (SoakTraceWriter writer = new SoakTraceWriter(traceFile)) {
            writer.emit(
                    "enqueued", Map.of("jobId", exclusiveId, "queue", "q", "lockKey", "k", "lockMode", "EXCLUSIVE"));
            writer.emit("enqueued", Map.of("jobId", sharedId, "queue", "q", "lockKey", "k", "lockMode", "SHARED"));
            writer.emit("exec_started", Map.of("jobId", sharedId, "attempt", 1));
        }
        assertThat(verify(traceFile, InvariantChecks.strictInGroupOrder()).passed())
                .isTrue();
    }

    @Test
    void strictInGroupOrderStillFlagsARealLeapfrogWithUuidIds(@TempDir Path tempDir) throws Exception {
        // EXCLUSIVE created 100ms before the SHARED — clearly earlier — and
        // never executed: a genuine leapfrog, whatever the emission order.
        String exclusiveId = "019f21a0-f000-7bbb-8bbb-bbbbbbbbbbbb";
        String sharedId = "019f21a0-f064-7aaa-8aaa-aaaaaaaaaaaa";
        Path traceFile = tempDir.resolve("trace.jsonl");
        try (SoakTraceWriter writer = new SoakTraceWriter(traceFile)) {
            writer.emit("enqueued", Map.of("jobId", sharedId, "queue", "q", "lockKey", "k", "lockMode", "SHARED"));
            writer.emit(
                    "enqueued", Map.of("jobId", exclusiveId, "queue", "q", "lockKey", "k", "lockMode", "EXCLUSIVE"));
            writer.emit("exec_started", Map.of("jobId", sharedId, "attempt", 1));
        }
        InvariantResult result = verify(traceFile, InvariantChecks.strictInGroupOrder());
        assertThat(result.passed()).isFalse();
        assertThat(result.violations()).anyMatch(v -> v.contains(sharedId) && v.contains(exclusiveId));
    }

    @Test
    void strictInGroupOrderExcusesTheSameInstantAmbiguityBand(@TempDir Path tempDir) throws Exception {
        // Created 1ms apart: inside the band the engine's microsecond
        // (current_state_at, id) order is not observable from the trace —
        // asserting it would assert a guess.
        String exclusiveId = "019f21a0-f000-7bbb-8bbb-bbbbbbbbbbbb";
        String sharedId = "019f21a0-f001-7aaa-8aaa-aaaaaaaaaaaa";
        Path traceFile = tempDir.resolve("trace.jsonl");
        try (SoakTraceWriter writer = new SoakTraceWriter(traceFile)) {
            writer.emit(
                    "enqueued", Map.of("jobId", exclusiveId, "queue", "q", "lockKey", "k", "lockMode", "EXCLUSIVE"));
            writer.emit("enqueued", Map.of("jobId", sharedId, "queue", "q", "lockKey", "k", "lockMode", "SHARED"));
            writer.emit("exec_started", Map.of("jobId", sharedId, "attempt", 1));
        }
        assertThat(verify(traceFile, InvariantChecks.strictInGroupOrder()).passed())
                .isTrue();
    }

    @Test
    void strictInGroupOrderDoesNotLetALateRetryExcuseARealLeapfrog(@TempDir Path tempDir) throws Exception {
        // The excuse window only absorbs the reclaim-thread emission race
        // (µs–ms). An EXCLUSIVE retried long after the SHARED executed was
        // genuinely leapfrogged first — the suspicion must be promoted.
        Path traceFile = tempDir.resolve("trace.jsonl");
        try (SoakTraceWriter writer = new SoakTraceWriter(traceFile)) {
            writer.emit("enqueued", Map.of("jobId", "job-excl", "queue", "q", "lockKey", "k", "lockMode", "EXCLUSIVE"));
            writer.emit("enqueued", Map.of("jobId", "job-shared", "queue", "q", "lockKey", "k", "lockMode", "SHARED"));
            writer.emit("exec_started", Map.of("jobId", "job-shared", "attempt", 1));
        }
        appendRawEvent(
                traceFile,
                Instant.now().plusSeconds(30),
                "retried",
                Map.of("jobId", "job-excl", "queue", "q", "attempts", 0));
        InvariantResult result = verify(traceFile, InvariantChecks.strictInGroupOrder());
        assertThat(result.passed()).isFalse();
        assertThat(result.violations()).anyMatch(v -> v.contains("job-shared") && v.contains("job-excl"));
    }

    /** Appends one trace line with a forged timestamp — for window-expiry tests. */
    private static void appendRawEvent(Path traceFile, Instant timestamp, String event, Map<String, Object> fields)
            throws IOException {
        var mapper = new ObjectMapper();
        var ordered = new LinkedHashMap<String, Object>();
        ordered.put("timestamp", timestamp.toString());
        ordered.put("event", event);
        ordered.putAll(fields);
        Files.writeString(
                traceFile,
                mapper.writeValueAsString(ordered) + "\n",
                StandardCharsets.UTF_8,
                StandardOpenOption.APPEND);
    }

    @Test
    void noLockLeaksPassesTheOrphanReclaimRetryLifecycle(@TempDir Path tempDir) throws Exception {
        // Attempt 0 never started a handler (no acquire, no release — the
        // interceptor suppresses the release for never-started attempts);
        // attempt 1 runs a full bracket. Balance must close.
        Path traceFile = tempDir.resolve("trace.jsonl");
        try (SoakTraceWriter writer = new SoakTraceWriter(traceFile)) {
            writer.emit("enqueued", Map.of("jobId", "job-1", "queue", "q", "lockKey", "k", "lockMode", "EXCLUSIVE"));
            writer.emit(
                    "failed",
                    Map.of(
                            "jobId",
                            "job-1",
                            "queue",
                            "q",
                            "attempts",
                            0,
                            "causeKind",
                            "ORPHAN_RECLAIM",
                            "final",
                            false));
            writer.emit("retried", Map.of("jobId", "job-1", "queue", "q", "attempts", 0));
            writer.emit("claimed", Map.of("jobId", "job-1", "queue", "q", "attempt", 1));
            writer.emit("lock_acquired", Map.of("jobId", "job-1", "lockKey", "k", "lockMode", "EXCLUSIVE"));
            writer.emit("succeeded", Map.of("jobId", "job-1", "queue", "q", "attempts", 1, "final", true));
            writer.emit("lock_released", Map.of("jobId", "job-1", "lockKey", "k", "lockMode", "EXCLUSIVE"));
        }
        assertThat(verify(traceFile, InvariantChecks.noLockLeaks()).passed()).isTrue();
    }

    @Test
    void retryBudgetFiresTheMomentAClaimExceedsTheCeiling(@TempDir Path tempDir) throws Exception {
        Path traceFile = tempDir.resolve("trace.jsonl");
        try (SoakTraceWriter writer = new SoakTraceWriter(traceFile)) {
            writer.emit("claimed", Map.of("jobId", "job-1", "queue", "q", "attempt", 4));
        }
        InvariantResult result = verify(traceFile, InvariantChecks.retryBudgetRespected(3));
        assertThat(result.passed()).isFalse();
        assertThat(result.violations()).anyMatch(v -> v.contains("job-1") && v.contains("4 attempts"));
    }

    @Test
    void pauseObeyedFiresOnAClaimInsideThePauseBracket(@TempDir Path tempDir) throws Exception {
        Path traceFile = tempDir.resolve("trace.jsonl");
        try (SoakTraceWriter writer = new SoakTraceWriter(traceFile)) {
            writer.emit("queue_paused", Map.of("queue", "q", "reason", "test"));
            writer.emit("claimed", Map.of("jobId", "job-1", "queue", "q", "attempt", 1));
            writer.emit("queue_resumed", Map.of("queue", "q"));
        }
        InvariantResult result = verify(traceFile, InvariantChecks.pauseObeyed());
        assertThat(result.passed()).isFalse();
        assertThat(result.violations()).anyMatch(v -> v.contains("paused queue q"));
    }

    @Test
    void bulkInsertAtomicFiresForBothEventOrderings(@TempDir Path tempDir) throws Exception {
        Path enqueueFirst = tempDir.resolve("enqueue-first.jsonl");
        try (SoakTraceWriter writer = new SoakTraceWriter(enqueueFirst)) {
            writer.emit("enqueued", Map.of("jobId", "job-1", "queue", "q", "batchId", "b1"));
            writer.emit("bulk_batch_rejected", Map.of("batchId", "b1"));
        }
        assertThat(verify(enqueueFirst, InvariantChecks.bulkInsertAtomic()).passed())
                .isFalse();

        Path rejectFirst = tempDir.resolve("reject-first.jsonl");
        try (SoakTraceWriter writer = new SoakTraceWriter(rejectFirst)) {
            writer.emit("bulk_batch_rejected", Map.of("batchId", "b1"));
            writer.emit("enqueued", Map.of("jobId", "job-1", "queue", "q", "batchId", "b1"));
        }
        assertThat(verify(rejectFirst, InvariantChecks.bulkInsertAtomic()).passed())
                .isFalse();
    }

    @Test
    void midRunSnapshotDoesNotFlagOpenJobsOrHeldLocksAsViolations() {
        StreamingInvariantCheck atLeastOnce = InvariantChecks.atLeastOnce().newCheck();
        StreamingInvariantCheck noLockLeaks = InvariantChecks.noLockLeaks().newCheck();
        feed(atLeastOnce, "enqueued", Map.of("jobId", "job-1", "queue", "q"));
        feed(noLockLeaks, "lock_acquired", Map.of("jobId", "job-1", "lockKey", "k", "lockMode", "EXCLUSIVE"));

        // Mid-run: the job is simply still running — not a violation.
        assertThat(atLeastOnce.snapshot().passed()).isTrue();
        assertThat(noLockLeaks.snapshot().passed()).isTrue();

        // End of run: now both are completeness violations.
        assertThat(atLeastOnce.finish().passed()).isFalse();
        assertThat(noLockLeaks.finish().passed()).isFalse();
    }

    @Test
    void violationRecordingIsCappedButTheTotalCountIsReported() {
        StreamingInvariantCheck check = InvariantChecks.retryBudgetRespected(1).newCheck();
        for (int i = 0; i < 250; i++) {
            feed(check, "claimed", Map.of("jobId", "job-" + i, "queue", "q", "attempt", 2));
        }
        assertThat(check.violationCount()).isEqualTo(250);
        InvariantResult result = check.snapshot();
        assertThat(result.passed()).isFalse();
        // 100 recorded messages plus the "+N more" marker.
        assertThat(result.violations()).hasSize(101);
        assertThat(result.violations().getLast()).contains("+150 more");
        assertThat(result.sampleChains()).hasSize(5);
    }

    private static void feed(StreamingInvariantCheck check, String event, Map<String, Object> fields) {
        var mapper = new ObjectMapper();
        var ordered = new LinkedHashMap<String, Object>();
        ordered.put("timestamp", Instant.now().toString());
        ordered.put("event", event);
        ordered.putAll(fields);
        try {
            String line = mapper.writeValueAsString(ordered);
            check.onEvent(new TraceEvent(mapper.readTree(line), line));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static InvariantResult verify(Path traceFile, SoakInvariant invariant) throws Exception {
        List<InvariantResult> results = TraceReplay.verify(traceFile, List.of(invariant));
        return results.getFirst();
    }
}
