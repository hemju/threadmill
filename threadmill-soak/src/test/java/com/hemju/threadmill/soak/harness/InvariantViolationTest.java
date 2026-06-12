package com.hemju.threadmill.soak.harness;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
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
    void exclusivityHeldFiresWhenExclusiveOverlapsShared(@TempDir Path tempDir) throws Exception {
        Path traceFile = tempDir.resolve("trace.jsonl");
        try (SoakTraceWriter writer = new SoakTraceWriter(traceFile)) {
            writer.emit("lock_acquired", Map.of("jobId", "job-1", "lockKey", "k", "lockMode", "SHARED"));
            // EXCLUSIVE on the same key while SHARED is held is a contract violation.
            writer.emit("lock_acquired", Map.of("jobId", "job-2", "lockKey", "k", "lockMode", "EXCLUSIVE"));
            writer.emit("lock_released", Map.of("jobId", "job-2", "lockKey", "k", "lockMode", "EXCLUSIVE"));
            writer.emit("lock_released", Map.of("jobId", "job-1", "lockKey", "k", "lockMode", "SHARED"));
        }
        InvariantResult result = verify(traceFile, InvariantChecks.exclusivityHeld());
        assertThat(result.passed()).isFalse();
        assertThat(result.violations()).isNotEmpty();
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
            // The later-enqueued SHARED runs while the earlier EXCLUSIVE is still pending.
            writer.emit("lock_acquired", Map.of("jobId", "job-shared", "lockKey", "k", "lockMode", "SHARED"));
        }
        InvariantResult result = verify(traceFile, InvariantChecks.strictInGroupOrder());
        assertThat(result.passed()).isFalse();
        assertThat(result.violations()).anyMatch(v -> v.contains("job-shared") && v.contains("job-excl"));
    }

    @Test
    void strictInGroupOrderPassesWhenTheEarlierExclusiveAlreadyFinished(@TempDir Path tempDir) throws Exception {
        Path traceFile = tempDir.resolve("trace.jsonl");
        try (SoakTraceWriter writer = new SoakTraceWriter(traceFile)) {
            writer.emit("enqueued", Map.of("jobId", "job-excl", "queue", "q", "lockKey", "k", "lockMode", "EXCLUSIVE"));
            writer.emit("enqueued", Map.of("jobId", "job-shared", "queue", "q", "lockKey", "k", "lockMode", "SHARED"));
            writer.emit("lock_acquired", Map.of("jobId", "job-excl", "lockKey", "k", "lockMode", "EXCLUSIVE"));
            writer.emit("lock_released", Map.of("jobId", "job-excl", "lockKey", "k", "lockMode", "EXCLUSIVE"));
            writer.emit("succeeded", Map.of("jobId", "job-excl", "queue", "q", "attempts", 1, "final", true));
            writer.emit("lock_acquired", Map.of("jobId", "job-shared", "lockKey", "k", "lockMode", "SHARED"));
            writer.emit("lock_released", Map.of("jobId", "job-shared", "lockKey", "k", "lockMode", "SHARED"));
            writer.emit("succeeded", Map.of("jobId", "job-shared", "queue", "q", "attempts", 1, "final", true));
        }
        assertThat(verify(traceFile, InvariantChecks.strictInGroupOrder()).passed())
                .isTrue();
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
