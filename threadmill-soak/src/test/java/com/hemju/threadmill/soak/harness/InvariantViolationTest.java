package com.hemju.threadmill.soak.harness;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.hemju.threadmill.soak.harness.invariant.InvariantChecks;
import com.hemju.threadmill.soak.harness.invariant.InvariantResult;
import com.hemju.threadmill.soak.harness.invariant.TraceCorpus;

/**
 * Confirms the invariant checker actually fires on a deliberately bad trace.
 *
 * <p>If we only ever exercise the green path, a regression that silently
 * disables the checker (e.g. early-returns before the violations list is
 * populated) would pass every other test. This is the red-path coverage.
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
        TraceCorpus corpus = TraceCorpus.load(traceFile);
        InvariantResult result = InvariantChecks.noLockLeaks().check(corpus);
        assertThat(result.passed()).isFalse();
        assertThat(result.violations()).anyMatch(v -> v.contains("job-1"));
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
        TraceCorpus corpus = TraceCorpus.load(traceFile);
        InvariantResult result = InvariantChecks.exclusivityHeld().check(corpus);
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
        TraceCorpus corpus = TraceCorpus.load(traceFile);
        InvariantResult result = InvariantChecks.atLeastOnce().check(corpus);
        assertThat(result.passed()).isFalse();
        assertThat(result.violations()).anyMatch(v -> v.contains("job-orphan"));
    }
}
