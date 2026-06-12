package com.hemju.threadmill.soak.harness;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.engine.JobInterceptor.FailureCause;
import com.hemju.threadmill.core.spec.JobSpec;

/**
 * Regression from the first real dual-backend endurance validation: a node
 * churned away between the store-level claim and {@code onProcessingStarting}
 * leaves a PROCESSING job whose handler never ran. Orphan reclaim then fires
 * the failure hook — and the interceptor used to emit a {@code lock_released}
 * for a {@code lock_acquired} that was never traced, tripping the
 * lock-pairing invariant (acquire=1, release=2 across the retry). The trace's
 * lock vocabulary describes handler-level brackets; an attempt that never
 * started has no bracket to close.
 */
final class SoakInterceptorOrphanReclaimTest {

    @Test
    void orphanReclaimOfANeverStartedAttemptEmitsNoLockReleased(@TempDir Path tempDir) throws Exception {
        Path traceFile = tempDir.resolve("trace.jsonl");
        Job job = keyedJob();
        try (SoakTraceWriter trace = new SoakTraceWriter(traceFile);
                LatencyTracker latency = new LatencyTracker(tempDir.resolve("latencies.jsonl"))) {
            var interceptor = new SoakInterceptor(trace, latency);
            // No onProcessingStarting — the claiming node died before the
            // handler ran; the surviving node's orphan scan fails the job.
            interceptor.onProcessingFailed(
                    job, null, new IllegalStateException("orphaned"), FailureCause.ORPHAN_RECLAIM);
        }
        String trace = Files.readString(traceFile);
        assertThat(trace).contains("\"event\":\"failed\"");
        assertThat(trace).doesNotContain("\"event\":\"lock_released\"");
    }

    @Test
    void aStartedAttemptStillPairsItsAcquireAndRelease(@TempDir Path tempDir) throws Exception {
        Path traceFile = tempDir.resolve("trace.jsonl");
        Job job = keyedJob();
        try (SoakTraceWriter trace = new SoakTraceWriter(traceFile);
                LatencyTracker latency = new LatencyTracker(tempDir.resolve("latencies.jsonl"))) {
            var interceptor = new SoakInterceptor(trace, latency);
            interceptor.onProcessingStarting(job, null);
            interceptor.onProcessingFailed(job, null, new IllegalStateException("boom"), FailureCause.ORPHAN_RECLAIM);
        }
        String trace = Files.readString(traceFile);
        assertThat(trace).contains("\"event\":\"lock_acquired\"");
        assertThat(trace).contains("\"event\":\"lock_released\"");
    }

    private static Job keyedJob() {
        // SCHEDULED = the post-retry state, so the failure hook sees a
        // non-final failure (the orphan-reclaim-then-retry shape).
        return Job.builder()
                .spec(new JobSpec("com.example.SomeHandler", List.of()))
                .queue("project:1")
                .concurrencyKey("project:1")
                .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
                .initialState(JobState.SCHEDULED)
                .build();
    }
}
