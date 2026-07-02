package com.hemju.threadmill.soak.harness;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.soak.harness.invariant.InvariantChecks;
import com.hemju.threadmill.soak.harness.invariant.InvariantResult;
import com.hemju.threadmill.soak.harness.invariant.LiveInvariantVerifier;
import com.hemju.threadmill.soak.harness.invariant.SoakInvariant;
import com.hemju.threadmill.soak.harness.invariant.StreamingInvariantCheck;
import com.hemju.threadmill.soak.harness.invariant.TraceEvent;

/**
 * The live verifier is what turns an hours-long endurance run supervisable:
 * fail-fast must fire on the first definite violation (exactly once), and a
 * broken check must be quarantined without masking the other checks.
 */
final class LiveInvariantVerifierTest {

    @Test
    void firstDefiniteViolationFiresTheCallbackExactlyOnce() {
        var fired = new AtomicInteger();
        var verifier = new LiveInvariantVerifier(List.of(InvariantChecks.exclusivityHeld()), fired::incrementAndGet);

        verifier.onEvent(event("enqueued", Map.of("jobId", "j1", "queue", "q", "lockKey", "k", "lockMode", "SHARED")));
        verifier.onEvent(
                event("enqueued", Map.of("jobId", "j2", "queue", "q", "lockKey", "k", "lockMode", "EXCLUSIVE")));
        verifier.onEvent(
                event("enqueued", Map.of("jobId", "j3", "queue", "q", "lockKey", "k", "lockMode", "EXCLUSIVE")));
        verifier.onEvent(event("exec_started", Map.of("jobId", "j1", "attempt", 1)));
        assertThat(verifier.hasDefiniteViolation()).isFalse();
        assertThat(fired.get()).isZero();

        // EXCLUSIVE handler executing while the SHARED bracket is open.
        verifier.onEvent(event("exec_started", Map.of("jobId", "j2", "attempt", 1)));
        assertThat(verifier.hasDefiniteViolation()).isTrue();
        assertThat(fired.get()).isEqualTo(1);

        // A second violation must not re-fire the callback.
        verifier.onEvent(event("exec_started", Map.of("jobId", "j3", "attempt", 1)));
        assertThat(fired.get()).isEqualTo(1);
    }

    @Test
    void completenessChecksDoNotTriggerFailFastMidRun() {
        var fired = new AtomicInteger();
        var verifier = new LiveInvariantVerifier(
                List.of(InvariantChecks.atLeastOnce(), InvariantChecks.noLockLeaks()), fired::incrementAndGet);

        verifier.onEvent(event("enqueued", Map.of("jobId", "j1", "queue", "q")));
        verifier.onEvent(event("lock_acquired", Map.of("jobId", "j1", "lockKey", "k", "lockMode", "EXCLUSIVE")));

        assertThat(fired.get()).isZero();
        assertThat(verifier.snapshotResults()).allMatch(InvariantResult::passed);
        // Final results report both as completeness violations.
        assertThat(verifier.finishResults()).noneMatch(InvariantResult::passed);
    }

    @Test
    void aThrowingCheckIsQuarantinedAndReportedWithoutMaskingOthers() {
        SoakInvariant broken = new SoakInvariant() {
            @Override
            public String name() {
                return "broken";
            }

            @Override
            public String description() {
                return "always throws";
            }

            @Override
            public StreamingInvariantCheck newCheck() {
                return new StreamingInvariantCheck("broken") {
                    @Override
                    protected void observe(TraceEvent event) {
                        throw new IllegalStateException("boom");
                    }
                };
            }
        };
        var fired = new AtomicInteger();
        var verifier =
                new LiveInvariantVerifier(List.of(broken, InvariantChecks.exclusivityHeld()), fired::incrementAndGet);

        verifier.onEvent(event("enqueued", Map.of("jobId", "j1", "queue", "q", "lockKey", "k", "lockMode", "SHARED")));
        verifier.onEvent(
                event("enqueued", Map.of("jobId", "j2", "queue", "q", "lockKey", "k", "lockMode", "EXCLUSIVE")));
        verifier.onEvent(event("exec_started", Map.of("jobId", "j1", "attempt", 1)));
        verifier.onEvent(event("exec_started", Map.of("jobId", "j2", "attempt", 1)));

        List<InvariantResult> results = verifier.finishResults();
        assertThat(results.get(0).passed()).isFalse();
        assertThat(results.get(0).violations()).anyMatch(v -> v.contains("IllegalStateException"));
        // The healthy check still detected the real violation.
        assertThat(results.get(1).passed()).isFalse();
        assertThat(fired.get()).isEqualTo(1);
    }

    private static TraceEvent event(String event, Map<String, Object> fields) {
        var mapper = new ObjectMapper();
        var ordered = new LinkedHashMap<String, Object>();
        ordered.put("timestamp", Instant.now().toString());
        ordered.put("event", event);
        ordered.putAll(fields);
        try {
            String line = mapper.writeValueAsString(ordered);
            return new TraceEvent(mapper.readTree(line), line);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
