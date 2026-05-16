package com.hemju.threadmill.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;

class JobTest {

    private static Job freshEnqueued() {
        return Job.builder()
                .spec(JobSpec.of("com.example.Handler", new JobArgument("java.lang.String", "\"x\"")))
                .build();
    }

    @Test
    void newJobStartsEnqueuedWithVersionZero() {
        Job j = freshEnqueued();
        assertThat(j.currentState()).isEqualTo(JobState.ENQUEUED);
        assertThat(j.version()).isZero();
        assertThat(j.stateHistory()).hasSize(1);
    }

    @Test
    void transitionToAppendsToHistoryAndUpdatesCurrentState() {
        Job j = freshEnqueued();
        j.transitionTo(JobState.PROCESSING, Instant.now());
        assertThat(j.currentState()).isEqualTo(JobState.PROCESSING);
        assertThat(j.stateHistory()).hasSize(2);
    }

    @Test
    void illegalTransitionsThrow() {
        Job j = freshEnqueued();
        assertThatThrownBy(() -> j.transitionTo(JobState.ENQUEUED, Instant.now()))
                .isInstanceOf(IllegalJobTransitionException.class);
        assertThat(j.stateHistory()).hasSize(1);
    }

    @Test
    void adoptVersionAcceptsForwardMovesOnly() {
        Job j = freshEnqueued();
        j.adoptVersion(5);
        assertThat(j.version()).isEqualTo(5);
        j.adoptVersion(5); // no-op forward
        j.adoptVersion(7);
        assertThat(j.version()).isEqualTo(7);
        assertThatThrownBy(() -> j.adoptVersion(6)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void snapshotIsSeparateFromMutations() {
        Job j = freshEnqueued();
        JobSnapshot before = j.snapshot();
        j.metadata().put("k", "v");
        j.transitionTo(JobState.PROCESSING, Instant.now());

        // The snapshot taken before is unaffected by later mutations.
        assertThat(before.currentState()).isEqualTo(JobState.ENQUEUED);
        assertThat(before.metadata()).doesNotContainKey("k");
        // The fresh snapshot reflects them.
        JobSnapshot after = j.snapshot();
        assertThat(after.currentState()).isEqualTo(JobState.PROCESSING);
        assertThat(after.metadata()).containsEntry("k", "v");
    }

    @Test
    void jobIdsAreTimeOrderedV7() {
        JobId a = JobId.newId();
        // Ensure clear ordering by waiting a millisecond.
        try {
            Thread.sleep(2);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        JobId b = JobId.newId();
        // High 48 bits of the UUID encode the time component; b should be >= a.
        long aHi = a.asUuid().getMostSignificantBits() >>> 16;
        long bHi = b.asUuid().getMostSignificantBits() >>> 16;
        assertThat(bHi).isGreaterThanOrEqualTo(aHi);
        // Version nibble is 7 (UUIDv7).
        assertThat(a.asUuid().version()).isEqualTo(7);
        assertThat(b.asUuid().version()).isEqualTo(7);
    }
}
