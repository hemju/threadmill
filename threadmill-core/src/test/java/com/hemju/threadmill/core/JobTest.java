package com.hemju.threadmill.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;

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
    void concurrencyModeWithoutKeyIsRejectedLoudly() {
        assertThatThrownBy(() -> Job.builder()
                        .spec(JobSpec.of("com.example.Handler", new JobArgument("java.lang.String", "\"x\"")))
                        .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("concurrencyKey");
    }

    @Test
    void queueNamesAreValidatedAtTheModelBoundary() {
        assertThatThrownBy(() -> Job.builder()
                        .spec(JobSpec.of("com.example.Handler", new JobArgument("java.lang.String", "\"x\"")))
                        .queue("bad\nqueue")
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("queue");
        assertThatThrownBy(() -> Job.builder()
                        .spec(JobSpec.of("com.example.Handler", new JobArgument("java.lang.String", "\"x\"")))
                        .queue("q".repeat(200))
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("queue");
        assertThatThrownBy(() -> Job.builder()
                        .spec(JobSpec.of("com.example.Handler", new JobArgument("java.lang.String", "\"x\"")))
                        .queue("   ")
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("queue");
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
    void snapshotConstructorDefensivelyCopiesMutableCollections() {
        JobSnapshot source = freshEnqueued().snapshot();
        var history = new ArrayList<>(source.stateHistory());
        var metadata = new HashMap<>(source.metadata());
        var log = new ArrayList<>(source.log());
        var snapshot = new JobSnapshot(
                source.id(),
                source.spec(),
                source.queue(),
                source.priority(),
                source.createdAt(),
                source.cronTaskName(),
                source.relationship(),
                source.workflowRootId(),
                source.concurrencyKey(),
                source.concurrencyMode(),
                history,
                metadata,
                log,
                source.progress(),
                source.version(),
                source.ownerNodeId(),
                source.ownerHeartbeatAt(),
                source.lastCheckinAt(),
                source.scheduledFor(),
                source.result(),
                source.attempts());

        history.clear();
        metadata.put("later", "mutation");
        log.clear();

        assertThat(snapshot.stateHistory()).isNotEmpty();
        assertThat(snapshot.metadata()).doesNotContainKey("later");
        assertThatThrownBy(() -> snapshot.metadata().put("blocked", "mutation"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void defaultIdTimePrefixMatchesCreatedAtFromASingleClockRead() {
        var fixed = Clock.fixed(Instant.parse("2026-07-05T10:15:30.123456Z"), ZoneOffset.UTC);
        Job j = Job.builder()
                .spec(JobSpec.of("com.example.Handler", new JobArgument("java.lang.String", "\"x\"")))
                .clock(fixed)
                .build();
        long idMillis = j.id().asUuid().getMostSignificantBits() >>> 16;
        assertThat(idMillis).isEqualTo(j.createdAt().toEpochMilli());
        assertThat(j.stateHistory().getFirst().at()).isEqualTo(j.createdAt());
    }

    @Test
    void explicitIdAndCreatedAtAreNotOverriddenByTheDerivedDefaults() {
        var explicitId = JobId.newId();
        var explicitCreatedAt = Instant.parse("2020-01-01T00:00:00Z");
        Job j = Job.builder()
                .spec(JobSpec.of("com.example.Handler", new JobArgument("java.lang.String", "\"x\"")))
                .id(explicitId)
                .createdAt(explicitCreatedAt)
                .build();
        assertThat(j.id()).isEqualTo(explicitId);
        assertThat(j.createdAt()).isEqualTo(explicitCreatedAt);
    }

    @Test
    void millisSeededIdKeepsUuidV7VersionAndVariantBits() {
        long millis = Instant.parse("2026-07-05T10:15:30.123Z").toEpochMilli();
        var uuid = JobId.newId(millis).asUuid();
        assertThat(uuid.getMostSignificantBits() >>> 16).isEqualTo(millis);
        assertThat(uuid.version()).isEqualTo(7);
        assertThat(uuid.variant()).isEqualTo(2);
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
