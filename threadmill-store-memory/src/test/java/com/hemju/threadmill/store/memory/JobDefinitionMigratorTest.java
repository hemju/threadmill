package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobDefinitionMigrator;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;

/**
 * Regression tests for {@link JobDefinitionMigrator}: rewriting persisted job
 * definitions after a handler or payload class has been renamed.
 *
 * <p>Covers: replaceable-state-only rewrite ({@code ENQUEUED} / {@code SCHEDULED}
 * / {@code AWAITING}); PROCESSING and terminal jobs untouched; payload-side
 * spec migration applied before the handler-type rewrite; the migrator's
 * post-callback guard that the resulting spec carries the new handler type;
 * {@code max} batch bound; null-argument validation.
 */
class JobDefinitionMigratorTest {

    private static final String OLD_HANDLER = "com.example.OldHandler";
    private static final String NEW_HANDLER = "com.example.NewHandler";

    private InMemoryJobStore store;
    private JobDefinitionMigrator migrator;

    @BeforeEach
    void setUp() {
        store = new InMemoryJobStore();
        migrator = new JobDefinitionMigrator(store);
    }

    @Test
    void rewritesEnqueuedScheduledAndAwaitingJobsToNewHandlerSignature() {
        JobId enq = insertWithState(JobState.ENQUEUED, OLD_HANDLER);
        JobId sch = insertWithState(JobState.SCHEDULED, OLD_HANDLER);
        JobId awa = insertWithState(JobState.AWAITING, OLD_HANDLER);

        long migrated = migrator.migrateHandlerSignature(OLD_HANDLER, NEW_HANDLER, UnaryOperator.identity(), 100);

        assertThat(migrated).isEqualTo(3L);
        assertThat(store.findById(enq).orElseThrow().spec().handlerType()).isEqualTo(NEW_HANDLER);
        assertThat(store.findById(sch).orElseThrow().spec().handlerType()).isEqualTo(NEW_HANDLER);
        assertThat(store.findById(awa).orElseThrow().spec().handlerType()).isEqualTo(NEW_HANDLER);
        assertThat(store.findByHandlerSignature(OLD_HANDLER, 100)).isEmpty();
    }

    @Test
    void leavesProcessingAndTerminalJobsUntouched() {
        // PROCESSING: insert ENQUEUED then claim.
        JobId processing = insertWithState(JobState.ENQUEUED, OLD_HANDLER);
        var claimed = store.claimReady(NodeId.newId(), "default", 1, Instant.now());
        assertThat(claimed).extracting(Job::id).containsExactly(processing);

        // Terminal states: drive ENQUEUED → PROCESSING → terminal.
        JobId succeeded = insertAndFinishAt(JobState.SUCCEEDED);
        JobId failed = insertAndFinishAt(JobState.FAILED);
        JobId quarantined = insertAndFinishAt(JobState.QUARANTINED);

        long migrated = migrator.migrateHandlerSignature(OLD_HANDLER, NEW_HANDLER, UnaryOperator.identity(), 100);

        assertThat(migrated).isZero();
        for (JobId id : List.of(processing, succeeded, failed, quarantined)) {
            assertThat(store.findById(id).orElseThrow().spec().handlerType()).isEqualTo(OLD_HANDLER);
        }
    }

    @Test
    void appliesPayloadSpecMigrationBeforeRewritingHandler() {
        JobId id = insertWithState(JobState.ENQUEUED, OLD_HANDLER);

        long migrated = migrator.migrateHandlerSignature(
                OLD_HANDLER,
                NEW_HANDLER,
                spec -> new JobSpec(
                        spec.handlerType(),
                        List.of(new JobArgument(
                                "com.example.NewPayload",
                                spec.arguments().get(0).serialized()))),
                100);

        assertThat(migrated).isEqualTo(1L);
        var after = store.findById(id).orElseThrow();
        assertThat(after.spec().handlerType()).isEqualTo(NEW_HANDLER);
        assertThat(after.spec().arguments()).singleElement().satisfies(arg -> {
            assertThat(arg.typeTag()).isEqualTo("com.example.NewPayload");
            assertThat(arg.serialized()).isEqualTo("\"hello\"");
        });
    }

    @Test
    void newHandlerTypeOverridesAnythingTheSpecMigrationReturned() {
        JobId id = insertWithState(JobState.ENQUEUED, OLD_HANDLER);

        long migrated = migrator.migrateHandlerSignature(
                OLD_HANDLER, NEW_HANDLER, spec -> new JobSpec("com.example.WrongHandler", spec.arguments()), 100);

        assertThat(migrated).isEqualTo(1L);
        assertThat(store.findById(id).orElseThrow().spec().handlerType()).isEqualTo(NEW_HANDLER);
    }

    @Test
    void respectsMaxBatchSizeAndReturnsZeroForNonPositiveMax() {
        for (int i = 0; i < 10; i++) {
            insertWithState(JobState.ENQUEUED, OLD_HANDLER);
        }

        long firstBatch = migrator.migrateHandlerSignature(OLD_HANDLER, NEW_HANDLER, UnaryOperator.identity(), 3);
        assertThat(firstBatch).isEqualTo(3L);
        assertThat(store.findByHandlerSignature(OLD_HANDLER, 100)).hasSize(7);
        assertThat(store.findByHandlerSignature(NEW_HANDLER, 100)).hasSize(3);

        long zeroBatch = migrator.migrateHandlerSignature(OLD_HANDLER, NEW_HANDLER, UnaryOperator.identity(), 0);
        assertThat(zeroBatch).isZero();
        assertThat(store.findByHandlerSignature(OLD_HANDLER, 100)).hasSize(7);

        long negativeBatch = migrator.migrateHandlerSignature(OLD_HANDLER, NEW_HANDLER, UnaryOperator.identity(), -1);
        assertThat(negativeBatch).isZero();
        assertThat(store.findByHandlerSignature(OLD_HANDLER, 100)).hasSize(7);
    }

    @Test
    void maxBatchSizeCountsMigratedJobsNotSkippedNonReplaceableJobs() {
        JobId processing = insertWithState(JobState.ENQUEUED, OLD_HANDLER);
        assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                .extracting(Job::id)
                .containsExactly(processing);
        JobId terminal = insertAndFinishAt(JobState.SUCCEEDED);
        JobId firstReplaceable = insertWithState(JobState.ENQUEUED, OLD_HANDLER);
        JobId secondReplaceable = insertWithState(JobState.ENQUEUED, OLD_HANDLER);

        long migrated = migrator.migrateHandlerSignature(OLD_HANDLER, NEW_HANDLER, UnaryOperator.identity(), 2);

        assertThat(migrated).isEqualTo(2L);
        assertThat(store.findById(processing).orElseThrow().spec().handlerType())
                .isEqualTo(OLD_HANDLER);
        assertThat(store.findById(terminal).orElseThrow().spec().handlerType()).isEqualTo(OLD_HANDLER);
        assertThat(store.findById(firstReplaceable).orElseThrow().spec().handlerType())
                .isEqualTo(NEW_HANDLER);
        assertThat(store.findById(secondReplaceable).orElseThrow().spec().handlerType())
                .isEqualTo(NEW_HANDLER);
    }

    @Test
    void nullArgsAreRejected() {
        assertThatThrownBy(() -> migrator.migrateHandlerSignature(null, NEW_HANDLER, UnaryOperator.identity(), 10))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> migrator.migrateHandlerSignature(OLD_HANDLER, null, UnaryOperator.identity(), 10))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> migrator.migrateHandlerSignature(OLD_HANDLER, NEW_HANDLER, null, 10))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new JobDefinitionMigrator(null)).isInstanceOf(NullPointerException.class);
    }

    // ---------------------------------------------------------------- helpers

    private static JobSpec specOf(String handler) {
        return JobSpec.of(handler, new JobArgument("com.example.OldPayload", "\"hello\""));
    }

    private JobId insertWithState(JobState state, String handler) {
        Job job = Job.builder().spec(specOf(handler)).initialState(state).build();
        store.insert(job);
        return job.id();
    }

    private JobId insertAndFinishAt(JobState terminal) {
        Job job = Job.builder().spec(specOf(OLD_HANDLER)).build();
        store.insert(job);
        var claimed = store.claimReady(NodeId.newId(), "default", 1, Instant.now());
        assertThat(claimed).hasSize(1);
        Job p = claimed.get(0);
        long v = p.version();
        p.transitionTo(terminal, Instant.now(), "test.finish", null);
        p.clearOwner();
        store.saveAtomic(p, v);
        return job.id();
    }
}
