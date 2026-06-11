package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;

/** Minor-hardening regressions for the in-memory store. */
class InMemoryJobStoreHardeningTest {

    private InMemoryJobStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryJobStore();
    }

    private static Job job(String handler, int priority) {
        return Job.builder()
                .spec(JobSpec.of(handler, new JobArgument("java.lang.String", "\"x\"")))
                .priority(priority)
                .build();
    }

    @Test
    void integerMinValuePrioritySortsLastLikeTheRelationalBackends() {
        Job lowest = job("com.example.Lowest", Integer.MIN_VALUE);
        store.insert(lowest);
        Job normal = job("com.example.Normal", 0);
        store.insert(normal);

        // -Integer.MIN_VALUE == Integer.MIN_VALUE: the historical negating
        // comparator sorted the MIN_VALUE job FIRST, diverging from
        // Postgres ORDER BY priority DESC.
        List<Job> claimed = store.claimReady(NodeId.newId(), "default", 1, Instant.now());
        assertThat(claimed).extracting(Job::id).containsExactly(normal.id());
    }

    @Test
    void zombieExecutionUpdateFromAPreviousAttemptIsRejected() {
        var node = NodeId.newId();
        Job inserted = job("com.example.H", 0);
        store.insert(inserted);

        // Attempt 1, claimed by this node.
        Job attemptOne = store.claimReady(node, "default", 1, Instant.now()).get(0);
        assertThat(attemptOne.attempts()).isEqualTo(1);

        // Orphan-reclaim shape: attempt 1 fails, is rescheduled, promoted,
        // and re-claimed by the SAME node as attempt 2.
        long v = attemptOne.version();
        Job failed = store.findById(inserted.id()).orElseThrow();
        failed.transitionTo(JobState.FAILED, Instant.now(), "test", "orphaned");
        failed.clearOwner();
        store.saveAtomic(failed, v);
        Job scheduled = store.findById(inserted.id()).orElseThrow();
        v = scheduled.version();
        scheduled.transitionTo(JobState.ENQUEUED, Instant.now(), "test", null);
        store.saveAtomic(scheduled, v);
        Job attemptTwo = store.claimReady(node, "default", 1, Instant.now()).get(0);
        assertThat(attemptTwo.attempts()).isEqualTo(2);
        attemptTwo.checkIn(Instant.now());
        assertThat(store.saveExecutionUpdate(attemptTwo, node)).isTrue();

        // The stale writer from attempt 1 passes the state and owner checks
        // but must not overwrite the live attempt's wire form.
        attemptOne.log().info("zombie write");
        assertThat(store.saveExecutionUpdate(attemptOne, node)).isFalse();
        Job persisted = store.findById(inserted.id()).orElseThrow();
        assertThat(persisted.attempts()).isEqualTo(2);
        assertThat(persisted.log().snapshot())
                .noneSatisfy(e -> assertThat(e.message()).isEqualTo("zombie write"));
    }

    @Test
    void insertAllWithDuplicateIdsRejectsTheBatchWithoutPhantomVisibility() {
        Job a = job("com.example.A", 0);
        Job duplicate = Job.builder()
                .id(a.id())
                .spec(JobSpec.of("com.example.B", new JobArgument("java.lang.String", "\"x\"")))
                .build();

        assertThatThrownBy(() -> store.insertAll(List.of(a, duplicate))).isInstanceOf(IllegalStateException.class);

        // Nothing from the rejected batch is visible, and versions are untouched.
        assertThat(store.findById(a.id())).isEmpty();
        assertThat(a.version()).isZero();
        assertThat(store.countsByState().getOrDefault(JobState.ENQUEUED, 0L)).isZero();
    }

    @Test
    void insertAllDuplicateOfAnExistingJobLeavesTheStoreUntouched() {
        Job existing = job("com.example.Existing", 0);
        store.insert(existing);
        Job fresh = job("com.example.Fresh", 0);
        Job clash = Job.builder()
                .id(existing.id())
                .spec(JobSpec.of("com.example.Clash", new JobArgument("java.lang.String", "\"x\"")))
                .build();

        assertThatThrownBy(() -> store.insertAll(List.of(fresh, clash))).isInstanceOf(IllegalStateException.class);

        assertThat(store.findById(fresh.id())).isEmpty();
        assertThat(store.findById(existing.id()).orElseThrow().spec().handlerType())
                .isEqualTo("com.example.Existing");
    }
}
