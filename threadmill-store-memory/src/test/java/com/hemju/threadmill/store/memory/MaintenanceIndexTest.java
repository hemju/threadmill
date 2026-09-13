package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.spec.JobSpec;

class MaintenanceIndexTest {
  @Test
  void indexedPagesFollowClaimsHeartbeatsStateChangesDeletionAndBulkInsert() {
    var store = new InMemoryJobStore();
    var first = Job.builder().spec(JobSpec.of("example.Handler")).build();
    var second = Job.builder().spec(first.spec()).build();
    store.insertAll(List.of(first, second));
    var owner = NodeId.newId();
    var claimed = store.claimReady(owner, "default", 2, Instant.now());
    assertThat(store.scanJobs(JobState.ENQUEUED, null, 100)).isEmpty();
    assertThat(store.scanJobs(JobState.PROCESSING, null, 100)).hasSize(2);
    var beat = Instant.now().plusSeconds(1);
    store.touchOwnerHeartbeat(owner, beat);
    assertThat(store.scanJobs(JobState.PROCESSING, null, 100))
        .allSatisfy(job -> assertThat(job.ownerHeartbeatAt()).contains(beat));
    var done = claimed.getFirst();
    var terminalAt = Instant.now().minusSeconds(1);
    done.transitionTo(JobState.SUCCEEDED, terminalAt);
    store.saveAtomic(done, done.version());
    assertThat(store.scanJobs(JobState.PROCESSING, null, 100)).hasSize(1);
    assertThat(store.scanJobs(JobState.SUCCEEDED, null, 100))
        .extracting(Job::id)
        .containsExactly(done.id());
    store.softDelete(claimed.getLast().id());
    assertThat(store.scanJobs(JobState.PROCESSING, null, 100)).isEmpty();
    assertThat(store
            .deleteFinishedPage(Instant.now(), JobState.SUCCEEDED, 100, null)
            .deleted())
        .isEqualTo(1);
    assertThat(store.scanJobs(JobState.SUCCEEDED, null, 100)).isEmpty();
    assertThat(store.findById(done.id())).isEmpty();
    assertThat(store.scanJobs(JobState.DELETED, null, 100)).hasSize(1);
  }
}
