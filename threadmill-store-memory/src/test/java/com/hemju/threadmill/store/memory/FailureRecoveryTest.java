package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.FailureDecision;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobRelationship;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.JobInterceptors;
import com.hemju.threadmill.core.engine.JobRunner;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.engine.RetryInterceptor;
import com.hemju.threadmill.core.engine.RetryPolicy;
import com.hemju.threadmill.core.engine.WorkflowInterceptor;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.ForwardingJobStore;

class FailureRecoveryTest {
  private final InMemoryJobStore store = new InMemoryJobStore();
  private static final JobSpec SPEC = JobSpec.of("example.Handler");

  @Test
  void exceptionSpecificNoRetrySurvivesFailureRecovery() {
    var parent = Job.builder().spec(SPEC).build();
    store.insert(parent);
    runFailingAttempt(parent, RetryPolicy.noRetry());
    assertThat(store.findById(parent.id()).orElseThrow().failureDecision())
        .contains(FailureDecision.finalFailure());
    assertThat(new RetryInterceptor(store, 5, Duration.ZERO)
            .recoverStrandedFailures(500, Duration.ZERO))
        .isZero();
    assertThat(store.findById(parent.id()).orElseThrow().currentState()).isEqualTo(JobState.FAILED);
  }

  @Test
  void retryHandoffFailureAndYoungParentNeverAbandonWaitingChildren() {
    var parent = Job.builder().spec(SPEC).build();
    store.insert(parent);
    var child = waitingChild(parent);
    runFailingAttempt(parent, new RetryPolicy(3, Duration.ofSeconds(7)));
    var failed = store.findById(parent.id()).orElseThrow();
    var decision = failed.failureDecision().orElseThrow();
    assertThat(failed.currentState()).isEqualTo(JobState.FAILED);
    assertThat(decision.willRetry()).isTrue();
    var recovery = new RetryInterceptor(store, 1, Duration.ofHours(1));
    assertThat(recovery.recoverStrandedFailures(500, Duration.ofMinutes(5))).isZero();
    new WorkflowInterceptor(store).reconcileOrphanedAwaitingChildren(500);
    assertThat(store.findById(child.id()).orElseThrow().currentState())
        .isEqualTo(JobState.AWAITING);
    assertThat(recovery.recoverStrandedFailures(500, Duration.ZERO)).isEqualTo(1);
    assertThat(store.findById(parent.id()).orElseThrow().scheduledFor())
        .contains(decision.retryAt());
  }

  @Test
  void reschedulingThreePagesDoesNotSkipParentsOrDeleteTheirChildren() {
    var jobs = new ArrayList<Job>();
    var at = Instant.now().minusSeconds(7200);
    for (int i = 0; i < 1001; i++) {
      jobs.add(Job.builder()
          .spec(SPEC)
          .initialState(JobState.FAILED)
          .attempts(1)
          .createdAt(at.plusSeconds(i))
          .failureDecision(new FailureDecision(at, false))
          .build());
    }
    store.insertAll(jobs.subList(0, 1000));
    store.insertAll(jobs.subList(1000, jobs.size()));
    var child = waitingChild(jobs.getFirst());
    var retry = new RetryInterceptor(store, 3, Duration.ZERO);
    int recovered = 0;
    for (int pass = 0; pass < 100 && recovered < 1001; pass++) {
      int batch = retry.recoverStrandedFailures(500, Duration.ZERO);
      assertThat(batch).isBetween(0, 500);
      recovered += batch;
    }
    assertThat(recovered).isEqualTo(1001);
    new WorkflowInterceptor(store).reconcileOrphanedAwaitingChildren(500);
    assertThat(store.findById(child.id()).orElseThrow().currentState())
        .isEqualTo(JobState.AWAITING);
    assertThat(store.countsByState().get(JobState.FAILED)).isZero();
  }

  @Test
  void shutdownDecisionRefundsTheAttemptExactlyOnceAfterRecovery() {
    var job = Job.builder()
        .spec(SPEC)
        .initialState(JobState.FAILED)
        .attempts(1)
        .failureDecision(new FailureDecision(Instant.now(), true))
        .build();
    store.insert(job);
    var recovery = new RetryInterceptor(store, 1, Duration.ZERO);
    assertThat(recovery.recoverStrandedFailures(500, Duration.ZERO)).isEqualTo(1);
    assertThat(recovery.recoverStrandedFailures(500, Duration.ZERO)).isZero();
    assertThat(store.findById(job.id()).orElseThrow().attempts()).isZero();
  }

  @Test
  void legacyFailureWithoutADecisionIsNeitherRetriedNorAssumedFinal() {
    var parent =
        Job.builder().spec(SPEC).initialState(JobState.FAILED).attempts(1).build();
    store.insert(parent);
    var child = waitingChild(parent);
    assertThat(new RetryInterceptor(store, 3, Duration.ZERO)
            .recoverStrandedFailures(500, Duration.ZERO))
        .isZero();
    new WorkflowInterceptor(store).reconcileOrphanedAwaitingChildren(500);
    assertThat(store.findById(child.id()).orElseThrow().currentState())
        .isEqualTo(JobState.AWAITING);
  }

  private Job waitingChild(Job parent) {
    var child = Job.builder()
        .spec(SPEC)
        .initialState(JobState.AWAITING)
        .relationship(new JobRelationship(parent.id(), JobRelationship.Kind.WORKFLOW_STEP))
        .build();
    store.insert(child);
    return child;
  }

  private void runFailingAttempt(Job parent, RetryPolicy policy) {
    var failing = new ForwardingJobStore(store) {
      @Override
      public void saveAtomic(Job job, long expectedVersion) {
        if (job.currentState() == JobState.SCHEDULED) {
          throw new IllegalStateException("retry handoff unavailable");
        }
        super.saveAtomic(job, expectedVersion);
      }
    };
    var retry = new RetryInterceptor(failing, 5, Duration.ZERO)
        .policyFor(IllegalArgumentException.class, policy);
    var interceptors = new JobInterceptors().add(retry).add(new WorkflowInterceptor(store));
    var owner = NodeId.newId();
    var claimed = store.claimReady(owner, "default", 1, Instant.now()).getFirst();
    assertThat(claimed.id()).isEqualTo(parent.id());
    JobHandler<JobPayload> handler = (payload, context) -> {
      throw new IllegalArgumentException("failed");
    };
    var runner = new JobRunner(
        failing,
        owner,
        type -> handler,
        new JsonJobSerializer(),
        interceptors,
        ProcessingNodeConfig.defaults());
    try {
      runner.run(claimed);
    } finally {
      runner.shutdown();
    }
  }
}
