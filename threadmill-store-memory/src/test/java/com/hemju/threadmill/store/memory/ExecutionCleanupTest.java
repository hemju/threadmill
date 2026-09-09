package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.JobInterceptor;
import com.hemju.threadmill.core.engine.JobInterceptors;
import com.hemju.threadmill.core.engine.JobRunner;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobSpec;

class ExecutionCleanupTest {
  @Test
  void cleanupUnwindsEveryInterceptorAndPreservesTheOriginalFatalError() {
    var store = new InMemoryJobStore();
    var owner = NodeId.newId();
    var job = claimed(store, owner);
    var order = new ArrayList<String>();
    var original = new InternalError("handler fatal");
    var cleanup = new InternalError("cleanup fatal");
    var hooks = new JobInterceptors()
        .add(finisher(() -> order.add("first")))
        .add(finisher(() -> {
          order.add("second");
          throw cleanup;
        }))
        .add(finisher(() -> order.add("third")));
    JobHandler<JobPayload> handler = (payload, ctx) -> {
      throw original;
    };
    var runner = new JobRunner(
        store,
        owner,
        name -> handler,
        new JsonJobSerializer(),
        hooks,
        ProcessingNodeConfig.defaults());
    try {
      assertThatThrownBy(() -> runner.run(job)).isSameAs(original);
      assertThat(original.getSuppressed()).containsExactly(cleanup);
      assertThat(order).containsExactly("third", "second", "first");
    } finally {
      runner.shutdown();
    }
  }

  @Test
  void recoveryAndReleaseAlsoFinishTheirOwnExecutionContexts() {
    var store = new InMemoryJobStore();
    var owner = NodeId.newId();
    var contexts = new ArrayList<JobExecutionContext>();
    var hooks = new JobInterceptors().add(new JobInterceptor() {
      @Override
      public void onProcessingFinished(Job job, JobExecutionContext ctx) {
        contexts.add(ctx);
      }
    });
    JobHandler<JobPayload> handler = (payload, ctx) -> {};
    var runner = new JobRunner(
        store,
        owner,
        name -> handler,
        new JsonJobSerializer(),
        hooks,
        ProcessingNodeConfig.defaults());
    try {
      var orphan = claimed(store, owner);
      runner.reclaimOrphan(orphan);
      var released = claimed(store, owner);
      runner.releaseWithoutRunning(released, "required tags absent");
      assertThat(contexts).hasSize(2).doesNotHaveDuplicates();
      assertThat(store.findById(orphan.id()).orElseThrow().currentState())
          .isEqualTo(JobState.FAILED);
      assertThat(store.findById(released.id()).orElseThrow().currentState())
          .isEqualTo(JobState.FAILED);
    } finally {
      runner.shutdown();
    }
  }

  private static JobInterceptor finisher(Runnable action) {
    return new JobInterceptor() {
      @Override
      public void onProcessingFinished(Job job, JobExecutionContext ctx) {
        action.run();
      }
    };
  }

  private static Job claimed(InMemoryJobStore store, NodeId owner) {
    var job = Job.builder().spec(JobSpec.of("example.Handler")).build();
    store.insert(job);
    return store.claimReady(owner, "default", 1, Instant.now()).getFirst();
  }
}
