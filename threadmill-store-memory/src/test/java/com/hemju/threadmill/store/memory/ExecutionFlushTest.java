package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.ExecutionContext;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.ForwardingJobStore;

class ExecutionFlushTest {
  @Test
  void concurrentContextFlushesAreSerializedUntilTheFirstWriteCompletes() throws Exception {
    var backing = new InMemoryJobStore();
    var firstEntered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var overlapped = new CountDownLatch(1);
    var active = new AtomicInteger();
    var calls = new AtomicInteger();
    var store = new ForwardingJobStore(backing) {
      @Override
      public boolean saveExecutionUpdate(Job job, NodeId nodeId) {
        if (active.incrementAndGet() > 1) overlapped.countDown();
        try {
          if (calls.incrementAndGet() == 1) {
            firstEntered.countDown();
            if (!release.await(5, TimeUnit.SECONDS))
              throw new AssertionError("First flush was never released");
          }
          return super.saveExecutionUpdate(job, nodeId);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new AssertionError(e);
        } finally {
          active.decrementAndGet();
        }
      }
    };
    var job = Job.builder().spec(JobSpec.of("example.Handler")).build();
    store.insert(job);
    var node = NodeId.newId();
    var claimed = store.claimReady(node, "default", 1, Instant.now()).getFirst();
    var config = ProcessingNodeConfig.defaults();
    var context = new ExecutionContext(
        claimed,
        store,
        claimed.id(),
        node,
        claimed.attempts(),
        Instant.now(),
        config.jobTimeout(),
        Optional::empty,
        claimed.log(),
        claimed.progress(),
        claimed.metadata(),
        new JsonJobSerializer(),
        config);
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var first = executor.submit(context::flushBestEffort);
      assertThat(firstEntered.await(5, TimeUnit.SECONDS)).isTrue();
      var second = executor.submit(context::flushBestEffort);
      try {
        assertThat(overlapped.await(150, TimeUnit.MILLISECONDS)).isFalse();
      } finally {
        release.countDown();
      }
      first.get(5, TimeUnit.SECONDS);
      second.get(5, TimeUnit.SECONDS);
    }
    assertThat(store.findById(job.id()).orElseThrow().executionRevision()).isEqualTo(2);
  }
}
