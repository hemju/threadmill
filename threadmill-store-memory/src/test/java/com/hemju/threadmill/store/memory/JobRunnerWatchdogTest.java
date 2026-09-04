package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.JobInterceptors;
import com.hemju.threadmill.core.engine.JobRunner;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobHandlerResolver;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.handler.ReflectiveJobHandlerResolver;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.ForwardingJobStore;

/**
 * The per-job timeout watchdog uses a ScheduledThreadPoolExecutor with
 * removeOnCancelPolicy enabled, so a completed job's cancelled watchdog (whose
 * initial delay can be minutes out) is dropped from the delay queue immediately
 * instead of retaining the captured Job graph until its scheduled time.
 */
class JobRunnerWatchdogTest {

  private final JsonJobSerializer serializer = new JsonJobSerializer();

  @Test
  @DisplayName("cancelled watchdog tasks are removed from the delay queue, not retained")
  void cancelledWatchdogTasksDoNotAccumulate() throws Exception {
    var store = new InMemoryJobStore();
    var nodeId = NodeId.newId();
    var runner = new JobRunner(
        store,
        nodeId,
        new ReflectiveJobHandlerResolver(),
        serializer,
        new JobInterceptors(),
        ProcessingNodeConfig.builder().jobTimeout(Duration.ofMinutes(5)).build());
    try {
      var field = JobRunner.class.getDeclaredField("timeoutExecutor");
      field.setAccessible(true);
      var executor = (ScheduledThreadPoolExecutor) field.get(runner);
      assertThat(executor.getRemoveOnCancelPolicy()).isTrue();

      for (int i = 0; i < 200; i++) {
        JobArgument arg = serializer.serializePayload(new EngineTestHandlers.HelloPayload("x"));
        Job job = Job.builder()
            .spec(new JobSpec(EngineTestHandlers.CountingHandler.class.getName(), List.of(arg)))
            .build();
        store.insert(job);
        Job claimed = store.claimReady(nodeId, "default", 1, Instant.now()).get(0);
        runner.run(claimed);
      }

      // With removeOnCancelPolicy, each completed job's watchdog leaves the
      // queue at once; it would otherwise hold ~200 entries for 5 minutes.
      await().atMost(Duration.ofSeconds(5)).until(() -> executor.getQueue().isEmpty());
      assertThat(executor.getQueue()).isEmpty();
    } finally {
      runner.shutdown();
    }
  }

  @Test
  @DisplayName("a fatal handler error cancels its watchdog before escaping")
  void fatalHandlerErrorCancelsWatchdogBeforeEscaping() throws Exception {
    var store = new InMemoryJobStore();
    var nodeId = NodeId.newId();
    var fatal = new TestVirtualMachineError();
    JobHandler<JobPayload> handler = (payload, ctx) -> {
      throw new IllegalStateException("wrapped fatal", fatal);
    };
    JobHandlerResolver resolver = ignored -> handler;
    var runner = new JobRunner(
        store,
        nodeId,
        resolver,
        serializer,
        new JobInterceptors(),
        ProcessingNodeConfig.builder().jobTimeout(Duration.ofMinutes(5)).build());
    try {
      var field = JobRunner.class.getDeclaredField("timeoutExecutor");
      field.setAccessible(true);
      var executor = (ScheduledThreadPoolExecutor) field.get(runner);
      JobArgument arg = serializer.serializePayload(new EngineTestHandlers.HelloPayload("fatal"));
      Job job =
          Job.builder().spec(new JobSpec("example.FatalHandler", List.of(arg))).build();
      store.insert(job);
      Job claimed = store.claimReady(nodeId, "default", 1, Instant.now()).getFirst();

      assertThatThrownBy(() -> runner.run(claimed)).isSameAs(fatal);
      await().atMost(Duration.ofSeconds(5)).until(() -> executor.getQueue().isEmpty());
      assertThat(executor.getQueue()).isEmpty();
    } finally {
      runner.shutdown();
    }
  }

  /**
   * The watchdog must be disarmed as soon as the handler returns, not merely
   * before the attempt leaves {@code run}. {@code saveTerminalWithRetry} retains
   * finalization responsibility for the whole length of a store outage, so a
   * watchdog still armed across {@code markSucceeded} interrupts that retry loop
   * at its {@code Thread.sleep}, aborts it, and routes a handler that already
   * succeeded through the failure path — where {@code RetryInterceptor} runs it
   * a second time.
   */
  @Test
  @DisplayName("a successful handler still finalizes through an outage that outlasts the timeout")
  void successfulHandlerRetainsFinalizationThroughAnOutageOutlastingTheJobTimeout()
      throws Exception {
    var inner = new InMemoryJobStore();
    var outage = new AtomicBoolean(true);
    var terminalSaveAttempted = new CountDownLatch(1);
    var terminalSaveAttempts = new AtomicInteger();
    var store = new ForwardingJobStore(inner) {
      @Override
      public void saveAtomic(Job job, long expectedVersion) {
        if (job.currentState() == JobState.SUCCEEDED && outage.get()) {
          terminalSaveAttempts.incrementAndGet();
          terminalSaveAttempted.countDown();
          throw new IllegalStateException("simulated store outage");
        }
        super.saveAtomic(job, expectedVersion);
      }
    };
    var nodeId = NodeId.newId();
    // The handler returns immediately, but the terminal-save retry loop outlives
    // the per-job timeout — so the watchdog deadline passes mid-finalization.
    var config =
        ProcessingNodeConfig.builder().jobTimeout(Duration.ofMillis(250)).build();
    JobHandler<JobPayload> handler = (payload, ctx) -> {};
    JobHandlerResolver resolver = ignored -> handler;
    var runner = new JobRunner(store, nodeId, resolver, serializer, new JobInterceptors(), config);
    Thread recovery = null;
    try {
      JobArgument arg = serializer.serializePayload(new EngineTestHandlers.HelloPayload("x"));
      Job job = Job.builder()
          .spec(new JobSpec("example.SucceedingHandler", List.of(arg)))
          .build();
      store.insert(job);
      Job claimed = store.claimReady(nodeId, "default", 1, Instant.now()).getFirst();

      recovery = Thread.ofVirtual().name("threadmill-watchdog-outage-recovery").start(() -> {
        try {
          terminalSaveAttempted.await();
          Thread.sleep(750);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
        }
        outage.set(false);
      });

      runner.run(claimed);

      assertThat(terminalSaveAttempts)
          .describedAs("the outage must actually have been exercised")
          .hasValueGreaterThan(1);
      assertThat(store.findById(claimed.id()).orElseThrow().currentState())
          .describedAs("a handler that returned successfully must not become FAILED just"
              + " because its terminal save outlasted the per-job timeout")
          .isEqualTo(JobState.SUCCEEDED);
    } finally {
      outage.set(false);
      if (recovery != null) {
        recovery.interrupt();
        recovery.join(TimeUnit.SECONDS.toMillis(5));
      }
      runner.shutdown();
    }
  }

  private static final class TestVirtualMachineError extends VirtualMachineError {
    private static final long serialVersionUID = 1L;
  }
}
