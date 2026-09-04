package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.Dispatcher;
import com.hemju.threadmill.core.engine.JobInterceptor;
import com.hemju.threadmill.core.engine.JobInterceptors;
import com.hemju.threadmill.core.engine.JobRunner;
import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.engine.MaintenanceCycle;
import com.hemju.threadmill.core.engine.NodeRegistry;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.engine.RetryInterceptor;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobHandlerResolver;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.handler.ReflectiveJobHandlerResolver;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.RecurringMaterializer;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.test.ForwardingJobStore;

class FatalErrorBoundaryTest {

  private static final Duration ASYNC_TIMEOUT = Duration.ofSeconds(5);

  private final JsonJobSerializer serializer = new JsonJobSerializer();

  @Test
  void processFatalErrorsEscapeHandlerWithoutEnteringTheFailurePath() {
    for (Error fatal : fatalErrors()) {
      var store = new InMemoryJobStore();
      var nodeId = NodeId.newId();
      var failureHookCalled = new AtomicBoolean();
      var interceptors = new JobInterceptors().add(new JobInterceptor() {
        @Override
        public void onProcessingFailed(
            Job job, JobExecutionContext ctx, Throwable cause, FailureCause kind) {
          failureHookCalled.set(true);
        }
      });
      JobHandler<JobPayload> handler = (payload, ctx) -> {
        throw new IllegalStateException("wrapped fatal handler failure", fatal);
      };
      JobHandlerResolver resolver = ignored -> handler;
      var runner = new JobRunner(
          store, nodeId, resolver, serializer, interceptors, ProcessingNodeConfig.defaults());
      Job claimed = insertAndClaim(store, nodeId);

      try {
        assertThatThrownBy(() -> runner.run(claimed)).isSameAs(fatal);
        assertThat(store.findById(claimed.id()).orElseThrow().currentState())
            .isEqualTo(JobState.PROCESSING);
        assertThat(failureHookCalled).isFalse();
      } finally {
        runner.shutdown();
      }
    }
  }

  @Test
  void ordinaryAssertionErrorIsIsolatedToOneJob() {
    var store = new InMemoryJobStore();
    var nodeId = NodeId.newId();
    var failureHookCalled = new AtomicBoolean();
    var interceptors = new JobInterceptors().add(new JobInterceptor() {
      @Override
      public void onProcessingFailed(
          Job job, JobExecutionContext ctx, Throwable cause, FailureCause kind) {
        failureHookCalled.set(true);
      }
    });
    JobHandler<JobPayload> handler = (payload, ctx) -> {
      throw new AssertionError("handler assertion");
    };
    JobHandlerResolver resolver = ignored -> handler;
    var runner = new JobRunner(
        store, nodeId, resolver, serializer, interceptors, ProcessingNodeConfig.defaults());
    Job claimed = insertAndClaim(store, nodeId);

    try {
      runner.run(claimed);

      assertThat(store.findById(claimed.id()).orElseThrow().currentState())
          .isEqualTo(JobState.FAILED);
      assertThat(failureHookCalled).isTrue();
    } finally {
      runner.shutdown();
    }
  }

  @Test
  void processFatalErrorsEscapeDispatcherLoop() throws Exception {
    for (Error fatal : fatalErrors()) {
      assertDispatcherFatalEscapes(fatal);
    }
  }

  @Test
  void processFatalHandlerErrorsReachTheWorkerUncaughtBoundary() throws Exception {
    for (Error fatal : fatalErrors()) {
      assertWorkerFatalReachesUncaughtBoundary(fatal);
    }
  }

  @Test
  void processFatalErrorsEscapeMaintenanceLoop() throws Exception {
    for (Error fatal : fatalErrors()) {
      assertMaintenanceFatalEscapes(fatal);
    }
  }

  @Test
  void processFatalErrorsEscapeRegistryBoundary() {
    for (Error fatal : fatalErrors()) {
      var heartbeatAttempts = new AtomicInteger();
      var store = new ForwardingJobStore(new InMemoryJobStore()) {
        @Override
        public void recordNodeHeartbeat(NodeId nodeId, Instant now) {
          if (heartbeatAttempts.getAndIncrement() == 0) {
            throw new IllegalStateException("wrapped fatal registry failure", fatal);
          }
          super.recordNodeHeartbeat(nodeId, now);
        }
      };
      var registry = new NodeRegistry(
          store,
          NodeId.newId(),
          Duration.ofSeconds(1),
          Duration.ofMillis(100),
          Duration.ofSeconds(1));

      assertThatThrownBy(registry::start).isSameAs(fatal);
      try {
        registry.start();
        assertThat(heartbeatAttempts).hasValueGreaterThanOrEqualTo(2);
      } finally {
        registry.stop();
      }
    }
  }

  @Test
  void fatalRegistryCleanupStillShutsDownTheTimeoutWatchdog() throws Exception {
    var fatal = new TestVirtualMachineError();
    var store = new ForwardingJobStore(new InMemoryJobStore()) {
      @Override
      public void releaseMaintenanceLease(NodeId nodeId) {
        throw new IllegalStateException("wrapped fatal registry cleanup", fatal);
      }
    };
    var node = ProcessingNode.builder(store).build();
    var runnerField = ProcessingNode.class.getDeclaredField("runner");
    runnerField.setAccessible(true);
    var runner = (JobRunner) runnerField.get(node);
    var executorField = JobRunner.class.getDeclaredField("timeoutExecutor");
    executorField.setAccessible(true);
    var timeoutExecutor = (ScheduledExecutorService) executorField.get(runner);
    node.start();

    assertThatThrownBy(node::close).isSameAs(fatal);
    assertThat(timeoutExecutor.isShutdown()).isTrue();
  }

  @Test
  void productionWorkerThreadsHaveDiagnosticNames() throws Exception {
    var store = new InMemoryJobStore();
    var workerName = new CompletableFuture<String>();
    JobHandler<JobPayload> handler =
        (payload, ctx) -> workerName.complete(Thread.currentThread().getName());
    var node = ProcessingNode.builder(store)
        .config(
            ProcessingNodeConfig.builder().pollInterval(Duration.ofMillis(10)).build())
        .handlerResolver(ignored -> handler)
        .build();
    insertJob(store);

    try {
      node.start();
      assertThat(workerName.get(ASYNC_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
          .startsWith("threadmill-worker-");
    } finally {
      node.close();
    }
  }

  private void assertDispatcherFatalEscapes(Error fatal) throws Exception {
    var inner = new InMemoryJobStore();
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var store = new ForwardingJobStore(inner) {
      @Override
      public Set<String> listPausedQueues() {
        entered.countDown();
        awaitRelease(release);
        throw new IllegalStateException("wrapped fatal dispatcher failure", fatal);
      }
    };
    var nodeId = NodeId.newId();
    var config =
        ProcessingNodeConfig.builder().pollInterval(Duration.ofMillis(10)).build();
    var runner = new JobRunner(
        inner,
        nodeId,
        new ReflectiveJobHandlerResolver(),
        serializer,
        new JobInterceptors(),
        config);

    try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
      var dispatcher = new Dispatcher(store, nodeId, runner, workers, new Semaphore(1), config);
      try {
        dispatcher.start();
        Throwable escaped = releaseAndCapture("threadmill-dispatcher-" + nodeId, entered, release);
        assertThat(escaped).isSameAs(fatal);
      } finally {
        release.countDown();
        dispatcher.stop();
        runner.shutdown();
      }
    }
  }

  private void assertWorkerFatalReachesUncaughtBoundary(Error fatal) throws Exception {
    var store = new InMemoryJobStore();
    var nodeId = NodeId.newId();
    var config =
        ProcessingNodeConfig.builder().pollInterval(Duration.ofMillis(10)).build();
    JobHandler<JobPayload> handler = (payload, ctx) -> {
      throw new IllegalStateException("wrapped fatal handler failure", fatal);
    };
    JobHandlerResolver resolver = ignored -> handler;
    var runner = new JobRunner(store, nodeId, resolver, serializer, new JobInterceptors(), config);
    Job job = insertJob(store);
    var uncaught = new CompletableFuture<Throwable>();
    var threadFactory = Thread.ofVirtual()
        .name("threadmill-fatal-worker")
        .uncaughtExceptionHandler((thread, failure) -> uncaught.complete(failure))
        .factory();

    try (var workers = Executors.newThreadPerTaskExecutor(threadFactory)) {
      var dispatcher = new Dispatcher(store, nodeId, runner, workers, new Semaphore(1), config);
      try {
        dispatcher.start();
        assertThat(uncaught.get(ASYNC_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
            .isSameAs(fatal);
        assertThat(store.findById(job.id()).orElseThrow().currentState())
            .isEqualTo(JobState.PROCESSING);
      } finally {
        dispatcher.stop();
        runner.shutdown();
      }
    }
  }

  private void assertMaintenanceFatalEscapes(Error fatal) throws Exception {
    var inner = new InMemoryJobStore();
    var nodeId = NodeId.newId();
    var config = ProcessingNodeConfig.builder()
        .maintenancePollInterval(Duration.ofMillis(10))
        .claimHeartbeat(Duration.ofMillis(100))
        .heartbeatTimeout(Duration.ofSeconds(1))
        .maintenanceLeaseDuration(Duration.ofSeconds(1))
        .build();
    var registry = new NodeRegistry(
        inner,
        nodeId,
        config.heartbeatTimeout(),
        config.claimHeartbeat(),
        config.maintenanceLeaseDuration());
    registry.start();

    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var store = new ForwardingJobStore(inner) {
      @Override
      public List<CronTask> listCronTasks() {
        entered.countDown();
        awaitRelease(release);
        throw new IllegalStateException("wrapped fatal maintenance failure", fatal);
      }
    };
    var runner = new JobRunner(
        inner,
        nodeId,
        new ReflectiveJobHandlerResolver(),
        serializer,
        new JobInterceptors(),
        config);
    var retry =
        new RetryInterceptor(inner, config.defaultMaxAttempts(), config.retryInitialBackoff());
    var maintenance = new MaintenanceCycle(
        store,
        nodeId,
        registry,
        runner,
        new RecurringMaterializer(store),
        retry,
        config,
        new LocalWakeBus());

    try {
      maintenance.start();
      Throwable escaped = releaseAndCapture("threadmill-maintenance-" + nodeId, entered, release);
      assertThat(escaped).isSameAs(fatal);
    } finally {
      release.countDown();
      maintenance.stop();
      registry.stop();
      runner.shutdown();
    }
  }

  private Job insertAndClaim(InMemoryJobStore store, NodeId nodeId) {
    Job job = insertJob(store);
    return store.claimReady(nodeId, "default", 1, Instant.now()).getFirst();
  }

  private Job insertJob(InMemoryJobStore store) {
    JobArgument argument =
        serializer.serializePayload(new EngineTestHandlers.HelloPayload("fatal"));
    Job job = Job.builder()
        .spec(new JobSpec("example.FatalHandler", List.of(argument)))
        .build();
    store.insert(job);
    return job;
  }

  private static Throwable releaseAndCapture(
      String threadName, CountDownLatch entered, CountDownLatch release) throws Exception {
    assertThat(entered.await(ASYNC_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isTrue();
    Thread loop = Thread.getAllStackTraces().keySet().stream()
        .filter(thread -> thread.getName().equals(threadName))
        .findFirst()
        .orElseThrow();
    var uncaught = new CompletableFuture<Throwable>();
    loop.setUncaughtExceptionHandler((thread, failure) -> uncaught.complete(failure));
    release.countDown();
    return uncaught.get(ASYNC_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
  }

  private static void awaitRelease(CountDownLatch release) {
    try {
      if (!release.await(ASYNC_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
        throw new IllegalStateException("test did not release the fatal boundary");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while staging fatal boundary", e);
    }
  }

  @SuppressWarnings("removal")
  private static Error[] fatalErrors() {
    return new Error[] {new TestVirtualMachineError(), new ThreadDeath()};
  }

  private static final class TestVirtualMachineError extends VirtualMachineError {
    private static final long serialVersionUID = 1L;
  }
}
