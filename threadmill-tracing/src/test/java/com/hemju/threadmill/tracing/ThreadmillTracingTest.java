package com.hemju.threadmill.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobLog;
import com.hemju.threadmill.core.JobMetadata;
import com.hemju.threadmill.core.JobProgress;
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
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

class ThreadmillTracingTest {

  private InMemorySpanExporter exporter;
  private SdkTracerProvider provider;
  private ThreadmillTracing tracing;

  @BeforeEach
  void setUp() {
    exporter = InMemorySpanExporter.create();
    provider = SdkTracerProvider.builder()
        .addSpanProcessor(SimpleSpanProcessor.create(exporter))
        .build();
    tracing = ThreadmillTracing.of(
        OpenTelemetrySdk.builder().setTracerProvider(provider).build());
  }

  @AfterEach
  void tearDown() {
    provider.close();
  }

  @Test
  void processingInterceptorCreatesSpanAroundHandlerWindow() {
    Job job = sample();
    job.transitionTo(JobState.PROCESSING, Instant.now());
    var ctx = context(job);
    JobInterceptor interceptor = tracing.asInterceptor();

    interceptor.onProcessingStarting(job, ctx);
    assertThat(Span.current().getSpanContext().isValid()).isTrue();

    job.transitionTo(JobState.SUCCEEDED, Instant.now());
    interceptor.onProcessingSucceeded(job, ctx);
    interceptor.onProcessingFinished(job, ctx);

    var spans = exporter.getFinishedSpanItems();
    assertThat(spans).hasSize(1);
    var span = spans.getFirst();
    assertThat(span.getName()).isEqualTo("threadmill.job.process");
    assertThat(span.getAttributes().get(ThreadmillTracing.JOB_ID))
        .isEqualTo(job.id().toString());
    assertThat(span.getAttributes().get(ThreadmillTracing.QUEUE)).isEqualTo("default");
    assertThat(span.getAttributes().get(ThreadmillTracing.FINAL_STATE)).isEqualTo("SUCCEEDED");
  }

  @Test
  void processingInterceptorRecordsFailureCauseAndException() {
    Job job = sample();
    job.transitionTo(JobState.PROCESSING, Instant.now());
    var ctx = context(job);
    JobInterceptor interceptor = tracing.asInterceptor();

    interceptor.onProcessingStarting(job, ctx);
    job.transitionTo(JobState.FAILED, Instant.now(), "test", "boom");
    interceptor.onProcessingFailed(
        job, ctx, new IllegalStateException("boom"), JobInterceptor.FailureCause.EXCEPTION);
    interceptor.onProcessingFinished(job, ctx);

    var span = exporter.getFinishedSpanItems().getFirst();
    assertThat(span.getStatus().getStatusCode().name()).isEqualTo("ERROR");
    assertThat(span.getAttributes().get(ThreadmillTracing.FAILURE_CAUSE)).isEqualTo("EXCEPTION");
    assertThat(span.getEvents())
        .anySatisfy(event -> assertThat(event.getName()).isEqualTo("exception"));
  }

  @Test
  void staleTerminalWriteStillClosesTheExecutionScopeAndSpan() throws Exception {
    var store = new InMemoryJobStore();
    var job = Job.builder().spec(JobSpec.of("example.Handler")).build();
    store.insert(job);
    var owner = NodeId.newId();
    var claimed = store.claimReady(owner, "default", 1, Instant.now()).getFirst();
    JobHandler<JobPayload> handler = (payload, ctx) -> store.softDelete(job.id());
    var runner = new JobRunner(
        store,
        owner,
        name -> handler,
        new JsonJobSerializer(),
        new JobInterceptors().add(tracing.asInterceptor()),
        ProcessingNodeConfig.defaults());
    try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
      assertThat(workers
              .submit(() -> {
                runner.run(claimed);
                return Span.current().getSpanContext().isValid();
              })
              .get(10, TimeUnit.SECONDS))
          .isFalse();
    } finally {
      runner.shutdown();
    }
    var spans = exporter.getFinishedSpanItems();
    assertThat(spans).hasSize(1);
    assertThat(spans.getFirst().getAttributes().get(ThreadmillTracing.COMPLETION_CONFIRMED))
        .isFalse();
    assertThat(spans.getFirst().getAttributes().get(ThreadmillTracing.FINAL_STATE))
        .isNull();
    assertThat(store.findById(job.id()).orElseThrow().currentState()).isEqualTo(JobState.DELETED);
  }

  @Test
  void orphanRecoveryOnAnotherThreadNeverClosesTheOriginalExecutionsScope() throws Exception {
    var job = sample();
    job.transitionTo(JobState.PROCESSING, Instant.now());
    var original = context(job);
    var recovery = context(job);
    var interceptor = tracing.asInterceptor();
    interceptor.onProcessingStarting(job, original);
    var originalSpan = Span.current().getSpanContext();
    try {
      job.transitionTo(JobState.FAILED, Instant.now());
      try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
        assertThat(workers
                .submit(() -> {
                  interceptor.onProcessingFailed(
                      job,
                      recovery,
                      new IllegalStateException("orphan"),
                      JobInterceptor.FailureCause.ORPHAN_RECLAIM);
                  interceptor.onProcessingFinished(job, recovery);
                  return Span.current().getSpanContext().isValid();
                })
                .get(10, TimeUnit.SECONDS))
            .isFalse();
      }
      assertThat(Span.current().getSpanContext()).isEqualTo(originalSpan);
      assertThat(exporter.getFinishedSpanItems()).hasSize(1);
    } finally {
      interceptor.onProcessingFinished(job, original);
    }
    assertThat(exporter.getFinishedSpanItems()).hasSize(2);
    assertThat(Span.current().getSpanContext().isValid()).isFalse();
  }

  @Test
  void storeDecoratorRecordsClaimCount() {
    var backing = new InMemoryJobStore();
    var store = tracing.wrapStore(backing);
    Job job = sample();
    store.insert(job);

    var claimed = store.claimReady(NodeId.newId(), "default", 10, Instant.now());

    assertThat(claimed).hasSize(1);
    var span = exporter.getFinishedSpanItems().stream()
        .filter(s -> s.getName().equals("threadmill.store.claim_ready"))
        .findFirst()
        .orElseThrow();
    assertThat(span.getAttributes().get(ThreadmillTracing.CLAIMED_COUNT)).isEqualTo(1L);
  }

  @Test
  void storeDecoratorExposesDelegateForFrameworkCapabilityDetection() {
    var backing = new InMemoryJobStore();
    var store = tracing.wrapStore(backing);

    assertThat(store.delegate()).isSameAs(backing);
  }

  private static Job sample() {
    return Job.builder()
        .spec(JobSpec.of("com.example.Handler", new JobArgument("java.lang.String", "\"x\"")))
        .build();
  }

  private static JobExecutionContext context(Job job) {
    return new JobExecutionContext() {
      private final NodeId nodeId = NodeId.newId();

      @Override
      public JobId jobId() {
        return job.id();
      }

      @Override
      public NodeId nodeId() {
        return nodeId;
      }

      @Override
      public int attempt() {
        return 1;
      }

      @Override
      public Instant claimedAt() {
        return Instant.now();
      }

      @Override
      public Instant deadline() {
        return Instant.MAX;
      }

      @Override
      public Optional<CancellationReason> cancellation() {
        return Optional.empty();
      }

      @Override
      public void checkIn() {}

      @Override
      public void setResult(Object value) {}

      @Override
      public JobLog log() {
        return job.log();
      }

      @Override
      public JobProgress progress() {
        return job.progress();
      }

      @Override
      public JobMetadata metadata() {
        return job.metadata();
      }

      @Override
      public Optional<Object> readResult() {
        return Optional.empty();
      }
    };
  }
}
