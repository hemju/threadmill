package com.hemju.threadmill.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Optional;

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
import com.hemju.threadmill.core.handler.JobExecutionContext;
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

        var span = exporter.getFinishedSpanItems().getFirst();
        assertThat(span.getStatus().getStatusCode().name()).isEqualTo("ERROR");
        assertThat(span.getAttributes().get(ThreadmillTracing.FAILURE_CAUSE)).isEqualTo("EXCEPTION");
        assertThat(span.getEvents())
                .anySatisfy(event -> assertThat(event.getName()).isEqualTo("exception"));
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
