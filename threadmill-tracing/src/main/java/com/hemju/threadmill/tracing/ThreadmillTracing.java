package com.hemju.threadmill.tracing;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.engine.JobInterceptor;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.store.JobStore;

/** Optional OpenTelemetry integration for Threadmill. */
public final class ThreadmillTracing {

    public static final String INSTRUMENTATION_NAME = "com.hemju.threadmill";

    static final AttributeKey<String> JOB_ID = AttributeKey.stringKey("threadmill.job.id");
    static final AttributeKey<String> QUEUE = AttributeKey.stringKey("threadmill.queue");
    static final AttributeKey<String> HANDLER = AttributeKey.stringKey("threadmill.handler");
    static final AttributeKey<Long> ATTEMPT = AttributeKey.longKey("threadmill.attempt");
    static final AttributeKey<String> NODE_ID = AttributeKey.stringKey("threadmill.node.id");
    static final AttributeKey<String> FINAL_STATE = AttributeKey.stringKey("threadmill.final_state");
    static final AttributeKey<String> FAILURE_CAUSE = AttributeKey.stringKey("threadmill.failure_cause");
    static final AttributeKey<String> STORE = AttributeKey.stringKey("threadmill.store");
    static final AttributeKey<Long> CLAIMED_COUNT = AttributeKey.longKey("threadmill.claimed_count");
    static final AttributeKey<Long> JOB_COUNT = AttributeKey.longKey("threadmill.job_count");

    private final Tracer tracer;

    private ThreadmillTracing(OpenTelemetry openTelemetry) {
        this.tracer = Objects.requireNonNull(openTelemetry, "openTelemetry").getTracer(INSTRUMENTATION_NAME);
    }

    /** Use the globally configured OpenTelemetry instance, including the Java agent when present. */
    public static ThreadmillTracing global() {
        return of(GlobalOpenTelemetry.get());
    }

    /** Use an application-supplied OpenTelemetry instance. */
    public static ThreadmillTracing of(OpenTelemetry openTelemetry) {
        return new ThreadmillTracing(openTelemetry);
    }

    /** Decorate a store so Threadmill store operations emit spans. */
    public TracingJobStore wrapStore(JobStore store) {
        return new TracingJobStore(store, tracer);
    }

    /** Return a processing interceptor that emits one span per job attempt. */
    public JobInterceptor asInterceptor() {
        return new TracingInterceptor(tracer);
    }

    private static final class TracingInterceptor implements JobInterceptor {
        private final Tracer tracer;
        private final ConcurrentHashMap<String, ActiveSpan> spans = new ConcurrentHashMap<>();

        private TracingInterceptor(Tracer tracer) {
            this.tracer = tracer;
        }

        @Override
        public void onProcessingStarting(Job job, JobExecutionContext ctx) {
            var span = baseSpan(job, ctx).startSpan();
            // JobRunner invokes start, handler execution, and finish/failure hooks on the
            // same execution thread; this Scope intentionally spans the handler call.
            var scope = span.makeCurrent();
            spans.put(job.id().toString(), new ActiveSpan(span, scope));
        }

        @Override
        public void onProcessingSucceeded(Job job, JobExecutionContext ctx) {
            finish(job);
        }

        @Override
        public void onProcessingFailed(Job job, JobExecutionContext ctx, Throwable cause, FailureCause causeKind) {
            ActiveSpan active = spans.remove(job.id().toString());
            if (active == null) {
                var span = baseSpan(job, ctx).startSpan();
                active = new ActiveSpan(span, span.makeCurrent());
            }
            try {
                active.span().setAttribute(FAILURE_CAUSE, causeKind.name());
                if (cause != null) {
                    active.span().recordException(cause);
                    active.span()
                            .setStatus(
                                    StatusCode.ERROR,
                                    cause.getMessage() == null ? causeKind.name() : cause.getMessage());
                } else {
                    active.span().setStatus(StatusCode.ERROR, causeKind.name());
                }
                active.span().setAttribute(FINAL_STATE, job.currentState().name());
            } finally {
                active.close();
            }
        }

        private void finish(Job job) {
            ActiveSpan active = spans.remove(job.id().toString());
            if (active == null) return;
            try {
                active.span().setAttribute(FINAL_STATE, job.currentState().name());
            } finally {
                active.close();
            }
        }

        private SpanBuilder baseSpan(Job job, JobExecutionContext ctx) {
            return tracer.spanBuilder("threadmill.job.process")
                    .setSpanKind(SpanKind.INTERNAL)
                    .setAttribute(JOB_ID, job.id().toString())
                    .setAttribute(QUEUE, job.queue())
                    .setAttribute(HANDLER, job.spec().handlerType())
                    .setAttribute(ATTEMPT, ctx.attempt())
                    .setAttribute(NODE_ID, ctx.nodeId().toString());
        }
    }

    private record ActiveSpan(Span span, Scope scope) implements AutoCloseable {
        @Override
        public void close() {
            try {
                scope.close();
            } finally {
                span.end();
            }
        }
    }
}
