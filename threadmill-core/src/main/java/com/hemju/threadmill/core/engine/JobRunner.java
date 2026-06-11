package com.hemju.threadmill.core.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.OversizedJobException;
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobHandlerResolver;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.serialization.SerializationException;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.store.JobStore;

/**
 * Single, centralised execution path: claim → process → complete.
 *
 * <p>All three failure modes — exception, timeout, orphan reclaim — funnel
 * through {@link #recordFailure(Job, ExecutionContext, Throwable, JobInterceptor.FailureCause)}
 * so the {@link JobInterceptor#onProcessingFailed} hook (and the retry
 * interceptor) is invoked exactly once per failure regardless of cause.
 *
 * <p>A job whose handler cannot be resolved or whose payload cannot be
 * deserialized is moved to {@code QUARANTINED}; it never crashes a loop.
 */
public final class JobRunner {

    private static final Logger LOG = LoggerFactory.getLogger(JobRunner.class);
    public static final String META_TIMEOUT_SECONDS = "threadmill.job.timeoutSeconds";

    private final JobStore store;
    private final NodeId nodeId;
    private final JobHandlerResolver resolver;
    private final JobSerializer serializer;
    private final JobInterceptors interceptors;
    private final Duration jobTimeout;
    private final ProcessingNodeConfig config;
    private final ScheduledExecutorService timeoutExecutor;

    public JobRunner(
            JobStore store,
            NodeId nodeId,
            JobHandlerResolver resolver,
            JobSerializer serializer,
            JobInterceptors interceptors,
            ProcessingNodeConfig config) {
        this.store = Objects.requireNonNull(store, "store");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.interceptors = Objects.requireNonNull(interceptors, "interceptors");
        this.config = Objects.requireNonNull(config, "config");
        this.jobTimeout = config.jobTimeout();
        // ScheduledThreadPoolExecutor directly (not Executors.newSingleThreadScheduledExecutor)
        // so removeOnCancelPolicy can be enabled: every completed job cancels a
        // watchdog whose initial delay is up to jobTimeout/noProgressTimeout. With
        // the default policy=false a cancelled-but-not-yet-due task lingers in the
        // delay queue for that whole window, retaining the captured ctx -> Job
        // graph; at high throughput that is a large, mysterious heap plateau.
        var executor = new ScheduledThreadPoolExecutor(
                1,
                r -> Thread.ofPlatform()
                        .name("threadmill-timeout-watchdog")
                        .daemon(true)
                        .unstarted(r));
        executor.setRemoveOnCancelPolicy(true);
        this.timeoutExecutor = executor;
    }

    /** Stops the timeout watchdog. Intended for engine shutdown. */
    public void shutdown() {
        timeoutExecutor.shutdownNow();
    }

    /**
     * Run a single claimed job to completion. Must be invoked on a virtual
     * thread from the worker pool. Never throws — every failure is captured
     * and routed through the single failure code path.
     */
    public void run(Job job) {
        Objects.requireNonNull(job, "job");
        var ctx = new ExecutionContext(
                job,
                store,
                job.id(),
                nodeId,
                job.attempts(),
                job.ownerHeartbeatAt().orElse(Instant.now()),
                job.log(),
                job.progress(),
                job.metadata(),
                serializer,
                config);
        interceptors.onProcessingStarting(job, ctx);

        // Resolve handler + payload first. A resolution failure is a poison
        // condition: quarantine, fire onProcessingFailed(QUARANTINE), do not retry.
        JobHandler<JobPayload> handler;
        JobPayload payload;
        try {
            @SuppressWarnings("unchecked")
            JobHandler<JobPayload> resolved =
                    (JobHandler<JobPayload>) resolver.resolve(job.spec().handlerType());
            handler = resolved;
            payload = deserializePayload(job);
        } catch (Throwable resolutionFailure) {
            quarantine(job, ctx, resolutionFailure);
            return;
        }

        Thread carrier = Thread.currentThread();
        AtomicBoolean timedOut = new AtomicBoolean(false);
        // Resolve the effective timeout once, before scheduling: the per-job
        // override must also drive the initial delay, and a malformed value
        // must degrade to the global timeout instead of throwing inside the
        // periodic task (which would silently cancel all future checks).
        Duration effectiveTimeout = resolveJobTimeout(job);
        ScheduledFuture<?> watchdog = timeoutExecutor.scheduleAtFixedRate(
                () -> {
                    try {
                        var now = Instant.now();
                        var lastCheckIn = ctx.lastCheckInAt();
                        Instant deadline = lastCheckIn
                                .map(at -> at.plus(config.noProgressTimeout()))
                                .orElse(ctx.claimedAt().plus(effectiveTimeout));
                        if (!deadline.isAfter(now)) {
                            timedOut.set(true);
                            carrier.interrupt();
                        }
                    } catch (Throwable t) {
                        LOG.warn("Timeout watchdog check failed for job {}", job.id(), t);
                    }
                },
                Math.max(
                        1L,
                        Math.min(
                                effectiveTimeout.toMillis(),
                                config.noProgressTimeout().toMillis())),
                Math.max(1L, Math.min(1000L, config.checkInMinInterval().toMillis())),
                TimeUnit.MILLISECONDS);

        try {
            ScopedValue.where(EngineScopedValues.CURRENT, ctx).run(() -> {
                try {
                    handler.run(payload, ctx);
                } catch (RuntimeException re) {
                    throw re;
                } catch (Exception e) {
                    throw new HandlerInvocationException(e);
                }
            });
            watchdog.cancel(false);
            // Clear any straggler interrupt the watchdog may have raised after the handler returned.
            Thread.interrupted();
            if (timedOut.get()) {
                throw new HandlerTimeoutException();
            }
            ctx.flushBestEffort();
            markSucceeded(job, ctx);
        } catch (Throwable t) {
            watchdog.cancel(false);
            Thread.interrupted();
            ctx.flushBestEffort();
            JobInterceptor.FailureCause cause = (t instanceof HandlerTimeoutException || timedOut.get())
                    ? JobInterceptor.FailureCause.TIMEOUT
                    : JobInterceptor.FailureCause.EXCEPTION;
            recordFailure(job, ctx, unwrap(t), cause);
        }
    }

    /** Called by orphan-recovery code in MaintenanceCycle. */
    public void reclaimOrphan(Job job) {
        var ctx = new ExecutionContext(
                job,
                store,
                job.id(),
                nodeId,
                job.attempts(),
                job.ownerHeartbeatAt().orElse(Instant.now()),
                job.log(),
                job.progress(),
                job.metadata(),
                serializer,
                config);
        recordFailure(
                job,
                ctx,
                new IllegalStateException("Job orphaned — owner node's heartbeat expired"),
                JobInterceptor.FailureCause.ORPHAN_RECLAIM);
    }

    // ---------------------------------------------------------------- the single failure path

    private static final int TERMINAL_SAVE_ATTEMPTS = 3;
    private static final long TERMINAL_SAVE_BACKOFF_MS = 50L;

    private void recordFailure(Job job, ExecutionContext ctx, Throwable cause, JobInterceptor.FailureCause kind) {
        try {
            long version = job.version();
            JobState from = job.currentState();
            job.transitionTo(
                    JobState.FAILED, Instant.now(), kindReason(kind), cause == null ? null : cause.getMessage());
            job.clearOwner();
            saveTerminalWithRetry(job, version);
            interceptors.onStateChange(job, from, JobState.FAILED);
            // After the failure transition lands, run interceptor failure hooks
            // (retry, metrics, etc.) — exactly once, regardless of cause.
            interceptors.onProcessingFailed(job, ctx, cause, kind);
        } catch (StaleJobException stale) {
            LOG.debug("Job {} version moved under us during failure path — skipping", job.id());
        } catch (Throwable t) {
            LOG.error("Failure path itself threw for job {}", job.id(), t);
        }
    }

    private void markSucceeded(Job job, ExecutionContext ctx) {
        long version = job.version();
        JobState from = job.currentState();
        // Persist any result the handler recorded via ctx.setResult(...).
        if (ctx.capturedResult() != null) {
            job.setResult(ctx.capturedResult());
        }
        job.transitionTo(JobState.SUCCEEDED, Instant.now(), "engine.success", null);
        job.clearOwner();
        try {
            saveTerminalWithRetry(job, version);
        } catch (StaleJobException stale) {
            LOG.debug("Job {} version moved under us during success path", job.id());
            return;
        } catch (Throwable t) {
            // The in-memory job already carries the SUCCEEDED entry, so the
            // failure transition would be illegal on it. Reload the persisted
            // PROCESSING row and route the reloaded job through the single
            // failure path — otherwise the job stays PROCESSING forever,
            // shielded from orphan reclaim by the node-wide heartbeat.
            LOG.error("SUCCEEDED save failed for job {} — routing through the failure path", job.id(), t);
            Job fresh = reloadForFailure(job);
            if (fresh != null) {
                recordFailure(fresh, ctx, t, JobInterceptor.FailureCause.EXCEPTION);
            }
            return;
        }
        interceptors.onStateChange(job, from, JobState.SUCCEEDED);
        interceptors.onProcessingSucceeded(job, ctx);
    }

    /**
     * Persist a terminal transition, retrying transient store errors with a
     * short bounded backoff. {@link StaleJobException} and
     * {@link OversizedJobException} are not transient and rethrow immediately.
     */
    private void saveTerminalWithRetry(Job job, long expectedVersion) {
        int attempt = 0;
        while (true) {
            try {
                store.saveAtomic(job, expectedVersion);
                return;
            } catch (StaleJobException | OversizedJobException notTransient) {
                throw notTransient;
            } catch (RuntimeException e) {
                attempt++;
                if (attempt >= TERMINAL_SAVE_ATTEMPTS) {
                    throw e;
                }
                try {
                    Thread.sleep(TERMINAL_SAVE_BACKOFF_MS * attempt);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }

    private Job reloadForFailure(Job job) {
        try {
            return store.findById(job.id()).orElse(null);
        } catch (RuntimeException e) {
            LOG.error(
                    "Could not reload job {} after a failed SUCCEEDED save; it stays PROCESSING until reclaim",
                    job.id(),
                    e);
            return null;
        }
    }

    private void quarantine(Job job, ExecutionContext ctx, Throwable cause) {
        try {
            long version = job.version();
            JobState from = job.currentState();
            job.transitionTo(
                    JobState.QUARANTINED,
                    Instant.now(),
                    "engine.quarantine",
                    cause == null ? null : cause.getMessage());
            job.clearOwner();
            store.saveAtomic(job, version);
            interceptors.onStateChange(job, from, JobState.QUARANTINED);
            interceptors.onProcessingFailed(job, ctx, cause, JobInterceptor.FailureCause.QUARANTINE);
        } catch (Throwable t) {
            LOG.error("Quarantine path itself threw for job {}", job.id(), t);
        }
    }

    /**
     * Resolve the per-job timeout override ({@link #META_TIMEOUT_SECONDS}),
     * falling back to the global job timeout when the metadata is absent,
     * malformed, or non-positive. Metadata is user-mutable — a bad value must
     * never disable timeout enforcement.
     */
    private Duration resolveJobTimeout(Job job) {
        var meta = job.metadata().get(META_TIMEOUT_SECONDS);
        if (meta.isEmpty()) {
            return jobTimeout;
        }
        try {
            long seconds = Long.parseLong(meta.get().trim());
            if (seconds < 1) {
                LOG.warn(
                        "Ignoring non-positive {}='{}' for job {} — using the global job timeout",
                        META_TIMEOUT_SECONDS,
                        meta.get(),
                        job.id());
                return jobTimeout;
            }
            return Duration.ofSeconds(seconds);
        } catch (NumberFormatException malformed) {
            LOG.warn(
                    "Ignoring malformed {}='{}' for job {} — using the global job timeout",
                    META_TIMEOUT_SECONDS,
                    meta.get(),
                    job.id());
            return jobTimeout;
        }
    }

    @SuppressWarnings("unchecked")
    private JobPayload deserializePayload(Job job) {
        if (job.spec().arguments().isEmpty()) {
            return new EmptyPayload();
        }
        JobArgument first = serializer.migrateArgument(job.spec().arguments().get(0));
        String resolvedType = serializer.resolveTypeTag(first.typeTag());
        try {
            // Load without initialization: the assignability check must run
            // before any static initializer of a persisted, attacker-influenced
            // class name can execute.
            Class<?> klass = Class.forName(resolvedType, false, JobRunner.class.getClassLoader());
            if (!JobPayload.class.isAssignableFrom(klass)) {
                throw new SerializationException("Argument type is not a JobPayload: " + resolvedType);
            }
            return serializer.deserializePayload(first, (Class<JobPayload>) klass);
        } catch (ClassNotFoundException cnf) {
            throw new SerializationException("Unknown payload type: " + first.typeTag(), cnf);
        }
    }

    private static String kindReason(JobInterceptor.FailureCause kind) {
        return switch (kind) {
            case EXCEPTION -> "engine.exception";
            case TIMEOUT -> "engine.timeout";
            case ORPHAN_RECLAIM -> "engine.orphan-reclaim";
            case QUARANTINE -> "engine.quarantine";
        };
    }

    private static Throwable unwrap(Throwable t) {
        if (t instanceof HandlerInvocationException && t.getCause() != null) return t.getCause();
        if (t instanceof HandlerTimeoutException) return t;
        return t;
    }

    /** Marker payload used when a JobSpec has no arguments. */
    public static final class EmptyPayload implements JobPayload {}

    /** Wrapper for a checked exception thrown by handler code. */
    static final class HandlerInvocationException extends RuntimeException {
        HandlerInvocationException(Throwable cause) {
            super(cause);
        }
    }

    /** Thrown when the watchdog interrupted the handler. */
    static final class HandlerTimeoutException extends TimeoutException {
        HandlerTimeoutException() {
            super("Job exceeded its configured timeout");
        }
    }
}
