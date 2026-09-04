package com.hemju.threadmill.core.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.OversizedJobException;
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.handler.JobExecutionContext.CancellationReason;
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
 *
 * <p><strong>Engine-internal.</strong> This class is {@code public} only for
 * the engine's own cross-package wiring and its test harnesses; it is NOT
 * part of Threadmill's supported public API. Its constructors, methods, and
 * behavior may change in any release without notice — do not reference it
 * from application code. The supported surface is {@code ProcessingNode},
 * {@code Scheduler}, and the SPI interfaces.
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
  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
  // Set by the owning ProcessingNode; true once close() has begun. Lets the
  // failure path distinguish a shutdown interrupt from a handler fault.
  private volatile BooleanSupplier shuttingDown = () -> false;
  // Every attempt currently inside run(); the node marks them all SHUTDOWN
  // right before it interrupts the worker pool.
  private final Set<ExecutionContext> inFlight = ConcurrentHashMap.newKeySet();
  // The instant the owning node will interrupt still-running attempts; null
  // until close() begins. Caps ctx.deadline() for every in-flight attempt.
  private volatile Instant shutdownDeadline;
  // Latched once the node has decided to interrupt the worker pool. Closes the
  // race between the SHUTDOWN sweep over inFlight and a worker registering its
  // context a moment later: that worker still receives shutdownNow()'s
  // interrupt and must classify it as SHUTDOWN, not as a handler fault.
  private volatile boolean forcedShutdown;

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
        r ->
            Thread.ofPlatform().name("threadmill-timeout-watchdog").daemon(true).unstarted(r));
    executor.setRemoveOnCancelPolicy(true);
    this.timeoutExecutor = executor;
  }

  /** Wire the owning node's shutdown signal; claimed before {@link #run} is first called. */
  public void shutdownSignal(BooleanSupplier signal) {
    this.shuttingDown = Objects.requireNonNull(signal, "signal");
  }

  /**
   * Record that the owning node has begun closing and will interrupt any
   * attempt still running at {@code deadline} — the end of its shutdown
   * grace period. From this point every in-flight context's
   * {@code deadline()} is capped at that instant, so a cooperative handler
   * can wind down before the interrupt instead of being cut off mid-step.
   */
  public void beginShutdown(Instant deadline) {
    this.shutdownDeadline = Objects.requireNonNull(deadline, "deadline");
  }

  /** The instant the owning node will interrupt still-running attempts, once it has begun closing. */
  public Optional<Instant> shutdownDeadline() {
    return Optional.ofNullable(shutdownDeadline);
  }

  /**
   * Mark every in-flight attempt as cancelled for shutdown. The owning node
   * calls this immediately before it interrupts the worker pool, so the
   * record is already the fact when the interrupt lands and the failure
   * path classifies the attempt as {@code SHUTDOWN} regardless of which
   * exception the interrupt surfaces as. Forced shutdown is latched before the
   * sweep, so an attempt that registers after the sweep iterated (the set
   * iterator is weakly consistent) marks itself on the way in.
   */
  public void cancelInFlightForShutdown() {
    forcedShutdown = true;
    for (ExecutionContext ctx : inFlight) {
      ctx.markCancelled(CancellationReason.SHUTDOWN);
    }
  }

  /** Stops the timeout watchdog. Intended for engine shutdown. */
  public void shutdown() {
    shutdownRequested.set(true);
    timeoutExecutor.shutdownNow();
  }

  /**
   * Run a single claimed job to completion. Must be invoked on a virtual
   * thread from the worker pool. Never throws — every failure is captured
   * and routed through the single failure code path.
   */
  public void run(Job job) {
    Objects.requireNonNull(job, "job");
    var ctx = newContext(job);
    inFlight.add(ctx);
    // Both sides of the add-versus-sweep race: the sweep marks everything it
    // sees, and anything it could not see yet marks itself here.
    if (forcedShutdown) {
      ctx.markCancelled(CancellationReason.SHUTDOWN);
    }
    try {
      runTracked(job, ctx);
    } finally {
      inFlight.remove(ctx);
    }
  }

  private void runTracked(Job job, ExecutionContext ctx) {
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
    // The deadline rule lives on the context (ExecutionContext.watchdogDeadline)
    // so the watchdog and ctx.deadline() read one formula and can never
    // drift. The effective timeout was resolved once at context creation:
    // the per-job override drives the initial delay too, and a malformed
    // value degrades to the global timeout instead of throwing inside the
    // periodic task (which would silently cancel all future checks).
    ScheduledFuture<?> watchdog = timeoutExecutor.scheduleAtFixedRate(
        () -> {
          try {
            // Once cancelled, keep interrupting every tick until the handler
            // returns: a check-in from cleanup code moves watchdogDeadline()
            // forward, and without the latch repeated check-ins could keep a
            // timed-out attempt alive indefinitely. This is the TIMEOUT
            // guarantee only — a SHUTDOWN cancellation is delivered once,
            // because ProcessingNode.close() stops this executor right after
            // shutdownNow(); the branch merely re-asserts it for the few
            // ticks the executor may still run.
            if (ctx.isCancelled() || !ctx.watchdogDeadline().isAfter(Instant.now())) {
              // Record the reason BEFORE interrupting, so ctx.cancellation()
              // is already the fact when the handler observes the interrupt.
              ctx.markCancelled(CancellationReason.TIMEOUT);
              carrier.interrupt();
            }
          } catch (Throwable t) {
            LOG.warn("Timeout watchdog check failed for job {}", job.id(), t);
          }
        },
        Math.max(
            1L,
            Math.min(
                ctx.effectiveTimeout().toMillis(), config.noProgressTimeout().toMillis())),
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
      if (ctx.cancellation().orElse(null) == CancellationReason.TIMEOUT) {
        throw new HandlerTimeoutException();
      }
      ctx.flushBestEffort();
      markSucceeded(job, ctx);
    } catch (Throwable t) {
      watchdog.cancel(false);
      Thread.interrupted();
      ctx.flushBestEffort();
      Throwable unwrapped = unwrap(t);
      recordFailure(job, ctx, unwrapped, classify(ctx, unwrapped));
    }
  }

  /**
   * Classify a failed attempt from the engine's own cancellation record
   * first and from the exception only as a fallback. A handler interrupted
   * inside socket I/O surfaces {@code SocketException}, not
   * {@code InterruptedException}, so judging by exception type alone would
   * bill a shutdown interrupt as a handler fault and burn a retry attempt.
   */
  private JobInterceptor.FailureCause classify(ExecutionContext ctx, Throwable unwrapped) {
    return switch (ctx.cancellation().orElse(null)) {
      case TIMEOUT -> JobInterceptor.FailureCause.TIMEOUT;
      case SHUTDOWN -> JobInterceptor.FailureCause.SHUTDOWN;
      // No record: an interrupt that reached the handler while the node is
      // closing is still a shutdown, not the job's fault.
      case null ->
        unwrapped instanceof InterruptedException && shuttingDown.getAsBoolean()
            ? JobInterceptor.FailureCause.SHUTDOWN
            : JobInterceptor.FailureCause.EXCEPTION;
    };
  }

  /**
   * Release a claimed job this node will not run (tag mismatch, dispatch
   * failure, shutdown mid-batch). The job is PROCESSING but no handler ever
   * started; PROCESSING has no legal transition back to a pending state, and
   * the concurrency slot taken at claim is only freed by a terminal save. So
   * the release routes through the single failure path with
   * {@link JobInterceptor.FailureCause#SHUTDOWN} semantics: the FAILED save
   * frees the slot, and {@link RetryInterceptor} reschedules the job
   * immediately without consuming the claim-time attempt increment.
   */
  public void releaseWithoutRunning(Job job, String reason) {
    Objects.requireNonNull(job, "job");
    var ctx = newContext(job);
    recordFailure(
        job, ctx, new IllegalStateException(reason), JobInterceptor.FailureCause.SHUTDOWN);
  }

  /** Called by orphan-recovery code in MaintenanceCycle. */
  public void reclaimOrphan(Job job) {
    var ctx = newContext(job);
    recordFailure(
        job,
        ctx,
        new IllegalStateException("Job orphaned — owner node's heartbeat expired"),
        JobInterceptor.FailureCause.ORPHAN_RECLAIM);
  }

  // ---------------------------------------------------------------- the single failure path

  private static final long TERMINAL_SAVE_BACKOFF_MS = 50L;
  private static final long TERMINAL_SAVE_MAX_BACKOFF_MS = 1_000L;

  private void recordFailure(
      Job job, ExecutionContext ctx, Throwable cause, JobInterceptor.FailureCause kind) {
    try {
      long version = job.version();
      JobState from = job.currentState();
      job.transitionTo(
          JobState.FAILED,
          Instant.now(),
          kindReason(kind),
          cause == null ? null : cause.getMessage());
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
   * Persist a terminal transition, retaining responsibility until the write
   * succeeds or node shutdown begins. The worker remains occupied while the
   * store is unavailable, so owner heartbeats protect an attempt that still
   * has an active finalizer rather than an abandoned PROCESSING row.
   * {@link StaleJobException}, {@link OversizedJobException}, and
   * {@link SerializationException} are deterministic and rethrow immediately.
   */
  private void saveTerminalWithRetry(Job job, long expectedVersion) {
    int failures = 0;
    while (true) {
      try {
        store.saveAtomic(job, expectedVersion);
        return;
      } catch (StaleJobException | OversizedJobException | SerializationException notTransient) {
        throw notTransient;
      } catch (RuntimeException e) {
        failures++;
        if (isShuttingDown()) {
          throw e;
        }
        if (failures == 3 || failures % 30 == 0) {
          LOG.warn(
              "Terminal save for job {} has failed {} times; retaining finalization responsibility",
              job.id(),
              failures,
              e);
        }
        long backoff = Math.min(
            TERMINAL_SAVE_MAX_BACKOFF_MS,
            TERMINAL_SAVE_BACKOFF_MS * (1L << Math.min(failures - 1, 5)));
        try {
          Thread.sleep(backoff);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw e;
        }
      }
    }
  }

  private boolean isShuttingDown() {
    return shutdownRequested.get() || shuttingDown.getAsBoolean();
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
      if (seconds > ProcessingNodeConfig.MAX_TIMEOUT.toSeconds()) {
        // A value near Long.MAX_VALUE parses, but Duration.toMillis() and
        // claimedAt.plus(...) overflow before the handler's try block, which
        // would escape the single failure path and strand the claimed row
        // in PROCESSING. Bad metadata must degrade, never disable or crash.
        LOG.warn(
            "Ignoring {}='{}' for job {} — exceeds the maximum timeout of {}; using the global job timeout",
            META_TIMEOUT_SECONDS,
            meta.get(),
            job.id(),
            ProcessingNodeConfig.MAX_TIMEOUT);
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

  /**
   * Build the context for one attempt. The effective timeout is resolved
   * here, once, so the watchdog's initial delay, {@code ctx.deadline()},
   * and the interceptor-visible contexts of releases and orphan reclaims
   * all carry the same value: the attempt's deadline is defined for every
   * context the engine hands out, not only for attempts that run.
   */
  private ExecutionContext newContext(Job job) {
    return new ExecutionContext(
        job,
        store,
        job.id(),
        nodeId,
        job.attempts(),
        job.ownerHeartbeatAt().orElse(Instant.now()),
        resolveJobTimeout(job),
        this::shutdownDeadline,
        job.log(),
        job.progress(),
        job.metadata(),
        serializer,
        config);
  }

  private static String kindReason(JobInterceptor.FailureCause kind) {
    return switch (kind) {
      case EXCEPTION -> "engine.exception";
      case TIMEOUT -> "engine.timeout";
      case ORPHAN_RECLAIM -> "engine.orphan-reclaim";
      case QUARANTINE -> "engine.quarantine";
      case SHUTDOWN -> "engine.shutdown";
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
