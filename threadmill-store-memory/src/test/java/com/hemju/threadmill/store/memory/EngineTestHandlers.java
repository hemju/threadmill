package com.hemju.threadmill.store.memory;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;

/** Handler implementations used by the engine tests. */
public final class EngineTestHandlers {

  private EngineTestHandlers() {}

  /** Records every invocation by job id; returns immediately. */
  public static final class CountingHandler implements JobHandler<HelloPayload> {
    public static final ConcurrentHashMap<String, AtomicInteger> COUNT = new ConcurrentHashMap<>();

    @Override
    public void run(HelloPayload payload, JobExecutionContext ctx) {
      COUNT.computeIfAbsent(ctx.jobId().toString(), k -> new AtomicInteger()).incrementAndGet();
    }
  }

  /** Always throws; used to exercise the failure + retry path. */
  public static final class FailingHandler implements JobHandler<HelloPayload> {
    public static final AtomicInteger ATTEMPTS = new AtomicInteger();

    @Override
    public void run(HelloPayload payload, JobExecutionContext ctx) {
      ATTEMPTS.incrementAndGet();
      throw new RuntimeException("boom");
    }
  }

  /** Sleeps longer than the configured job timeout. */
  public static final class HangingHandler implements JobHandler<HelloPayload> {
    public static final AtomicInteger INTERRUPTS = new AtomicInteger();

    @Override
    public void run(HelloPayload payload, JobExecutionContext ctx) throws Exception {
      try {
        Thread.sleep(60_000);
      } catch (InterruptedException e) {
        INTERRUPTS.incrementAndGet();
        throw e;
      }
    }
  }

  /** Blocks until the node is closed; useful for inspecting claimed work. */
  public static final class BlockingHandler implements JobHandler<HelloPayload> {
    public static final AtomicInteger INTERRUPTS = new AtomicInteger();

    @Override
    public void run(HelloPayload payload, JobExecutionContext ctx) throws Exception {
      try {
        Thread.sleep(60_000);
      } catch (InterruptedException e) {
        INTERRUPTS.incrementAndGet();
        throw e;
      }
    }
  }

  /**
   * Blocks until interrupted while publishing, for the whole duration of
   * {@code run}, that user code is executing. The flag is cleared in a
   * {@code finally} so it goes false at the last instant inside {@code run}
   * — anything observed after that point is engine bookkeeping, not the
   * handler.
   */
  public static final class ShutdownLatchHandler implements JobHandler<HelloPayload> {
    public static final AtomicBoolean INSIDE_RUN = new AtomicBoolean(false);

    @Override
    public void run(HelloPayload payload, JobExecutionContext ctx) throws Exception {
      INSIDE_RUN.set(true);
      try {
        Thread.sleep(60_000);
      } finally {
        INSIDE_RUN.set(false);
      }
    }
  }

  /** Runs briefly so shutdown can prove it waits for in-flight work. */
  public static final class SlowHandler implements JobHandler<HelloPayload> {
    public static final AtomicInteger COMPLETIONS = new AtomicInteger();

    @Override
    public void run(HelloPayload payload, JobExecutionContext ctx) throws Exception {
      Thread.sleep(250);
      COMPLETIONS.incrementAndGet();
    }
  }

  /** Checks in regularly so a long job can outlive the wall-clock timeout. */
  public static final class CheckInHandler implements JobHandler<HelloPayload> {
    public static final AtomicInteger COMPLETIONS = new AtomicInteger();

    @Override
    public void run(HelloPayload payload, JobExecutionContext ctx) throws Exception {
      for (int i = 0; i < 5; i++) {
        ctx.checkIn("step " + i);
        ctx.updateProgress((i + 1) / 5.0);
        Thread.sleep(120);
      }
      COMPLETIONS.incrementAndGet();
    }
  }

  /** Throws an exception whose {@code getMessage()} is large enough to blow past
   * a job's serialized-size cap when concatenated into the FAILED state-history
   * entry. Used to pre-cover the audit §4.3 truncation invariant. */
  public static final class BigErrorMessageHandler implements JobHandler<HelloPayload> {
    public static final AtomicInteger ATTEMPTS = new AtomicInteger();
    public static final int MESSAGE_BYTES = 200 * 1024;

    @Override
    public void run(HelloPayload payload, JobExecutionContext ctx) {
      ATTEMPTS.incrementAndGet();
      throw new RuntimeException("big-error: " + "x".repeat(MESSAGE_BYTES));
    }
  }

  /** Checks in once, then hangs so the no-progress timeout can interrupt it. */
  public static final class StalledAfterCheckInHandler implements JobHandler<HelloPayload> {
    public static final AtomicInteger INTERRUPTS = new AtomicInteger();

    @Override
    public void run(HelloPayload payload, JobExecutionContext ctx) throws Exception {
      ctx.checkIn("started");
      try {
        Thread.sleep(60_000);
      } catch (InterruptedException e) {
        INTERRUPTS.incrementAndGet();
        throw e;
      }
    }
  }

  /** Records a result far larger than the per-job metadata budget. */
  public static final class BigResultHandler implements JobHandler<HelloPayload> {
    public static final AtomicInteger COMPLETIONS = new AtomicInteger();
    public static final int RESULT_BYTES = 300 * 1024;

    @Override
    public void run(HelloPayload payload, JobExecutionContext ctx) {
      ctx.setResult("big-result: " + "x".repeat(RESULT_BYTES));
      COMPLETIONS.incrementAndGet();
    }
  }

  /** Records the attempt deadline at start and around a check-in. */
  public static final class DeadlineRecordingHandler implements JobHandler<HelloPayload> {
    public static final AtomicReference<Instant> CLAIMED_AT = new AtomicReference<>();
    public static final AtomicReference<Instant> START_DEADLINE = new AtomicReference<>();
    public static final AtomicReference<Instant> BEFORE_CHECK_IN = new AtomicReference<>();
    public static final AtomicReference<Instant> AFTER_CHECK_IN = new AtomicReference<>();
    public static final AtomicReference<Instant> CHECK_IN_DEADLINE = new AtomicReference<>();

    @Override
    public void run(HelloPayload payload, JobExecutionContext ctx) {
      CLAIMED_AT.set(ctx.claimedAt());
      START_DEADLINE.set(ctx.deadline());
      BEFORE_CHECK_IN.set(Instant.now());
      ctx.checkIn();
      AFTER_CHECK_IN.set(Instant.now());
      CHECK_IN_DEADLINE.set(ctx.deadline());
    }
  }

  /**
   * Takes fixed-cost steps only while the remaining budget covers one more.
   * Left to itself it would overshoot a one-second timeout by a step; a
   * cooperative stop must let it succeed without ever being interrupted.
   */
  public static final class CooperativeStepHandler implements JobHandler<HelloPayload> {
    public static final Duration STEP = Duration.ofMillis(250);
    public static final Duration STEP_BUDGET = Duration.ofMillis(400);
    public static final AtomicInteger STEPS = new AtomicInteger();
    public static final AtomicInteger INTERRUPTS = new AtomicInteger();

    @Override
    public void run(HelloPayload payload, JobExecutionContext ctx) throws Exception {
      try {
        while (ctx.remaining().compareTo(STEP_BUDGET) >= 0) {
          Thread.sleep(STEP.toMillis());
          STEPS.incrementAndGet();
        }
      } catch (InterruptedException e) {
        INTERRUPTS.incrementAndGet();
        throw e;
      }
    }
  }

  /**
   * Blocks until interrupted, records the engine's cancellation reason, and
   * surfaces the interrupt as a plain runtime exception — the shape an aborted
   * socket read produces on a virtual thread.
   */
  public static final class SocketLikeBlockingHandler implements JobHandler<HelloPayload> {
    public static final AtomicReference<Optional<JobExecutionContext.CancellationReason>>
        CANCELLATION = new AtomicReference<>();

    @Override
    public void run(HelloPayload payload, JobExecutionContext ctx) {
      try {
        Thread.sleep(60_000);
      } catch (InterruptedException e) {
        CANCELLATION.set(ctx.cancellation());
        throw new IllegalStateException("Closed by interrupt");
      }
    }
  }

  /**
   * Waits until the deadline collapses below five seconds — the node began
   * closing — then records what it sees and returns cleanly.
   */
  public static final class ShutdownDeadlineObservingHandler implements JobHandler<HelloPayload> {
    public static final AtomicReference<Instant> INITIAL_DEADLINE = new AtomicReference<>();
    public static final AtomicReference<Instant> COLLAPSED_DEADLINE = new AtomicReference<>();
    public static final AtomicBoolean CANCELLED_AT_COLLAPSE = new AtomicBoolean();

    @Override
    public void run(HelloPayload payload, JobExecutionContext ctx) throws Exception {
      INITIAL_DEADLINE.set(ctx.deadline());
      while (ctx.remaining().compareTo(Duration.ofSeconds(5)) > 0) {
        Thread.sleep(10);
      }
      COLLAPSED_DEADLINE.set(ctx.deadline());
      CANCELLED_AT_COLLAPSE.set(ctx.isCancelled());
    }
  }

  /** Resolves the current context from a helper two calls below {@code run}. */
  public static final class CurrentContextHandler implements JobHandler<HelloPayload> {
    public static final AtomicBoolean CURRENT_IS_THIS_CONTEXT = new AtomicBoolean();

    @Override
    public void run(HelloPayload payload, JobExecutionContext ctx) {
      CURRENT_IS_THIS_CONTEXT.set(serviceLayer() == ctx);
    }

    private static JobExecutionContext serviceLayer() {
      return repository();
    }

    private static JobExecutionContext repository() {
      return JobExecutionContext.current().orElseThrow();
    }
  }

  /** Simple payload used by every test handler. */
  public static final class HelloPayload implements JobPayload {
    public String name;

    public HelloPayload() {}

    public HelloPayload(String name) {
      this.name = name;
    }
  }

  public static void reset() {
    CountingHandler.COUNT.clear();
    FailingHandler.ATTEMPTS.set(0);
    HangingHandler.INTERRUPTS.set(0);
    BlockingHandler.INTERRUPTS.set(0);
    SlowHandler.COMPLETIONS.set(0);
    CheckInHandler.COMPLETIONS.set(0);
    StalledAfterCheckInHandler.INTERRUPTS.set(0);
    BigErrorMessageHandler.ATTEMPTS.set(0);
    BigResultHandler.COMPLETIONS.set(0);
    DeadlineRecordingHandler.CLAIMED_AT.set(null);
    DeadlineRecordingHandler.START_DEADLINE.set(null);
    DeadlineRecordingHandler.BEFORE_CHECK_IN.set(null);
    DeadlineRecordingHandler.AFTER_CHECK_IN.set(null);
    DeadlineRecordingHandler.CHECK_IN_DEADLINE.set(null);
    CooperativeStepHandler.STEPS.set(0);
    CooperativeStepHandler.INTERRUPTS.set(0);
    SocketLikeBlockingHandler.CANCELLATION.set(null);
    ShutdownDeadlineObservingHandler.INITIAL_DEADLINE.set(null);
    ShutdownDeadlineObservingHandler.COLLAPSED_DEADLINE.set(null);
    ShutdownDeadlineObservingHandler.CANCELLED_AT_COLLAPSE.set(false);
    CurrentContextHandler.CURRENT_IS_THIS_CONTEXT.set(false);
  }
}
