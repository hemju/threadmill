package com.hemju.threadmill.core.handler;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobLog;
import com.hemju.threadmill.core.JobMetadata;
import com.hemju.threadmill.core.JobProgress;
import com.hemju.threadmill.core.NodeId;

/**
 * The view of a job exposed to user code during a run.
 *
 * <p>Critically, the context is <strong>not the same mutable structure the
 * engine serializes</strong>. It exposes the user-touchable areas
 * ({@link JobLog}, {@link JobProgress}, {@link JobMetadata}) directly — those
 * types are thread-safe and the engine snapshots them before serialization —
 * and otherwise gives the handler read-only access to identity and timing.
 *
 * <p>Per-execution propagation (job id, attempt number, MDC) is implemented
 * with {@code ScopedValue} (final in Java 25), not {@code ThreadLocal}. A
 * {@code ScopedValue} binding is inherited only by structured-concurrency forks
 * (a {@code StructuredTaskScope} opened inside the handler); it is <strong>not</strong>
 * inherited by virtual threads the handler spawns directly (e.g. via an executor
 * or {@code Thread.ofVirtual().start(...)}). To carry the context across such a
 * boundary, wrap the work with {@code EngineScopedValues.capturing(...)}. Code
 * below the handler that has no {@code ctx} parameter reaches the running
 * context through {@link #current()}.
 *
 * <h2>Deadlines and cancellation</h2>
 *
 * <p>Every attempt runs under a deadline, and when it passes the engine
 * <strong>interrupts the worker thread</strong>. Workers are virtual threads,
 * and on a virtual thread an interrupt can also abort blocking I/O: the JDK
 * guarantees it for {@code java.net.Socket} with the default implementation
 * and for {@code InterruptibleChannel}s, which close the socket and throw
 * {@code SocketException: Closed by interrupt} (or
 * {@code ClosedByInterruptException}); third-party JDBC, HTTP, or Redis
 * clients that use their own transports may translate the interrupt
 * differently or observe it only at their next interruptible call. The
 * interrupt flag stays set afterwards — only methods that throw
 * {@code InterruptedException} clear it — and for a {@code TIMEOUT}
 * cancellation the watchdog re-asserts it every tick until the handler
 * returns, including after a check-in made from cleanup code. A
 * {@code SHUTDOWN} cancellation is delivered once, when the grace period
 * expires: the node is closing and stops its watchdog, so a handler that
 * swallows that interrupt is not interrupted again. A handler must therefore
 * treat an interrupt as
 * cancellation: stop issuing blocking calls, do not blame the external system
 * it was talking to, and return or rethrow promptly. Cleanup that keeps
 * borrowing pooled connections after the interrupt may destroy each one on
 * first use.
 *
 * <p>The engine never has to interrupt a cooperative handler. {@link #deadline()}
 * is the instant the interrupt will arrive if the attempt is still running,
 * computed by the same rule the engine's watchdog applies:
 *
 * <ul>
 *   <li>before the first {@link #checkIn()}: {@link #claimedAt()} plus the
 *       job's effective timeout (the per-job {@code threadmill.job.timeoutSeconds}
 *       override, else the node's {@code jobTimeout});</li>
 *   <li>after a check-in: the most recent check-in plus the node's
 *       {@code noProgressTimeout} — a handler that checks in between steps is
 *       bounded by how long it may go silent, not by total runtime;</li>
 *   <li>while the node is shutting down: no later than the end of the node's
 *       {@code shutdownGracePeriod}, after which the draining worker pool
 *       interrupts whatever is still running.</li>
 * </ul>
 *
 * <p>A handler that performs a loop of expensive steps checks
 * {@link #remaining()} against the cost of the next step and stops early
 * instead of being interrupted mid-step. Two ways out exist and they mean
 * different things: <em>checkpoint and return</em> leaves the job
 * {@code SUCCEEDED}, so the handler must make the remaining work reachable
 * itself (a continuation job, a persisted cursor); <em>throw</em> leaves the
 * job {@code FAILED} and retried under the normal retry policy, which costs
 * an attempt — including when the deadline collapsed because the node is
 * closing. The free immediate requeue applies only to an attempt the engine
 * itself cancelled, that is once {@link #cancellation()} reports
 * {@link CancellationReason#SHUTDOWN} because the interrupt landed; a handler
 * winding down early during a drain should therefore checkpoint and return.
 *
 * <p>{@link #cancellation()} is the fact, not the forecast: it is set the
 * moment the engine decides to abandon the attempt, immediately before the
 * interrupt is sent, and stays set. Cleanup code that runs after an
 * interrupt reads it instead of inspecting the thread's interrupt status or
 * the exception it caught.
 */
public interface JobExecutionContext {

  /**
   * Metadata key carrying the nominal fire time of a recurring instance,
   * stamped by the recurring materializer as an ISO-8601 instant.
   */
  String CRON_FIRE_TIME_META = "threadmill.cron.fireTime";

  /**
   * Metadata key distinguishing what triggered a recurring instance:
   * {@link #CRON_ORIGIN_SCHEDULE} for a regular schedule fire,
   * {@link #CRON_ORIGIN_NUDGE} for an on-demand nudge
   * ({@code Scheduler.nudgeRecurring}), and {@link #CRON_ORIGIN_MANUAL}
   * for the dashboard's operator force-trigger.
   */
  String CRON_ORIGIN_META = "threadmill.cron.origin";

  /** {@link #CRON_ORIGIN_META} value for a regular schedule fire. */
  String CRON_ORIGIN_SCHEDULE = "schedule";

  /** {@link #CRON_ORIGIN_META} value for an on-demand nudge. */
  String CRON_ORIGIN_NUDGE = "nudge";

  /** {@link #CRON_ORIGIN_META} value for the dashboard's operator force-trigger. */
  String CRON_ORIGIN_MANUAL = "manual";

  /**
   * Why the engine abandoned an attempt that was still running. See
   * {@link #cancellation()}.
   */
  enum CancellationReason {
    /**
     * The attempt's deadline passed: the wall-clock timeout before the
     * first check-in, or {@code noProgressTimeout} since the last one.
     */
    TIMEOUT,
    /**
     * The node is shutting down and the {@code shutdownGracePeriod}
     * expired with this attempt still running. Not the job's fault: the
     * engine reschedules it immediately without consuming an attempt.
     */
    SHUTDOWN
  }

  /**
   * The context of the job executing on the current thread, or empty when
   * the caller is not running inside a handler. Resolves through the
   * engine's scoped-value binding, so it is available to any code the
   * handler calls on the same thread and to structured-concurrency forks
   * — but not on threads the handler spawns through a plain executor
   * (see the class documentation).
   */
  static Optional<JobExecutionContext> current() {
    return JobExecutionContexts.CURRENT.isBound()
        ? Optional.of(JobExecutionContexts.CURRENT.get())
        : Optional.empty();
  }

  /** The id of the job being executed. */
  JobId jobId();

  /** The id of the node executing the job. */
  NodeId nodeId();

  /** The attempt number, starting at 1. */
  int attempt();

  /** The instant the engine claimed this job for this attempt. */
  Instant claimedAt();

  /** Append-only per-job log. */
  JobLog log();

  /** Progress reporting for the job. */
  JobProgress progress();

  /** Mutable per-job metadata. */
  JobMetadata metadata();

  /**
   * The instant at which the engine will interrupt this attempt if it has
   * not returned by then. This is the earliest instant the interrupt may
   * arrive; the watchdog checks about once a second, so the actual
   * interrupt lands up to a second later. The value moves forward on every
   * {@link #checkIn()} and collapses to the end of the shutdown grace
   * period once the node begins closing — see the class documentation for
   * the exact rule. Outside the engine (test doubles) there is no
   * deadline and this returns {@link Instant#MAX}.
   */
  default Instant deadline() {
    return Instant.MAX;
  }

  /**
   * Time left until {@link #deadline()}, never negative. A handler that
   * runs a loop of costly steps compares this against the next step's
   * budget before starting it.
   */
  default Duration remaining() {
    Duration left = Duration.between(Instant.now(), deadline());
    return left.isNegative() ? Duration.ZERO : left;
  }

  /**
   * Why the engine has decided to abandon this attempt, or empty while it
   * is still wanted. Set immediately before the engine interrupts the
   * worker thread and never cleared, so cleanup code running after the
   * interrupt can rely on it even if an intermediate layer swallowed the
   * interrupt.
   */
  default Optional<CancellationReason> cancellation() {
    return Optional.empty();
  }

  /** {@code true} once the engine has decided to abandon this attempt. */
  default boolean isCancelled() {
    return cancellation().isPresent();
  }

  /**
   * Record that this long-running job is alive and making progress. After
   * the first check-in the attempt's {@link #deadline()} is no longer
   * {@link #claimedAt()} plus the job timeout but the most recent check-in
   * plus {@code noProgressTimeout}.
   */
  default void checkIn() {}

  /** Record a check-in and append a user-visible log message. */
  default void checkIn(String message) {
    checkIn();
    log(message);
  }

  /** Update the current fraction complete, from {@code 0.0} through {@code 1.0}. */
  default void updateProgress(double fractionComplete) {
    progress().update(fractionComplete);
  }

  /** Append an INFO entry to the per-job log. */
  default void log(String message) {
    log().info(message);
  }

  /**
   * Record a result for this job. The engine persists it together with
   * the {@code SUCCEEDED} state transition. The result is bounded by the
   * same job size cap as the rest of the job body.
   */
  default void setResult(Object value) {
    // default no-op; the engine's ExecutionContext overrides this.
  }

  /** Read the result previously set by this handler, if any. */
  default Optional<Object> readResult() {
    return Optional.empty();
  }

  /**
   * The nominal fire time of this recurring instance — the schedule tick
   * the instance represents, not the wall-clock materialization time.
   * Present only for jobs materialized from a recurring definition. Under
   * the {@code CATCH_UP} missed-run policy every missed interval's instance
   * carries its own distinct fire time, so an idempotent handler can derive
   * a per-interval idempotency key from it.
   */
  default Optional<Instant> cronFireTime() {
    return metadata().get(CRON_FIRE_TIME_META).map(Instant::parse);
  }

  /**
   * What triggered this recurring instance: {@link #CRON_ORIGIN_SCHEDULE},
   * {@link #CRON_ORIGIN_NUDGE}, or {@link #CRON_ORIGIN_MANUAL}. Present only
   * for jobs materialized from a recurring definition.
   */
  default Optional<String> cronOrigin() {
    return metadata().get(CRON_ORIGIN_META);
  }
}
