package com.hemju.threadmill.core.schedule;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;

import com.hemju.threadmill.core.Names;
import com.hemju.threadmill.core.spec.JobArgument;

/**
 * A recurring job definition.
 *
 * <p><strong>Identity vs schedule-state.</strong> A {@code CronTask} is the
 * identity (a stable, user-chosen name and what to run). The schedule
 * state — last-run time, next-run time — lives in
 * {@link CronTaskScheduleState} separately. Re-registering a {@code CronTask}
 * therefore cannot resurrect stale timing or cause a catch-up storm: the
 * caller decides whether to retain or reset the state on upsert.
 *
 * @param name              stable user-chosen name
 * @param trigger           the trigger (cron or fixed-interval)
 * @param handlerType       fully-qualified handler type name
 * @param payloadArgument   pre-serialized payload to feed to each materialised instance
 * @param queue             target queue
 * @param priority          job priority
 * @param timeout           per-instance execution timeout, or {@code null} to
 *                          use the engine's global job timeout; stamped on each
 *                          materialised instance
 * @param maxAttempts       per-instance retry budget, or {@code null} to use
 *                          the {@code RetryInterceptor} defaults; stamped on
 *                          each materialised instance
 * @param exclusive         whether instances of this task run one at a time
 *                          across the whole cluster, enforced at claim time
 *                          under the derived key from
 *                          {@link #concurrencyKeyFor(String)}
 * @param missedRunPolicy   what to do with runs missed during downtime
 * @param zone              time zone for the cron expression (ignored for interval triggers)
 * @param enabled           whether the task is currently active
 */
public record CronTask(
    String name,
    Trigger trigger,
    String handlerType,
    JobArgument payloadArgument,
    String queue,
    int priority,
    Duration timeout,
    Integer maxAttempts,
    boolean exclusive,
    MissedRunPolicy missedRunPolicy,
    ZoneId zone,
    boolean enabled) {

  /** Namespace for the derived per-task concurrency key. */
  private static final String CONCURRENCY_KEY_PREFIX = "recurring:";

  /** {@code Job.concurrencyKey} is capped at 256 UTF-8 bytes. */
  private static final int MAX_CONCURRENCY_KEY_BYTES = 256;

  public CronTask {
    Objects.requireNonNull(name, "name");
    if (name.isBlank()) throw new IllegalArgumentException("name must not be blank");
    Objects.requireNonNull(trigger, "trigger");
    Objects.requireNonNull(handlerType, "handlerType");
    Objects.requireNonNull(payloadArgument, "payloadArgument");
    Objects.requireNonNull(queue, "queue");
    Objects.requireNonNull(missedRunPolicy, "missedRunPolicy");
    Objects.requireNonNull(zone, "zone");
    // The materialised instance carries the timeout as whole seconds
    // (JobRunner.META_TIMEOUT_SECONDS), so sub-second values would
    // silently truncate to "use the global timeout".
    if (timeout != null && timeout.toSeconds() < 1) {
      throw new IllegalArgumentException("timeout must be at least one second when set");
    }
    if (maxAttempts != null && maxAttempts < 1) {
      throw new IllegalArgumentException("maxAttempts must be at least one when set");
    }
  }

  /**
   * Convenience constructor for tasks that run under the engine's global
   * job timeout and the {@code RetryInterceptor} defaults.
   */
  public CronTask(
      String name,
      Trigger trigger,
      String handlerType,
      JobArgument payloadArgument,
      String queue,
      int priority,
      MissedRunPolicy missedRunPolicy,
      ZoneId zone,
      boolean enabled) {
    this(
        name,
        trigger,
        handlerType,
        payloadArgument,
        queue,
        priority,
        null,
        null,
        false,
        missedRunPolicy,
        zone,
        enabled);
  }

  /** Convenience constructor for tasks that do not opt into exclusive execution. */
  public CronTask(
      String name,
      Trigger trigger,
      String handlerType,
      JobArgument payloadArgument,
      String queue,
      int priority,
      Duration timeout,
      Integer maxAttempts,
      MissedRunPolicy missedRunPolicy,
      ZoneId zone,
      boolean enabled) {
    this(
        name,
        trigger,
        handlerType,
        payloadArgument,
        queue,
        priority,
        timeout,
        maxAttempts,
        false,
        missedRunPolicy,
        zone,
        enabled);
  }

  /**
   * The concurrency key every instance of this task claims under when
   * {@link #exclusive()} is set, or {@code null} when it is not.
   */
  public String derivedConcurrencyKey() {
    return exclusive ? concurrencyKeyFor(name) : null;
  }

  /**
   * The derived, namespaced claim-time concurrency key for a recurring task
   * name. The key is derived rather than user-supplied so the feature stays
   * declarative and cannot collide with an application's own keys; the
   * {@code recurring:} namespace is reserved for exactly this.
   *
   * <p>Task names may be up to {@link Names#MAX_LENGTH} characters of
   * arbitrary non-control text, which can exceed the 256-UTF-8-byte
   * concurrency-key cap. Over-long names are truncated on a code-point
   * boundary (never mid-surrogate-pair) and disambiguated with a stable hash
   * suffix, the same shape
   * {@link RecurringMaterializer#taskMutexName(String)} uses.
   */
  public static String concurrencyKeyFor(String taskName) {
    Objects.requireNonNull(taskName, "taskName");
    String raw = CONCURRENCY_KEY_PREFIX + taskName;
    if (raw.getBytes(UTF_8).length <= MAX_CONCURRENCY_KEY_BYTES) {
      return raw;
    }
    String suffix = ":" + Integer.toHexString(taskName.hashCode());
    int budget = MAX_CONCURRENCY_KEY_BYTES - CONCURRENCY_KEY_PREFIX.length() - suffix.length();
    var kept = new StringBuilder(CONCURRENCY_KEY_PREFIX);
    int used = 0;
    for (int i = 0; i < taskName.length(); ) {
      int cp = taskName.codePointAt(i);
      int width = Character.toString(cp).getBytes(UTF_8).length;
      if (used + width > budget) break;
      kept.appendCodePoint(cp);
      used += width;
      i += Character.charCount(cp);
    }
    return kept.append(suffix).toString();
  }

  /** A trigger is either a cron expression or a fixed interval. */
  public sealed interface Trigger permits Trigger.CronExpr, Trigger.Interval {

    Instant nextAfter(Instant after, ZoneId zone);

    record CronExpr(CronExpression expression) implements Trigger {
      public CronExpr {
        Objects.requireNonNull(expression, "expression");
      }

      @Override
      public Instant nextAfter(Instant after, ZoneId zone) {
        return expression.nextAfter(after, zone);
      }
    }

    record Interval(Duration interval) implements Trigger {
      public Interval {
        Objects.requireNonNull(interval, "interval");
        if (interval.isZero() || interval.isNegative()) {
          throw new IllegalArgumentException("interval must be positive");
        }
      }

      @Override
      public Instant nextAfter(Instant after, ZoneId zone) {
        return after.plus(interval);
      }
    }
  }

  /**
   * The contract for runs missed while no materializer observed the task —
   * a downtime window, a paused maintenance master, or a long stall. The
   * policy applies identically to all of them: an overdue next-run survives
   * application restarts (re-registration preserves it while the schedule
   * is unchanged), so restart-missed firings are recovered the same way as
   * stall-missed ones.
   * <ul>
   *   <li>{@link #DROP} — the whole missed backlog collapses into one
   *       instance for the single most recent nominal fire; everything
   *       older is dropped. The instance's
   *       {@code JobExecutionContext.CRON_FIRE_TIME_META} carries that
   *       nominal fire time, and the next run is computed from it, so an
   *       interval trigger's phase never drifts. Default. This is the
   *       right behaviour for monitoring, housekeeping, and latest-state
   *       sweeps.</li>
   *   <li>{@link #CATCH_UP} — every missed firing is enqueued as its own
   *       instance, each carrying its distinct nominal fire time, capped
   *       per materializer tick with carry-over. Opt-in; use only for jobs
   *       that must run for every interval (idempotent ledger updates, for
   *       example).</li>
   * </ul>
   */
  public enum MissedRunPolicy {
    DROP,
    CATCH_UP
  }
}
