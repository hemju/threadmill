package com.hemju.threadmill.core.schedule;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;

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
        MissedRunPolicy missedRunPolicy,
        ZoneId zone,
        boolean enabled) {

    public CronTask {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) throw new IllegalArgumentException("name must not be blank");
        Objects.requireNonNull(trigger, "trigger");
        Objects.requireNonNull(handlerType, "handlerType");
        Objects.requireNonNull(payloadArgument, "payloadArgument");
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(missedRunPolicy, "missedRunPolicy");
        Objects.requireNonNull(zone, "zone");
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
     * The contract for runs missed during downtime.
     * <ul>
     *   <li>{@link #DROP} — only the next run from <em>now</em> is enqueued.
     *       Default. This is the right behaviour for monitoring and
     *       housekeeping jobs.</li>
     *   <li>{@link #CATCH_UP} — every missed firing is enqueued as its own
     *       instance. Opt-in; use only for jobs that must run for every
     *       interval (idempotent ledger updates, for example).</li>
     * </ul>
     */
    public enum MissedRunPolicy {
        DROP,
        CATCH_UP
    }
}
