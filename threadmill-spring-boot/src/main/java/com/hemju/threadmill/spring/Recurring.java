package com.hemju.threadmill.spring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.hemju.threadmill.core.schedule.CronTask;

/**
 * Schedules a {@link Job}-annotated handler to fire on a recurring trigger.
 * Apply alongside {@code @Job} on a handler that takes no per-invocation
 * payload — i.e. one implementing {@code JobAction} (or
 * {@code JobHandler<NoPayload>} directly). The annotation cannot carry a
 * runtime payload value, so handlers with a non-trivial payload type must
 * schedule themselves imperatively via {@code Scheduler.defineRecurring}.
 *
 * <p>Any number of {@code @Recurring} handlers can coexist in the same
 * application. The registry keys handlers by their implementing class, so
 * multiple {@code JobAction} beans — all of which declare {@code NoPayload}
 * as their payload type — register independently without collision.
 *
 * <p>Exactly one of {@link #interval()} or {@link #cron()} must be set;
 * setting both is rejected at startup. The recurring task's durable identity
 * defaults to the handler's fully-qualified class name — override via
 * {@link #recurringName()} to lock the identity across renames.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Recurring {

    /**
     * Recurring interval as an ISO-8601 duration string (for example {@code "PT10S"}).
     * Mutually exclusive with {@link #cron()}.
     */
    String interval() default "";

    /**
     * Recurring schedule as a five-field cron expression. Mutually exclusive with
     * {@link #interval()}.
     */
    String cron() default "";

    /**
     * Policy for runs missed while no node was available to materialize them.
     */
    CronTask.MissedRunPolicy missedRunPolicy() default CronTask.MissedRunPolicy.DROP;

    /**
     * Durable identity for the recurring task. Defaults to the handler's
     * fully-qualified class name when blank. Set explicitly to keep the same
     * identity across handler renames or package moves.
     */
    String recurringName() default "";
}
