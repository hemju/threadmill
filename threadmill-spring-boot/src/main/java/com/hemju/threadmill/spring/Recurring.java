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

    /**
     * Whether instances of this task run one at a time across the whole
     * cluster. Every instance is claimed under a derived
     * {@code recurring:<name>} concurrency key in
     * {@code ConcurrencyMode.EXCLUSIVE}, so a second instance cannot be
     * admitted while one is processing — the declarative replacement for a
     * hand-rolled advisory lock in a singleton sweep.
     *
     * <p>This is enforced at claim time by the store, so it also covers a
     * dashboard manual trigger racing a scheduled instance. It does
     * <strong>not</strong> close the lease-expiry reclaim window: a node that
     * stops heartbeating is indistinguishable from one that is paused and
     * will resume, and reclaim releases the concurrency slot as part of the
     * terminal failure save, so a reclaimed instance can still overlap a
     * still-running original. Handlers remain idempotent by contract.
     */
    boolean exclusive() default false;
}
