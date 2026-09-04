package com.hemju.threadmill.spring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a Spring bean as a Threadmill job handler.
 *
 * <p>The bean must implement {@code JobHandler<P>} for exactly one payload
 * type, or {@code JobAction} for handlers that take no per-invocation payload.
 * Threadmill validates uniqueness of (payload type → handler) at startup, so
 * scheduling calls can route by handler class without queue or handler strings.
 *
 * <p>To schedule a handler on a recurring trigger, apply {@link Recurring}
 * alongside this annotation. Recurring is a separate concern and is intentionally
 * not mixed into this annotation's fields — a handler with only {@code @Job} is
 * one-shot; adding {@code @Recurring} makes its recurring nature visible at the
 * top of the file.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Job {

  String queue() default "default";

  /**
   * Total number of execution attempts, including the first one — the same
   * unit as {@code threadmill.retry.maxAttempts} metadata and
   * {@code CronTask.maxAttempts}. {@code 1} means a single attempt with no
   * retries. {@code -1} (the default) leaves the retry budget to the
   * {@code RetryInterceptor}: per-exception-type policies apply, then
   * {@code threadmill.default-max-attempts}. Any other value below 1 fails
   * startup — misconfiguration is never silently replaced by a default.
   *
   * <p>This field was named {@code maxRetries} before v0.1.4, but its value
   * always fed max <em>attempts</em>; the rename keeps every configured
   * number meaning exactly what it meant before.
   */
  int maxAttempts() default -1;

  /**
   * Wall-clock cap for one attempt, as an ISO-8601 duration (for example
   * {@code "PT2M"}); blank falls back to {@code threadmill.jobTimeout}.
   * Stamped on every enqueued job — and on every materialized instance of a
   * {@code @Recurring} handler — as the {@code threadmill.job.timeoutSeconds}
   * metadata override.
   *
   * <p>When the cap passes, the engine <strong>interrupts the worker
   * thread</strong>. Workers are virtual threads, so the interrupt aborts any
   * blocking socket I/O in progress ({@code SocketException: Closed by
   * interrupt}); the interrupt flag stays set afterwards and the engine
   * re-asserts it every second until the handler returns. Treat an interrupt
   * as cancellation: stop issuing blocking calls, do not blame the external
   * system you were talking to, and return or rethrow promptly. Once the
   * handler has called {@code ctx.checkIn()} this cap no longer applies;
   * {@code threadmill.noProgressTimeout} runs from the most recent check-in
   * instead. A cooperative handler reads {@code ctx.remaining()} between
   * steps and stops before the cap, so the engine never has to interrupt it
   * — see {@code JobExecutionContext}.
   */
  String timeout() default "";

  int priority() default 0;
}
