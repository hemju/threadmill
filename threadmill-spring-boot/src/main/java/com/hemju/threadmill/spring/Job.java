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

    String timeout() default "";

    int priority() default 0;
}
