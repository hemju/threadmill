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

    int maxRetries() default -1;

    String timeout() default "";

    int priority() default 0;
}
