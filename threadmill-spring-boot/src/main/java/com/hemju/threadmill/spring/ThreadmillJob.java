package com.hemju.threadmill.spring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a Spring bean as the handler for a Threadmill payload type.
 *
 * <p>The bean must implement {@code JobHandler<P>} for exactly one payload
 * type. Threadmill validates uniqueness at startup so enqueue calls can route
 * by payload class without queue or handler strings.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ThreadmillJob {
    String queue() default "default";

    int maxRetries() default -1;

    String timeout() default "";

    int priority() default 0;
}
