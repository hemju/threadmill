package com.hemju.threadmill.core.engine;

import java.time.Duration;
import java.util.Objects;

/**
 * A retry policy: how many attempts and what backoff to apply.
 *
 * <p>Precedence:
 * <pre>
 *   per-job override  &gt;  per-exception-type policy  &gt;  global default
 * </pre>
 *
 * @param maxAttempts    total attempts including the first one (1 = no retry)
 * @param initialBackoff backoff before the first retry; doubles per attempt up to one hour
 */
public record RetryPolicy(int maxAttempts, Duration initialBackoff) {

    public RetryPolicy {
        if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be >= 1");
        Objects.requireNonNull(initialBackoff, "initialBackoff");
    }

    public static RetryPolicy of(int maxAttempts, Duration initialBackoff) {
        return new RetryPolicy(maxAttempts, initialBackoff);
    }

    public static RetryPolicy noRetry() {
        return new RetryPolicy(1, Duration.ZERO);
    }
}
