package com.hemju.threadmill.core.engine;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bounded failure counter for the engine's loops.
 *
 * <p>Increments on transient infrastructure failure; <strong>decays on
 * success</strong> so a long-lived loop does not become a delayed kill
 * switch. A single poison job is quarantined separately and does not
 * count toward this counter — see {@code JobRunner}.
 */
public final class CircuitBreaker {

    private final int threshold;
    private final AtomicInteger consecutive = new AtomicInteger(0);

    public CircuitBreaker(int threshold) {
        if (threshold <= 0) throw new IllegalArgumentException("threshold must be positive");
        this.threshold = threshold;
    }

    public void recordSuccess() {
        consecutive.updateAndGet(v -> Math.max(0, v - 1));
    }

    /** Returns {@code true} if the threshold has been reached after this failure. */
    public boolean recordFailure() {
        return consecutive.incrementAndGet() >= threshold;
    }

    public int current() {
        return consecutive.get();
    }

    public void reset() {
        consecutive.set(0);
    }
}
