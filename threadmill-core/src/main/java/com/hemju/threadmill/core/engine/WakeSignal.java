package com.hemju.threadmill.core.engine;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Single-permit wake-up signal used by the {@link Dispatcher} to break out of
 * its poll-interval sleep when a worker becomes idle.
 *
 * <p>The permit cap is the load-bearing invariant: many idle transitions
 * while the dispatcher is mid-claim collapse into one wake-up, so the loop
 * never busy-spins. The cap also means {@link #signal()} is cheap and safe
 * to call from any number of threads on every job completion path.
 *
 * <p>The dispatcher's poll loop replaces {@code Thread.sleep(pollInterval)}
 * with {@link #awaitFor(Duration)}: it sleeps up to the configured interval,
 * waking immediately when a signal is available.
 */
final class WakeSignal {

    private final Semaphore permits = new Semaphore(0);

    /**
     * Make at most one pending wake-up available. If a permit is already
     * pending, this call is a no-op — many signals between awaits collapse
     * to one wake-up by construction.
     */
    void signal() {
        if (permits.availablePermits() == 0) {
            permits.release();
        }
    }

    /**
     * Sleep up to {@code timeout}, returning early if a signal arrives.
     * Returns {@code true} if a signal was consumed, {@code false} if the
     * timeout expired without a signal.
     */
    boolean awaitFor(Duration timeout) throws InterruptedException {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            return permits.tryAcquire();
        }
        return permits.tryAcquire(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }
}
