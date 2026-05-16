package com.hemju.threadmill.core.store;

import java.time.Duration;

/**
 * Tiny helpers shared by every {@link JobStore} implementation for the
 * mutex SPI.
 *
 * <p>Centralised so all three stores enforce identical preconditions —
 * notably, that {@code leaseDuration} is strictly positive. A zero or
 * negative lease has no defensible semantics: in-memory and PostgreSQL
 * would treat it as immediately expired (acquire succeeds, then any
 * subsequent caller can grab it instantly), while Redis would floor it
 * at 1 ms. Rejecting it up-front avoids divergent backend behaviour and
 * surfaces the bug at the call site.
 */
public final class Mutexes {

    private Mutexes() {}

    /**
     * Throws {@link IllegalArgumentException} unless {@code leaseDuration}
     * is non-null and strictly positive.
     */
    public static void requirePositive(Duration leaseDuration) {
        if (leaseDuration == null) {
            throw new IllegalArgumentException("leaseDuration must not be null");
        }
        if (leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("leaseDuration must be strictly positive, got " + leaseDuration);
        }
    }
}
