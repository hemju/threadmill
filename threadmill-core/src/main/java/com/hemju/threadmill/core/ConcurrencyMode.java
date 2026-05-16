package com.hemju.threadmill.core;

/**
 * Claim-time concurrency mode for jobs that share a concurrency key.
 *
 * <p>{@link #SHARED} jobs for the same key may run together until an earlier
 * {@link #EXCLUSIVE} job is pending or any exclusive job is already
 * processing. {@link #EXCLUSIVE} jobs run alone for their key.
 */
public enum ConcurrencyMode {
    /** Run alone for the job's concurrency key. */
    EXCLUSIVE,

    /** Run alongside adjacent shared jobs for the job's concurrency key. */
    SHARED
}
