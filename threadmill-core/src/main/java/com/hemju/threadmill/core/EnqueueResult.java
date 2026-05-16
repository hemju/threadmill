package com.hemju.threadmill.core;

/**
 * Result of an atomic producer-side enqueue with a deduplication key.
 *
 * <p>{@link Created} means Threadmill stored a new job. {@link Coalesced}
 * means another producer already created a job for the same logical event
 * inside the deduplication window, and the existing job id is returned for
 * correlation.
 */
public sealed interface EnqueueResult permits EnqueueResult.Created, EnqueueResult.Coalesced {

    /** A new job was created. */
    record Created(JobId id) implements EnqueueResult {}

    /** The enqueue coalesced with an existing job. */
    record Coalesced(JobId existingId) implements EnqueueResult {}
}
