package com.hemju.threadmill.core;

/**
 * Thrown when a job's serialized form exceeds the configured maximum size.
 * Oversized jobs are rejected at creation; a save that would push a job over
 * a store/driver limit is failed cleanly so the in-memory version is never
 * corrupted.
 */
public class OversizedJobException extends RuntimeException {

    private final long actualBytes;
    private final long limitBytes;

    public OversizedJobException(long actualBytes, long limitBytes) {
        super("Job serialized form is " + actualBytes + " bytes, exceeds limit of " + limitBytes);
        this.actualBytes = actualBytes;
        this.limitBytes = limitBytes;
    }

    public long actualBytes() {
        return actualBytes;
    }

    public long limitBytes() {
        return limitBytes;
    }
}
