package com.hemju.threadmill.core;

/**
 * Thrown by a {@link com.hemju.threadmill.core.store.JobStore} when a
 * version-matched update cannot proceed because the persisted version does
 * not match the version the caller expected. The caller is expected to
 * re-read and retry as appropriate; the in-memory job version is unchanged
 * and the in-memory object remains reusable.
 */
public class StaleJobException extends RuntimeException {

    private final JobId jobId;
    private final long expectedVersion;

    public StaleJobException(JobId jobId, long expectedVersion) {
        super("Stale job: expected version " + expectedVersion + " for job " + jobId);
        this.jobId = jobId;
        this.expectedVersion = expectedVersion;
    }

    public JobId jobId() {
        return jobId;
    }

    public long expectedVersion() {
        return expectedVersion;
    }
}
