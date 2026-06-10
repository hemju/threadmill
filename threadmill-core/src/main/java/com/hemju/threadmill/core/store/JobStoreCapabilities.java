package com.hemju.threadmill.core.store;

/**
 * Capability descriptor for a {@link JobStore} implementation.
 *
 * <p>The {@code JobStore} SPI is expressed in operations and guarantees, not
 * in SQL — but storage backends differ in what they can do cheaply (a
 * key-value store has limited search; a relational store does not). The
 * capabilities descriptor lets a backend declare those differences
 * <em>at runtime</em>, so engine logic can adapt without lying to the
 * contract test suite.
 *
 * @param maxSerializedJobBytes  hard upper bound on the serialized size of
 *                               a single job; saves above this are rejected
 *                               cleanly (never corrupt the version)
 * @param maxJobLogBytes         soft upper bound on the serialized
 *                               {@code JobLog} portion of a job. At
 *                               serialization time the oldest log entries
 *                               are trimmed FIFO until the log fits this
 *                               budget. Defaults to {@code maxSerializedJobBytes / 4},
 *                               capped at 64 KiB.
 * @param maxFailureMetadataBytes soft upper bound on the {@code message}
 *                               field of FAILED / QUARANTINED state-history
 *                               entries. Exception messages above this are
 *                               truncated with a sentinel suffix at
 *                               serialization time. Defaults to
 *                               {@code maxSerializedJobBytes / 8}, capped at 32 KiB.
 * @param maxClaimBatch          maximum number of jobs that can be claimed
 *                               in one call (a backend may further reduce
 *                               this internally)
 * @param supportsRichSearch     whether the store can search by arbitrary
 *                               metadata keys; key-value stores typically
 *                               return {@code false}
 * @param supportsExactCounts    whether per-state counts are point-in-time
 *                               exact; {@code false} indicates approximate
 *                               counts (still useful for dashboards)
 * @param supportsConcurrencyGroups whether claim-time per-key concurrency
 *                                  groups are enforced by this store
 * @param ordersByCreationTime   whether iteration / listing is naturally
 *                               ordered by job id (creation time, since ids
 *                               are time-ordered)
 * @param maxMetadataBytes       soft upper bound on the serialized
 *                               {@code JobMetadata} portion of a job. At
 *                               serialization time the largest user entries
 *                               are dropped first ({@code threadmill.}-prefixed
 *                               engine entries are kept longest) until the
 *                               metadata fits this budget; an elision marker
 *                               entry records the omission. Defaults to
 *                               {@code maxSerializedJobBytes / 4}, capped at 64 KiB.
 * @param maxStateHistoryEntries soft upper bound on the number of retained
 *                               state-history entries. At serialization time
 *                               the creation entry and the most recent entries
 *                               are kept and the middle is elided; an elision
 *                               marker metadata entry records the omission.
 */
public record JobStoreCapabilities(
        long maxSerializedJobBytes,
        int maxJobLogBytes,
        int maxFailureMetadataBytes,
        int maxClaimBatch,
        boolean supportsRichSearch,
        boolean supportsExactCounts,
        boolean supportsConcurrencyGroups,
        boolean ordersByCreationTime,
        int maxMetadataBytes,
        int maxStateHistoryEntries) {

    /** A reasonable default of 256 KiB per serialized job. */
    public static final long DEFAULT_MAX_SERIALIZED_BYTES = 256L * 1024L;

    /** Default per-job log budget. */
    public static final int DEFAULT_MAX_JOB_LOG_BYTES = 64 * 1024;

    /** Default failure-metadata budget. */
    public static final int DEFAULT_MAX_FAILURE_METADATA_BYTES = 32 * 1024;

    /** Default metadata budget. */
    public static final int DEFAULT_MAX_METADATA_BYTES = 64 * 1024;

    /** Default state-history entry budget (a retry adds ~3 entries). */
    public static final int DEFAULT_MAX_STATE_HISTORY_ENTRIES = 200;

    /**
     * Compatibility constructor deriving the metadata and state-history
     * budgets from {@code maxSerializedJobBytes}.
     */
    public JobStoreCapabilities(
            long maxSerializedJobBytes,
            int maxJobLogBytes,
            int maxFailureMetadataBytes,
            int maxClaimBatch,
            boolean supportsRichSearch,
            boolean supportsExactCounts,
            boolean supportsConcurrencyGroups,
            boolean ordersByCreationTime) {
        this(
                maxSerializedJobBytes,
                maxJobLogBytes,
                maxFailureMetadataBytes,
                maxClaimBatch,
                supportsRichSearch,
                supportsExactCounts,
                supportsConcurrencyGroups,
                ordersByCreationTime,
                (int) Math.min(maxSerializedJobBytes / 4, DEFAULT_MAX_METADATA_BYTES),
                DEFAULT_MAX_STATE_HISTORY_ENTRIES);
    }

    /** Sensible defaults for an in-memory or fully-featured relational store. */
    public static JobStoreCapabilities defaults() {
        return new JobStoreCapabilities(
                DEFAULT_MAX_SERIALIZED_BYTES,
                DEFAULT_MAX_JOB_LOG_BYTES,
                DEFAULT_MAX_FAILURE_METADATA_BYTES,
                1000,
                true,
                true,
                true,
                true);
    }
}
