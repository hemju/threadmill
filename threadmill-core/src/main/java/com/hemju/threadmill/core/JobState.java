package com.hemju.threadmill.core;

/**
 * The full lifecycle states of a job.
 *
 * <p>Threadmill carries every state from day one — including {@link #QUARANTINED}
 * for jobs that cannot be deserialized or instantiated, and {@link #PROCESSED}
 * which is reserved for the later external-jobs feature. The transition table
 * lives in {@link JobStateMachine}; new transitions must be added there and
 * exhaustively tested.
 */
public enum JobState {

    /** Created but waiting for a precondition (a workflow parent, an external trigger). */
    AWAITING,

    /** Scheduled to run at a future point in time. */
    SCHEDULED,

    /** Ready for a {@code Dispatcher} to claim and execute. */
    ENQUEUED,

    /** Currently executing on a node; the node is heart-beating ownership. */
    PROCESSING,

    /** Reserved for the external-jobs feature: the method returned but awaits an external signal. */
    PROCESSED,

    /** Terminal — execution completed successfully. */
    SUCCEEDED,

    /**
     * Terminal-pending: execution failed. A failed job may be re-scheduled, re-enqueued,
     * or deleted by the engine; in steady state failed jobs sit here until retention.
     */
    FAILED,

    /**
     * Soft-deleted by the engine or operator. Retention eventually hard-deletes the row.
     */
    DELETED,

    /**
     * Unrecoverable model-level failure: the payload cannot be deserialized, the handler
     * type cannot be resolved, or the job is otherwise structurally broken. Quarantined
     * jobs do not block other work and are not retried automatically.
     */
    QUARANTINED;

    /** Whether the engine considers the job terminal (no automatic transitions follow). */
    public boolean isTerminal() {
        return this == SUCCEEDED || this == DELETED || this == QUARANTINED;
    }
}
