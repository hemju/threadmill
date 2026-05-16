package com.hemju.threadmill.core;

/**
 * Thrown when a caller attempts to move a job into a state the
 * {@link JobStateMachine} does not allow. Illegal transitions are always
 * surfaced — they are never silently coerced.
 */
public class IllegalJobTransitionException extends RuntimeException {

    private final JobState from;
    private final JobState to;

    public IllegalJobTransitionException(JobState from, JobState to) {
        super("Illegal job state transition: " + from + " -> " + to);
        this.from = from;
        this.to = to;
    }

    public JobState from() {
        return from;
    }

    public JobState to() {
        return to;
    }
}
