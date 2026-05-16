package com.hemju.threadmill.core;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * One entry in a job's append-only state history.
 *
 * @param state     the state entered
 * @param at        when the transition happened
 * @param reason    short, machine-readable reason code (for example {@code "user.delete"},
 *                  {@code "engine.orphan-recovery"}); may be {@code null}
 * @param message   free-form human-readable message; may be {@code null}
 */
public record JobStateEntry(JobState state, Instant at, String reason, String message) {

    public JobStateEntry {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(at, "at");
    }

    public static JobStateEntry of(JobState state, Instant at) {
        return new JobStateEntry(state, at, null, null);
    }

    public static JobStateEntry of(JobState state, Instant at, String reason) {
        return new JobStateEntry(state, at, reason, null);
    }

    public Optional<String> reasonValue() {
        return Optional.ofNullable(reason);
    }

    public Optional<String> messageValue() {
        return Optional.ofNullable(message);
    }
}
