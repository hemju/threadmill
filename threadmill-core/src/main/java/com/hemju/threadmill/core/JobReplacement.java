package com.hemju.threadmill.core;

import java.time.Instant;
import java.util.Optional;

import com.hemju.threadmill.core.spec.JobSpec;

/**
 * Atomic in-place update to an existing, non-running job.
 *
 * <p>Replace the {@link JobSpec} (typically: a different handler or
 * different arguments), or rebroker it to a different queue/priority, or
 * change its scheduled-for time. Replacement is rejected unless the job
 * is currently in a non-running, non-terminal state (one of
 * {@link JobState#ENQUEUED}, {@link JobState#SCHEDULED},
 * {@link JobState#AWAITING}) — replacing a {@code PROCESSING} job under a
 * worker would be unsafe.
 *
 * <p>Every field is optional: only the non-null parts are applied. The
 * id, state, version, history, metadata, log, progress, and relationships
 * are preserved. The version bumps by one on success.
 *
 * <p>Use {@link Builder} to construct.
 *
 * @param newSpec          replacement {@link JobSpec}, or {@code null} to keep the existing one
 * @param newQueue         new queue name, or {@code null} to keep
 * @param newPriority      new priority, or {@code null} to keep
 * @param newScheduledFor  new {@code scheduled_at}, or {@code null} to keep
 */
public record JobReplacement(JobSpec newSpec, String newQueue, Integer newPriority, Instant newScheduledFor) {

    public static Builder builder() {
        return new Builder();
    }

    /** Convenience: replace just the spec. */
    public static JobReplacement ofSpec(JobSpec newSpec) {
        return new JobReplacement(newSpec, null, null, null);
    }

    public Optional<JobSpec> specValue() {
        return Optional.ofNullable(newSpec);
    }

    public Optional<String> queueValue() {
        return Optional.ofNullable(newQueue);
    }

    public Optional<Integer> priorityValue() {
        return Optional.ofNullable(newPriority);
    }

    public Optional<Instant> scheduledForValue() {
        return Optional.ofNullable(newScheduledFor);
    }

    /** Builder for a {@link JobReplacement}. */
    public static final class Builder {
        private JobSpec spec;
        private String queue;
        private Integer priority;
        private Instant scheduledFor;

        private Builder() {}

        public Builder spec(JobSpec spec) {
            this.spec = spec;
            return this;
        }

        public Builder queue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        public Builder scheduledFor(Instant at) {
            this.scheduledFor = at;
            return this;
        }

        public JobReplacement build() {
            return new JobReplacement(spec, queue, priority, scheduledFor);
        }
    }
}
