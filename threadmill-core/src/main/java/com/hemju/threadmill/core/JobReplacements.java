package com.hemju.threadmill.core;

import java.util.Objects;

/**
 * Internal helper for {@link com.hemju.threadmill.core.store.JobStore}
 * implementations that need to apply a {@link JobReplacement} to a loaded
 * {@link Job}. Centralised so all three stores share one definition of
 * "what does it mean to replace a job's definition?".
 */
public final class JobReplacements {

    private JobReplacements() {}

    /**
     * Build a new {@link Job} from {@code current} with {@code replacement}
     * applied to spec / queue / priority / scheduled-for. State history,
     * metadata, log, progress, attempts, relationship, cron-task name,
     * created-at, owner, result, workflow root, concurrency key, and
     * concurrency mode are preserved.
     *
     * <p>The returned job carries the same persisted-version as {@code current};
     * the caller bumps the version when it persists.
     */
    public static Job apply(Job current, JobReplacement replacement) {
        Objects.requireNonNull(current, "current");
        Objects.requireNonNull(replacement, "replacement");

        JobSnapshot s = current.snapshot();
        Job.Builder b = Job.builder()
                .id(s.id())
                .spec(replacement.specValue().orElse(s.spec()))
                .queue(replacement.queueValue().orElse(s.queue()))
                .priority(replacement.priorityValue().orElse(s.priority()))
                .createdAt(s.createdAt())
                .relationship(s.relationship())
                .version(s.version())
                .attempts(s.attempts())
                .workflowRootId(s.workflowRootId())
                .withStateHistory(s.stateHistory());
        if (s.concurrencyKey() != null) {
            b.concurrencyKey(s.concurrencyKey()).concurrencyMode(s.concurrencyMode());
        }
        if (s.cronTaskName() != null) b.cronTaskName(s.cronTaskName());
        if (replacement.scheduledForValue().isPresent()) {
            b.scheduledFor(replacement.scheduledForValue().get());
        } else if (s.scheduledFor() != null) {
            b.scheduledFor(s.scheduledFor());
        }
        s.metadata().forEach(b::metadata);

        Job out = b.build();
        if (s.ownerNodeId() != null && s.ownerHeartbeatAt() != null) {
            out.assignOwner(s.ownerNodeId(), s.ownerHeartbeatAt());
        }
        if (!s.log().isEmpty()) {
            out.log().replaceAll(s.log());
        }
        if (s.progress() != null) {
            out.progress().replace(s.progress());
        }
        if (s.result() != null) {
            out.setResult(s.result());
        }
        return out;
    }
}
