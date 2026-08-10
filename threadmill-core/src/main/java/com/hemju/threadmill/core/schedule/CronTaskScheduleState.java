package com.hemju.threadmill.core.schedule;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * The bookkeeping state of a {@link CronTask}.
 *
 * <p>Deliberately separate from {@link CronTask}: re-registering the task
 * does not touch this. The store keeps these together by name but the
 * caller (or {@code MaintenanceCycle}) decides whether to preserve or
 * reset the schedule-state when a task is upserted.
 *
 * @param taskName          the task this state belongs to
 * @param lastRunAt         when the most recent run was materialised (null if never)
 * @param lastRunJobId      the most recently-materialised job's id, if any
 * @param nextRunAt         when the task should next fire (null if disabled)
 * @param inFlightJobId     if a previous run is still un-terminal, its id; the
 *                          materializer must NOT enqueue another instance while
 *                          this is set
 * @param timingFingerprint the {@link #timingFingerprintOf(CronTask)} of the
 *                          task definition {@code nextRunAt} was computed from,
 *                          or {@code null} for rows written before the
 *                          fingerprint existed. Written atomically with
 *                          {@code nextRunAt}, it lets {@code Scheduler.upsertCron}
 *                          decide preserve-vs-recompute from this record alone —
 *                          a crash between the separate task and state writes can
 *                          never pair a new trigger with old timing undetectably
 */
public record CronTaskScheduleState(
        String taskName,
        Instant lastRunAt,
        UUID lastRunJobId,
        Instant nextRunAt,
        UUID inFlightJobId,
        String timingFingerprint) {

    public CronTaskScheduleState {
        Objects.requireNonNull(taskName, "taskName");
    }

    /**
     * Convenience constructor with no timing fingerprint. A null fingerprint
     * is always safe — the next re-registration recomputes the schedule
     * instead of preserving it — but production writers that recompute
     * {@code nextRunAt} from a task should stamp
     * {@link #timingFingerprintOf(CronTask)} so unchanged re-registrations
     * can preserve overdue state.
     */
    public CronTaskScheduleState(
            String taskName, Instant lastRunAt, UUID lastRunJobId, Instant nextRunAt, UUID inFlightJobId) {
        this(taskName, lastRunAt, lastRunJobId, nextRunAt, inFlightJobId, null);
    }

    public static CronTaskScheduleState initial(String taskName, Instant nextRunAt, String timingFingerprint) {
        return new CronTaskScheduleState(taskName, null, null, nextRunAt, null, timingFingerprint);
    }

    /** {@link #initial(String, Instant, String)} without a timing fingerprint. */
    public static CronTaskScheduleState initial(String taskName, Instant nextRunAt) {
        return initial(taskName, nextRunAt, null);
    }

    /**
     * Canonical fingerprint of the parts of a task definition that determine
     * its firing times: the trigger, plus the zone for cron triggers only —
     * a {@link CronTask.Trigger.Interval} deliberately ignores its zone, so
     * two nodes with different system-default zones must not treat the same
     * interval as a schedule edit.
     */
    public static String timingFingerprintOf(CronTask task) {
        Objects.requireNonNull(task, "task");
        return switch (task.trigger()) {
            case CronTask.Trigger.Interval interval -> "interval:" + interval.interval();
            case CronTask.Trigger.CronExpr cron ->
                "cron:" + cron.expression().expression() + "@" + task.zone().getId();
        };
    }

    public Optional<Instant> lastRunAtValue() {
        return Optional.ofNullable(lastRunAt);
    }

    public Optional<UUID> inFlightJobIdValue() {
        return Optional.ofNullable(inFlightJobId);
    }
}
