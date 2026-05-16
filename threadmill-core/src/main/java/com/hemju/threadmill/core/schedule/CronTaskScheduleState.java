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
 * @param taskName        the task this state belongs to
 * @param lastRunAt       when the most recent run was materialised (null if never)
 * @param lastRunJobId    the most recently-materialised job's id, if any
 * @param nextRunAt       when the task should next fire (null if disabled)
 * @param inFlightJobId   if a previous run is still un-terminal, its id; the
 *                        materializer must NOT enqueue another instance while
 *                        this is set
 */
public record CronTaskScheduleState(
        String taskName, Instant lastRunAt, UUID lastRunJobId, Instant nextRunAt, UUID inFlightJobId) {

    public CronTaskScheduleState {
        Objects.requireNonNull(taskName, "taskName");
    }

    public static CronTaskScheduleState initial(String taskName, Instant nextRunAt) {
        return new CronTaskScheduleState(taskName, null, null, nextRunAt, null);
    }

    public Optional<Instant> lastRunAtValue() {
        return Optional.ofNullable(lastRunAt);
    }

    public Optional<UUID> inFlightJobIdValue() {
        return Optional.ofNullable(inFlightJobId);
    }
}
