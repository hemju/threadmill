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
 *                          and {@link RecurringMaterializer} decide
 *                          preserve-vs-recompute from this record alone —
 *                          a crash between the separate task and state writes can
 *                          never pair a new trigger with old timing undetectably
 * @param nudgeRequestedAt  when an on-demand materialization ("nudge") was most
 *                          recently requested and not yet satisfied, or
 *                          {@code null}. <strong>Read-only through this
 *                          record:</strong> {@code JobStore.upsertCronTaskState}
 *                          deliberately never writes this field — a blanket state
 *                          upsert (re-registration, materializer bookkeeping,
 *                          dashboard edit) must not clobber a concurrently
 *                          accepted nudge. The only writers are
 *                          {@code JobStore.requestCronNudge} and the
 *                          compare-and-clear {@code JobStore.clearCronNudge}
 * @param nudgeRevision     store-generated revision of the most recent nudge
 *                          acceptance: strictly monotonic per task, incremented
 *                          on every accept and never reset — including by a
 *                          clear — so no two acceptances share a value for as
 *                          long as the task's schedule state exists (deleting
 *                          the task drops the state row, so a same-named task
 *                          registered later starts over at one).
 *                          This, not the wall-clock {@code nudgeRequestedAt},
 *                          is the compare-and-clear identity: timestamps can
 *                          collide within store precision, and a collision
 *                          would let a clear erase a newer nudge accepted
 *                          after materialization. Read-only like the
 *                          timestamp; {@code null} when no nudge was ever
 *                          accepted
 */
public record CronTaskScheduleState(
    String taskName,
    Instant lastRunAt,
    UUID lastRunJobId,
    Instant nextRunAt,
    UUID inFlightJobId,
    String timingFingerprint,
    Instant nudgeRequestedAt,
    Long nudgeRevision) {

  public CronTaskScheduleState {
    Objects.requireNonNull(taskName, "taskName");
  }

  /**
   * Convenience constructor without the read-only nudge components — the
   * natural shape for every writer, since {@code upsertCronTaskState} never
   * persists those fields anyway.
   */
  public CronTaskScheduleState(
      String taskName,
      Instant lastRunAt,
      UUID lastRunJobId,
      Instant nextRunAt,
      UUID inFlightJobId,
      String timingFingerprint) {
    this(
        taskName, lastRunAt, lastRunJobId, nextRunAt, inFlightJobId, timingFingerprint, null, null);
  }

  /**
   * Convenience constructor with no timing fingerprint. A materializer tick
   * adopts the current task's fingerprint without moving a non-null
   * {@code nextRunAt}; if timing is also null, it initializes the next fire
   * from that tick. Production writers that compute {@code nextRunAt} from a
   * task should stamp
   * {@link #timingFingerprintOf(CronTask)} so unchanged re-registrations
   * can preserve overdue state.
   */
  public CronTaskScheduleState(
      String taskName,
      Instant lastRunAt,
      UUID lastRunJobId,
      Instant nextRunAt,
      UUID inFlightJobId) {
    this(taskName, lastRunAt, lastRunJobId, nextRunAt, inFlightJobId, null, null, null);
  }

  public static CronTaskScheduleState initial(
      String taskName, Instant nextRunAt, String timingFingerprint) {
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
