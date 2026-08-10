package com.hemju.threadmill.core.schedule;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.Names;
import com.hemju.threadmill.core.engine.JobRunner;
import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.engine.RetryInterceptor;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStore;

/**
 * Master-only routine that materializes the next instance of every due
 * {@link CronTask}, honoring the configured missed-run policy.
 *
 * <p>The "is a run due?" question is decided by reading
 * {@link CronTaskScheduleState#nextRunAt()}, never by ad-hoc timestamp
 * arithmetic. An overdue value survives restarts (re-registration preserves
 * it while the schedule is unchanged), and the missed-run policy applied
 * here is what bounds the backlog: {@code DROP} collapses it into the single
 * most recent fire, {@code CATCH_UP} materializes every fire capped per tick
 * with carry-over. That cap — not re-registration — is the catch-up-storm
 * defense.
 *
 * <p>If a previously-materialised instance is still un-terminal, no new
 * instance is created until that one finishes. This guard prevents
 * pile-up under long-running recurring work.
 */
public final class RecurringMaterializer {

    private static final Logger LOG = LoggerFactory.getLogger(RecurringMaterializer.class);

    /** Per-tick cap on CATCH_UP materializations; the rest carries over. */
    private static final int MAX_CATCH_UP_PER_TICK = 100;

    /** Lease for the per-task schedule-state mutex. */
    private static final Duration TASK_MUTEX_LEASE = Duration.ofSeconds(30);

    private final String mutexHolder = UUID.randomUUID().toString();

    /**
     * The store mutex guarding a recurring task's {@link CronTaskScheduleState}
     * read-modify-write. Shared between the materializer's tick and
     * {@code Scheduler.upsertCron} so a re-registration cannot clobber a
     * concurrently-set {@code inFlightJobId}. Long task names are truncated
     * with a stable hash suffix to fit the store's name limit.
     */
    public static String taskMutexName(String taskName) {
        String raw = "cron:" + taskName;
        if (raw.length() <= Names.MAX_LENGTH) {
            return raw;
        }
        String hash = Integer.toHexString(taskName.hashCode());
        return raw.substring(0, Names.MAX_LENGTH - hash.length() - 1) + ":" + hash;
    }

    private final JobStore store;
    private final LocalWakeBus wakeBus;

    public RecurringMaterializer(JobStore store) {
        this(store, new LocalWakeBus());
    }

    public RecurringMaterializer(JobStore store, LocalWakeBus wakeBus) {
        this.store = Objects.requireNonNull(store, "store");
        this.wakeBus = Objects.requireNonNull(wakeBus, "wakeBus");
    }

    /** Examine every cron task; for those due, materialize new instances per policy. */
    public void tick(Instant now) {
        List<CronTask> tasks = store.listCronTasks();
        for (CronTask task : tasks) {
            if (!task.enabled()) continue;
            try {
                tickOne(task, now);
            } catch (Throwable t) {
                LOG.warn("Recurring tick failed for task {}", task.name(), t);
            }
        }
    }

    private void tickOne(CronTask task, Instant now) {
        // Guard the schedule-state read-modify-write with the per-task store
        // mutex shared with Scheduler.upsertCron. If another holder has the
        // task (a re-registration mid-rolling-deploy), skip this tick — the
        // next maintenance tick revisits.
        if (!store.tryAcquireMutex(taskMutexName(task.name()), mutexHolder, TASK_MUTEX_LEASE)) {
            return;
        }
        try {
            tickOneLocked(task, now);
        } finally {
            try {
                store.releaseMutex(taskMutexName(task.name()), mutexHolder);
            } catch (RuntimeException ignored) {
                // the lease expires on its own
            }
        }
    }

    private void tickOneLocked(CronTask task, Instant now) {
        var stateOpt = store.findCronTaskState(task.name());
        if (stateOpt.isEmpty()) return; // not yet initialised
        var state = stateOpt.get();
        Instant nudge = state.nudgeRequestedAt();
        boolean due = state.nextRunAt() != null && !state.nextRunAt().isAfter(now);
        if (!due && nudge == null) return;

        // Pile-up guard: a non-terminal in-flight instance blocks the next
        // materialization. FAILED is deliberately treated as non-blocking
        // even though it is only terminal-pending: a retry-exhausted FAILED
        // instance must never deadlock the task forever. The cost is a
        // narrow window — FAILED observed between the failure save and
        // RetryInterceptor's SCHEDULED save does not block, so a retrying
        // instance can briefly overlap a fresh one; handlers are required
        // to be idempotent anyway (at-least-once).
        if (state.inFlightJobId() != null) {
            Job inFlight = store.findById(JobId.of(state.inFlightJobId())).orElse(null);
            if (inFlight != null
                    && !inFlight.currentState().isTerminal()
                    && inFlight.currentState() != JobState.FAILED) {
                // Still running — leave the next_run_at where it is so we revisit on the next tick.
                return;
            }
        }

        if (!due) {
            // Nudge-only materialization: one instance through the normal
            // machinery, with the schedule left untouched — nextRunAt stays
            // exactly where it was, so a cron task's next fire remains the
            // regular wall-clock match and an interval trigger's phase is
            // preserved. The instance carries no nominal fire time (it
            // represents no schedule tick), only the nudge origin marker.
            JobId id = materializeNudge(task);
            store.upsertCronTaskState(new CronTaskScheduleState(
                    task.name(), now, id.asUuid(), state.nextRunAt(), id.asUuid(), state.timingFingerprint()));
            // Clear AFTER materializing (a crash between the two costs one
            // extra run, never a lost one), and only the observed value — a
            // nudge accepted since our read survives for a follow-up run.
            store.clearCronNudge(task.name(), nudge);
            return;
        }

        String fingerprint = CronTaskScheduleState.timingFingerprintOf(task);
        if (task.missedRunPolicy() == CronTask.MissedRunPolicy.CATCH_UP) {
            // Materialize every fire from nextRunAt up to and including now,
            // capped per tick so an unbounded backlog cannot occupy the
            // maintenance thread for an unbounded stretch; the remainder
            // carries over via nextRunAt and continues on later ticks.
            Instant fire = state.nextRunAt();
            JobId last = null;
            int materialized = 0;
            while (!fire.isAfter(now) && materialized < MAX_CATCH_UP_PER_TICK) {
                last = materialize(task, fire);
                fire = task.trigger().nextAfter(fire, task.zone());
                materialized++;
            }
            if (materialized == MAX_CATCH_UP_PER_TICK && !fire.isAfter(now)) {
                LOG.debug("CATCH_UP for task {} hit the per-tick cap; continuing on the next tick", task.name());
            }
            store.upsertCronTaskState(new CronTaskScheduleState(
                    task.name(),
                    now,
                    last == null ? null : last.asUuid(),
                    fire,
                    last == null ? null : last.asUuid(),
                    fingerprint));
        } else {
            // DROP: collapse everything missed into the single most recent
            // NOMINAL fire. Materializing at the nominal time (not `now`)
            // keeps CRON_FIRE_TIME_META meaningful as "which firing this
            // instance represents", and advancing nextRunAt from the nominal
            // fire keeps an interval trigger's phase fixed — an every-6h task
            // that was due at 06:00 and recovered at 07:00 still fires next
            // at 12:00, not 13:00.
            Instant fire = latestFireAtOrBefore(task, state.nextRunAt(), now);
            JobId id = materialize(task, fire);
            Instant next = task.trigger().nextAfter(fire, task.zone());
            store.upsertCronTaskState(
                    new CronTaskScheduleState(task.name(), now, id.asUuid(), next, id.asUuid(), fingerprint));
        }
        if (nudge != null) {
            // The scheduled instance(s) just materialized were enqueued after
            // the observed nudge committed, so their execution reads whatever
            // the nudger wrote — the nudge coalesces into them instead of
            // producing an extra run. Compare-and-clear: a newer nudge survives.
            store.clearCronNudge(task.name(), nudge);
        }
    }

    /**
     * The most recent nominal fire time at or before {@code now}, starting
     * from the (overdue) {@code overdueFire}. Intervals are computed
     * arithmetically so a tiny interval with a huge backlog cannot spin the
     * maintenance thread; cron triggers step fire-by-fire, which is bounded
     * by one iteration per missed firing (cron granularity is one minute).
     */
    private static Instant latestFireAtOrBefore(CronTask task, Instant overdueFire, Instant now) {
        return switch (task.trigger()) {
            case CronTask.Trigger.Interval interval -> {
                long missed = Duration.between(overdueFire, now).dividedBy(interval.interval());
                yield overdueFire.plus(interval.interval().multipliedBy(missed));
            }
            case CronTask.Trigger.CronExpr cron -> {
                Instant fire = overdueFire;
                Instant next = task.trigger().nextAfter(fire, task.zone());
                while (!next.isAfter(now)) {
                    fire = next;
                    next = task.trigger().nextAfter(fire, task.zone());
                }
                yield fire;
            }
        };
    }

    private JobId materialize(CronTask task, Instant when) {
        return materialize(task, when, JobExecutionContext.CRON_ORIGIN_SCHEDULE);
    }

    /**
     * A nudge-materialized instance: same machinery as a schedule fire, but
     * with no nominal fire time (a nudge represents no schedule tick) and the
     * {@code nudge} origin marker for dashboards and metrics.
     */
    private JobId materializeNudge(CronTask task) {
        return materialize(task, null, JobExecutionContext.CRON_ORIGIN_NUDGE);
    }

    private JobId materialize(CronTask task, Instant nominalFire, String origin) {
        var builder = Job.builder()
                .spec(new JobSpec(task.handlerType(), List.of(task.payloadArgument())))
                .queue(task.queue())
                .priority(task.priority())
                .cronTaskName(task.name())
                .metadata(JobExecutionContext.CRON_ORIGIN_META, origin)
                .initialState(JobState.ENQUEUED);
        if (nominalFire != null) {
            builder.metadata(JobExecutionContext.CRON_FIRE_TIME_META, nominalFire.toString());
        }
        if (task.timeout() != null) {
            builder.metadata(
                    JobRunner.META_TIMEOUT_SECONDS, Long.toString(task.timeout().toSeconds()));
        }
        if (task.maxAttempts() != null) {
            builder.metadata(RetryInterceptor.META_MAX_ATTEMPTS, Integer.toString(task.maxAttempts()));
        }
        Job job = builder.build();
        store.insert(job);
        wakeBus.wake(task.queue());
        return job.id();
    }
}
