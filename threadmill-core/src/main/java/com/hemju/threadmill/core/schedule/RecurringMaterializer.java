package com.hemju.threadmill.core.schedule;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStore;

/**
 * Master-only routine that materializes the next instance of every due
 * {@link CronTask}, honoring the configured missed-run policy.
 *
 * <p>The "is a run due?" question is decided by reading
 * {@link CronTaskScheduleState#nextRunAt()}, never by ad-hoc timestamp
 * arithmetic. Re-registering the task can change that field — that's how
 * the catch-up-storm hazard is averted.
 *
 * <p>If a previously-materialised instance is still un-terminal, no new
 * instance is created until that one finishes. This guard prevents
 * pile-up under long-running recurring work.
 */
public final class RecurringMaterializer {

    private static final Logger LOG = LoggerFactory.getLogger(RecurringMaterializer.class);

    /** Per-tick cap on CATCH_UP materializations; the rest carries over. */
    private static final int MAX_CATCH_UP_PER_TICK = 100;

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
        var stateOpt = store.findCronTaskState(task.name());
        if (stateOpt.isEmpty()) return; // not yet initialised
        var state = stateOpt.get();
        if (state.nextRunAt() == null || state.nextRunAt().isAfter(now)) return;

        // Pile-up guard.
        if (state.inFlightJobId() != null) {
            Job inFlight = store.findById(JobId.of(state.inFlightJobId())).orElse(null);
            if (inFlight != null
                    && !inFlight.currentState().isTerminal()
                    && inFlight.currentState() != JobState.SUCCEEDED
                    && inFlight.currentState() != JobState.FAILED
                    && inFlight.currentState() != JobState.DELETED
                    && inFlight.currentState() != JobState.QUARANTINED) {
                // Still running — leave the next_run_at where it is so we revisit on the next tick.
                return;
            }
        }

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
                    task.name(), now, last == null ? null : last.asUuid(), fire, last == null ? null : last.asUuid()));
        } else {
            // DROP: skip everything missed, only enqueue the single most-recent fire.
            JobId id = materialize(task, now);
            Instant next = task.trigger().nextAfter(now, task.zone());
            store.upsertCronTaskState(new CronTaskScheduleState(task.name(), now, id.asUuid(), next, id.asUuid()));
        }
    }

    private JobId materialize(CronTask task, Instant when) {
        Job job = Job.builder()
                .spec(new JobSpec(task.handlerType(), List.of(task.payloadArgument())))
                .queue(task.queue())
                .priority(task.priority())
                .cronTaskName(task.name())
                .metadata(JobExecutionContext.CRON_FIRE_TIME_META, when.toString())
                .initialState(JobState.ENQUEUED)
                .build();
        store.insert(job);
        wakeBus.wake(task.queue());
        return job.id();
    }
}
