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
        if (state.nextRunAt() == null || state.nextRunAt().isAfter(now)) return;

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
        var builder = Job.builder()
                .spec(new JobSpec(task.handlerType(), List.of(task.payloadArgument())))
                .queue(task.queue())
                .priority(task.priority())
                .cronTaskName(task.name())
                .metadata(JobExecutionContext.CRON_FIRE_TIME_META, when.toString())
                .initialState(JobState.ENQUEUED);
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
