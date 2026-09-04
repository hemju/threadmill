package com.hemju.threadmill.core.schedule;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.JobStateEntry;
import com.hemju.threadmill.core.Names;
import com.hemju.threadmill.core.engine.JobRunner;
import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.engine.RetryInterceptor;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.internal.FatalErrors;
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
 * <p>If the under-mutex definition reload finds that the schedule state's
 * timing fingerprint belongs to a different definition, the definition wins:
 * timing is recomputed forward from the current tick and the stale schedule
 * produces no firing. This completes a crashed timing edit without running a
 * trigger the user already replaced. {@code CATCH_UP} resumes normally from
 * the repaired timing; it never catches up the obsolete trigger's backlog.
 *
 * <p>If a previously-materialised instance is still un-terminal, no new
 * instance is created until that one finishes. This guard prevents
 * pile-up under long-running recurring work.
 *
 * <p><strong>Nudge failure semantics.</strong> Consuming a nudge is three
 * independent durable operations (job insert, state upsert, revision
 * compare-and-clear), so the "at most the current run plus one follow-up"
 * coalescing bound is a failure-free bound: a crash or outage between the
 * insert and the clear leaves the nudge pending, and recovery materializes
 * again. Consistent with Threadmill's at-least-once model, failures can only
 * produce extra runs — never lose one.
 */
public final class RecurringMaterializer {

  private static final Logger LOG = LoggerFactory.getLogger(RecurringMaterializer.class);

  /** Per-tick cap on CATCH_UP materializations; the rest carries over. */
  private static final int MAX_CATCH_UP_PER_TICK = 100;

  /**
   * How long a FAILED instance may block the next materialization while
   * {@link RetryInterceptor} is still expected to reschedule it. Sized well
   * above the reschedule save's bounded retry (three attempts, 50ms apart)
   * plus store latency, and small enough that a genuinely terminal instance
   * the budget test could not rule out delays the next run negligibly.
   */
  private static final Duration FAILED_RETRY_HANDOFF_GRACE = Duration.ofSeconds(5);

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
      } catch (RuntimeException t) {
        FatalErrors.rethrowIfFatal(t);
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
        FatalErrors.rethrowIfFatal(ignored);
        // the lease expires on its own
      }
    }
  }

  private void tickOneLocked(CronTask listed, Instant now) {
    var stateOpt = store.findCronTaskState(listed.name());
    if (stateOpt.isEmpty()) return; // not yet initialised
    var state = stateOpt.get();
    Long nudge = state.nudgeRequestedAt() == null ? null : state.nudgeRevision();
    boolean due = state.nextRunAt() != null && !state.nextRunAt().isAfter(now);
    boolean listedFingerprintMismatch =
        !CronTaskScheduleState.timingFingerprintOf(listed).equals(state.timingFingerprint());
    if (!due && nudge == null && !listedFingerprintMismatch) return;

    // About to act — reload the definition now that we hold the task
    // mutex. The listed object was snapshotted by tick() BEFORE the
    // mutex: a re-registration, edit, or disable can commit in between,
    // and materializing from the stale object would insert the old
    // handler/payload (and, for a nudge, consume a request that was made
    // against the new definition). The reload is deliberately done only
    // when a materialization is imminent or the listed definition already
    // proves the timing state is stale, so ordinary idle ticks stay at one
    // state read per task while future stale schedules self-heal promptly.
    CronTask task = store.findCronTask(listed.name()).orElse(null);
    if (task == null || !task.enabled()) return;

    String fingerprint = CronTaskScheduleState.timingFingerprintOf(task);
    boolean timingStateChanged = false;
    if (!fingerprint.equals(state.timingFingerprint())) {
      String previousFingerprint = state.timingFingerprint();
      boolean legacyTiming = previousFingerprint == null && state.nextRunAt() != null;
      // A non-null mismatch is the crash signature for a timing edit
      // that wrote the definition before its schedule state. Finish it
      // by scheduling forward from this tick: firing the stale timing
      // would run a trigger the user already replaced. A legacy null
      // fingerprint does not prove an edit, so adopt the fingerprint
      // without dropping or moving an already-recorded firing.
      Instant next = legacyTiming ? state.nextRunAt() : task.trigger().nextAfter(now, task.zone());
      if (!legacyTiming) due = false;
      state = new CronTaskScheduleState(
          task.name(),
          state.lastRunAt(),
          state.lastRunJobId(),
          next,
          state.inFlightJobId(),
          fingerprint,
          // These cells are carried in the in-memory record only;
          // upsertCronTaskState deliberately never writes them.
          state.nudgeRequestedAt(),
          state.nudgeRevision());
      timingStateChanged = true;
      if (previousFingerprint != null) {
        LOG.warn(
            "Repairing stale recurring timing for task {} from fingerprint {} to {}; next run at {}",
            task.name(),
            previousFingerprint,
            fingerprint,
            next);
      }
      if (nudge == null && !due) {
        store.upsertCronTaskState(state);
        return;
      }
    }

    // The listed definition may simply be stale relative to an
    // authoritative definition and state that already agree. In that
    // case no scheduled firing or nudge is owed this tick.
    if (!due && nudge == null) return;

    // Pile-up guard: an in-flight instance that is still going to run
    // blocks the next materialization.
    if (state.inFlightJobId() != null) {
      Job inFlight = store.findById(JobId.of(state.inFlightJobId())).orElse(null);
      if (inFlight != null && blocksNextMaterialization(inFlight, now)) {
        if (timingStateChanged) store.upsertCronTaskState(state);
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
          task.name(), now, id.asUuid(), state.nextRunAt(), id.asUuid(), fingerprint));
      // Clear AFTER materializing (a crash between the two costs one
      // extra run, never a lost one — see the failure-semantics note in
      // the class Javadoc), and only the observed revision — a nudge
      // accepted since our read carries a greater revision and survives
      // for a follow-up run.
      store.clearCronNudge(task.name(), nudge);
      return;
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
        LOG.debug(
            "CATCH_UP for task {} hit the per-tick cap; continuing on the next tick", task.name());
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
      // producing an extra run. Compare-and-clear on the revision: a
      // newer nudge survives.
      store.clearCronNudge(task.name(), nudge);
    }
  }

  /**
   * Whether an in-flight instance still stands between the task and its
   * next materialization.
   *
   * <p>Anything else non-terminal blocks outright, and a genuinely terminal
   * state never does. FAILED is the interesting case, and the reason
   * {@link JobState#isTerminal()} deliberately excludes it: a retry may
   * still follow, so the state alone does not say whether the instance is
   * finished. Treating every FAILED as non-blocking — the original guard —
   * left a window in which a tick landing between the failure save and
   * {@link RetryInterceptor}'s SCHEDULED save materialized a fresh instance
   * alongside a retrying one. Treating every FAILED as blocking would let a
   * retry-exhausted instance deadlock the task forever.
   *
   * <p>So FAILED blocks only while it is <em>plausibly</em> mid-handoff:
   * the retry budget is not provably spent <strong>and</strong> the failure
   * is younger than {@link #FAILED_RETRY_HANDOFF_GRACE}. Both halves carry
   * weight. The budget test alone is only approximate, because the
   * effective ceiling depends on the exception that caused the failure
   * (per-exception-type policies are registered on the interceptor and are
   * not readable from the job), so a job that is genuinely terminal under a
   * stricter policy looks budget-remaining here; the age bound is what
   * stops that job from blocking its task until
   * {@link RetryInterceptor#recoverStrandedFailures} happens to reach it.
   * The budget test in turn keeps the common terminal failure from delaying
   * the next run at all.
   */
  private static boolean blocksNextMaterialization(Job inFlight, Instant now) {
    JobState current = inFlight.currentState();
    if (current != JobState.FAILED) {
      return !current.isTerminal();
    }
    if (retryBudgetProvablySpent(inFlight)) return false;
    List<JobStateEntry> history = inFlight.stateHistory();
    if (history.isEmpty()) return false;
    return history.getLast().at().isAfter(now.minus(FAILED_RETRY_HANDOFF_GRACE));
  }

  /**
   * Whether the job's retry budget is provably spent from the job alone.
   * Only the per-job override is readable here; absent or malformed
   * metadata means "cannot tell", which is reported as budget remaining so
   * the age bound decides.
   */
  private static boolean retryBudgetProvablySpent(Job job) {
    return job.metadata()
        .get(RetryInterceptor.META_MAX_ATTEMPTS)
        .map(raw -> {
          try {
            return job.attempts() >= Integer.parseInt(raw.trim());
          } catch (RuntimeException malformed) {
            FatalErrors.rethrowIfFatal(malformed);
            return false;
          }
        })
        .orElse(false);
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
    // An exclusive task claims every instance under one derived key, so
    // claim-time admission — not this materializer's pile-up guard — is
    // what serializes them. That covers paths the guard cannot see: a
    // dashboard manual trigger racing a scheduled instance, and the
    // retry handoff window, where the fresh instance simply waits for
    // the retrying one to release the key instead of overlapping it.
    if (task.exclusive()) {
      builder
          .concurrencyKey(CronTask.concurrencyKeyFor(task.name()))
          .concurrencyMode(ConcurrencyMode.EXCLUSIVE);
    }
    Job job = builder.build();
    store.insert(job);
    wakeBus.wake(task.queue());
    return job.id();
  }
}
