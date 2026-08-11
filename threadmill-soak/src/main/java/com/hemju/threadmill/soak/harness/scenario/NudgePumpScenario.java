package com.hemju.threadmill.soak.harness.scenario;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.QueueLane;
import com.hemju.threadmill.core.handler.NoPayload;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.Scheduler;
import com.hemju.threadmill.soak.harness.LoadGenerator;
import com.hemju.threadmill.soak.harness.SoakOutbox;
import com.hemju.threadmill.soak.harness.invariant.InvariantChecks;
import com.hemju.threadmill.soak.harness.invariant.SoakInvariant;

/**
 * The wake-driven poller shape from issue #108, under sustained load.
 *
 * <p>One recurring task is registered with a deliberately slow backstop
 * schedule ({@value #BACKSTOP_MINUTES} minutes), so during a run of ordinary
 * length essentially every pump execution must come from a nudge rather than
 * from the schedule — the backstop is present to prove it self-heals, not to
 * carry the workload. Producers append an outbox row and nudge; the pump
 * handler drains everything visible.
 *
 * <p>Background {@code FixedWork} keeps the engine under real pressure
 * alongside the recurring machinery, because the interesting interference
 * lives there: maintenance-tick contention, master handover under node churn,
 * and a busy dispatcher competing with pump materialization.
 *
 * <p>The invariants encode the feature's two load-bearing promises directly:
 * every accepted nudge is followed by a run that starts after it, and a row
 * appended before a run started is drained by the time that run finishes —
 * the end-to-end statement of "a nudge is never silently swallowed".
 * Coalescing is deliberately <em>not</em> an invariant: it is a bound on
 * duplicate work, so violating it wastes effort but loses nothing, and the
 * nudge-to-run ratio in the trace shows it directly.
 */
public final class NudgePumpScenario implements SoakScenario {

    /** Slow enough that the schedule is a backstop, not the driver. */
    private static final int BACKSTOP_MINUTES = 10;

    private static final String TASK_NAME = "soak-outbox-pump";
    private static final String PUMP_QUEUE = Scheduler.SYSTEM_QUEUE;
    private static final String LOAD_QUEUE = "default";

    /** One outbox row + nudge per this many background jobs. */
    private static final int NUDGE_EVERY = 2;

    /**
     * How long the producer waits for its final nudge to produce a run before
     * giving up and letting the invariant judge. Generous against the default
     * one-second maintenance tick, and well inside {@link #drainBudget()}.
     */
    private static final Duration TAIL_RUN_WAIT = Duration.ofSeconds(30);

    private final AtomicBoolean registrationStarted = new AtomicBoolean();
    private final CountDownLatch registered = new CountDownLatch(1);
    private final AtomicLong nudgeSeq = new AtomicLong();

    public NudgePumpScenario() {
        // One scenario instance per run, constructed before any producer
        // thread starts — the only safe point to clear the shared outbox.
        SoakOutbox.reset();
    }

    @Override
    public String name() {
        return "nudge-pump";
    }

    @Override
    public String description() {
        return "wake-driven outbox pump: producers append work rows and nudge a recurring task whose own"
                + " schedule is a slow " + BACKSTOP_MINUTES + "-minute backstop, plus background load";
    }

    @Override
    public List<SoakInvariant> invariants() {
        return List.of(
                InvariantChecks.atLeastOnce(),
                InvariantChecks.nudgeRunAfterWake(),
                InvariantChecks.outboxDrainedByLaterRun());
    }

    @Override
    public void configureNode(ProcessingNode.Builder b) {
        // The pump gets its own lane: a recurring/system job must never be
        // starved by the background flood (that is what SYSTEM_QUEUE is for).
        b.lane(new QueueLane(PUMP_QUEUE, 2));
        b.lane(new QueueLane(LOAD_QUEUE, 8));
    }

    @Override
    public Duration drainBudget() {
        // The final nudges must still be served after the producers stop:
        // one maintenance tick to materialize, then the run itself.
        return Duration.ofSeconds(90);
    }

    @Override
    public void runWorkload(LoadGenerator gen, SoakRunContext ctx) throws InterruptedException {
        ensureTaskRegistered(gen.scheduler());
        Instant runStart = ctx.runStart();
        long n = 0;
        while (Instant.now().isBefore(ctx.runDeadline())) {
            gen.pace(gen.deadlineFor(runStart, n));
            gen.enqueue(
                    new SoakPayloads.FixedWork((int) n, 4, 0.0), SoakPayloads.FixedWorkHandler.class, LOAD_QUEUE, 0);
            if (n % NUDGE_EVERY == 0) {
                appendWorkAndNudge(gen, ctx);
            }
            n++;
        }
        // A final nudge after the producer loop, then hold the tail open
        // until a pump run actually starts. The harness's drain phase waits
        // for active jobs, and a not-yet-materialized nudge is not a job — so
        // without this wait the nodes can stop milliseconds after the final
        // nudge, before the maintenance tick could serve it, and the
        // run-after-wake completeness check would fail on a healthy engine.
        // The wait keeps its teeth: if no run starts within the window, the
        // invariant still fails, which is the correct outcome for a real bug.
        long runsBefore = SoakOutbox.runsStarted();
        appendWorkAndNudge(gen, ctx);
        awaitFollowUpRun(runsBefore);
    }

    private void awaitFollowUpRun(long runsBefore) throws InterruptedException {
        Instant deadline = Instant.now().plus(TAIL_RUN_WAIT);
        while (SoakOutbox.runsStarted() <= runsBefore && Instant.now().isBefore(deadline)) {
            Thread.sleep(50);
        }
    }

    private void appendWorkAndNudge(LoadGenerator gen, SoakRunContext ctx) {
        SoakOutbox.append();
        gen.scheduler().nudgeRecurring(TASK_NAME);
        // Emitted only after the store write returned: an accepted nudge is
        // what the invariant holds the engine to.
        var fields = new LinkedHashMap<String, Object>();
        fields.put("task", TASK_NAME);
        fields.put("nudgeSeq", nudgeSeq.incrementAndGet());
        ctx.trace().emit("nudge_accepted", fields);
    }

    /**
     * Register the pump task exactly once per run, with every other producer
     * thread waiting: a nudge for an unregistered task throws by design.
     */
    private void ensureTaskRegistered(Scheduler scheduler) throws InterruptedException {
        if (registrationStarted.compareAndSet(false, true)) {
            try {
                scheduler.defineIntervalTask(
                        TASK_NAME,
                        Duration.ofMinutes(BACKSTOP_MINUTES),
                        NoPayload.INSTANCE,
                        SoakPayloads.OutboxPumpHandler.class,
                        PUMP_QUEUE,
                        0,
                        CronTask.MissedRunPolicy.DROP);
            } finally {
                registered.countDown();
            }
            return;
        }
        registered.await();
    }
}
