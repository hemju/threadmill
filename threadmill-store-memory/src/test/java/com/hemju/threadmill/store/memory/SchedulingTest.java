package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.JobRunner;
import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.engine.QueueLane;
import com.hemju.threadmill.core.engine.RetryInterceptor;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.schedule.CronExpression;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.schedule.RecurringMaterializer;
import com.hemju.threadmill.core.schedule.Scheduler;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.test.ForwardingJobStore;

/**
 * End-to-end tests for the scheduling API, recurring tasks (interval +
 * missed-run policy), per-queue lanes (starvation prevention), and
 * priority-within-queue.
 */
class SchedulingTest {

    private InMemoryJobStore store;
    private Scheduler scheduler;
    private ProcessingNode node;
    private final JsonJobSerializer serializer = new JsonJobSerializer();

    @BeforeEach
    void setUp() {
        store = new InMemoryJobStore();
        scheduler = new Scheduler(store, serializer);
        RecorderHandler.RECORD.clear();
        AdHocHandler.RECORD.clear();
        SystemHandler.RECORD.clear();
        PriorityHandler.RECORD.clear();
    }

    @AfterEach
    void tearDown() {
        if (node != null) node.close();
    }

    // -------- handlers --------

    public static final class HelloPayload implements JobPayload {
        public String tag;

        public HelloPayload() {}

        public HelloPayload(String tag) {
            this.tag = tag;
        }
    }

    public static final class RecorderHandler implements JobHandler<HelloPayload> {
        public static final ConcurrentLinkedQueue<String> RECORD = new ConcurrentLinkedQueue<>();

        @Override
        public void run(HelloPayload p, JobExecutionContext c) {
            RECORD.add(p.tag);
        }
    }

    public static final class AdHocHandler implements JobHandler<HelloPayload> {
        public static final ConcurrentLinkedQueue<String> RECORD = new ConcurrentLinkedQueue<>();

        @Override
        public void run(HelloPayload p, JobExecutionContext c) throws InterruptedException {
            // Slow on purpose to flood the lane.
            Thread.sleep(40);
            RECORD.add(p.tag);
        }
    }

    public static final class SystemHandler implements JobHandler<HelloPayload> {
        public static final ConcurrentLinkedQueue<String> RECORD = new ConcurrentLinkedQueue<>();

        @Override
        public void run(HelloPayload p, JobExecutionContext c) {
            RECORD.add(p.tag);
        }
    }

    public static final class PriorityHandler implements JobHandler<HelloPayload> {
        public static final ConcurrentLinkedQueue<String> RECORD = new ConcurrentLinkedQueue<>();

        @Override
        public void run(HelloPayload p, JobExecutionContext c) throws InterruptedException {
            Thread.sleep(20);
            RECORD.add(p.tag);
        }
    }

    // -------- tests --------

    @Test
    void enqueueRunsAJobThroughTheSchedulerApi() {
        scheduler.enqueue(new HelloPayload("hi"), RecorderHandler.class);
        node = ProcessingNode.builder(store).config(fastConfig()).build();
        node.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> RecorderHandler.RECORD.contains("hi"));
    }

    @Test
    void scheduleInFiresAfterTheDelayWithTypedHandlerAgreement() {
        scheduler.scheduleIn(Duration.ofMillis(150), new HelloPayload("delayed"), RecorderHandler.class);
        node = ProcessingNode.builder(store).config(fastConfig()).build();
        node.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> RecorderHandler.RECORD.contains("delayed"));
    }

    @Test
    void scheduleAtFiresOnceItsTimeArrives() {
        scheduler.scheduleAt(Instant.now().plusMillis(200), new HelloPayload("later"), RecorderHandler.class);
        node = ProcessingNode.builder(store).config(fastConfig()).build();
        node.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> RecorderHandler.RECORD.contains("later"));
    }

    @Test
    void scheduledPromotionWakesLocalDispatcherWhenJobBecomesClaimable() {
        var wakeBus = new LocalWakeBus();
        scheduler = new Scheduler(store, serializer, wakeBus);
        scheduler.scheduleAt(Instant.now().plusMillis(150), new HelloPayload("later"), RecorderHandler.class);
        node = ProcessingNode.builder(store)
                .config(fastConfig().toBuilder()
                        .pollInterval(Duration.ofSeconds(3))
                        .maintenancePollInterval(Duration.ofMillis(20))
                        .build())
                .wakeBus(wakeBus)
                .build();

        node.start();

        await().atMost(Duration.ofSeconds(2)).until(() -> RecorderHandler.RECORD.contains("later"));
    }

    @Test
    void intervalRecurringFiresRepeatedlyUnderDropPolicy() {
        scheduler.defineIntervalTask("ping", Duration.ofMillis(250), new HelloPayload("tick"), RecorderHandler.class);
        node = ProcessingNode.builder(store).config(fastConfig()).build();
        node.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> RecorderHandler.RECORD.size() >= 3);
    }

    @Test
    void recurringRegistrationNeverMutatesWithoutItsTaskMutex() {
        String mutex = RecurringMaterializer.taskMutexName("locked");
        assertThat(store.tryAcquireMutex(mutex, "other-node", Duration.ofMinutes(1)))
                .isTrue();

        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> scheduler.defineIntervalTask(
                            "locked", Duration.ofMinutes(1), new HelloPayload("tick"), RecorderHandler.class))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("recurring-state mutex");
        } finally {
            Thread.interrupted();
        }

        assertThat(store.findCronTask("locked")).isEmpty();
        assertThat(store.findCronTaskState("locked")).isEmpty();
    }

    @Test
    void recurringDeletionNeverRacesTheMaterializerTaskMutex() {
        scheduler.defineIntervalTask("locked", Duration.ofMinutes(1), new HelloPayload("tick"), RecorderHandler.class);
        String mutex = RecurringMaterializer.taskMutexName("locked");
        assertThat(store.tryAcquireMutex(mutex, "materializer", Duration.ofMinutes(1)))
                .isTrue();

        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> scheduler.deleteCronTask("locked"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("recurring-state mutex");
        } finally {
            Thread.interrupted();
        }

        assertThat(store.findCronTask("locked")).isPresent();
        assertThat(store.findCronTaskState("locked")).isPresent();
    }

    @Test
    void catchUpPolicyMaterializesEveryMissedFire() {
        // Pre-create a task whose next run is in the past, so the master tick has to catch up.
        scheduler.defineIntervalTask(
                "catchup",
                Duration.ofMillis(100),
                new HelloPayload("ping"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.CATCH_UP);
        // Backdate next run by ~1 second; expect ~10 catch-up runs in the first tick.
        var existing = store.findCronTaskState("catchup").orElseThrow();
        store.upsertCronTaskState(new CronTaskScheduleState(
                existing.taskName(), null, null, Instant.now().minus(Duration.ofSeconds(1)), null));
        node = ProcessingNode.builder(store).config(fastConfig()).build();
        node.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> RecorderHandler.RECORD.size() >= 5);
        assertThat(RecorderHandler.RECORD.size()).isGreaterThanOrEqualTo(5);
    }

    @Test
    void dropPolicyDoesNotCauseACatchUpStorm() {
        scheduler.defineIntervalTask(
                "ping",
                Duration.ofMillis(100),
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.DROP);
        // Backdate next run far into the past.
        var existing = store.findCronTaskState("ping").orElseThrow();
        store.upsertCronTaskState(new CronTaskScheduleState(
                existing.taskName(), null, null, Instant.now().minus(Duration.ofSeconds(60)), null));
        node = ProcessingNode.builder(store).config(fastConfig()).build();
        node.start();

        // Allow a couple of materializer ticks: under DROP, only one instance should fire.
        await().atMost(Duration.ofSeconds(3)).until(() -> RecorderHandler.RECORD.size() >= 1);
        try {
            Thread.sleep(300);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        // Should be a single-digit number, not 600 (one per missed 100ms interval).
        assertThat(RecorderHandler.RECORD.size()).isLessThan(20);
    }

    @Test
    void catchUpInstancesCarryDistinctNominalFireTimes() {
        scheduler.defineIntervalTask(
                "stamped",
                Duration.ofMillis(100),
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.CATCH_UP);
        var existing = store.findCronTaskState("stamped").orElseThrow();
        Instant base = Instant.now().minusMillis(350);
        store.upsertCronTaskState(new CronTaskScheduleState(existing.taskName(), null, null, base, null));

        new RecurringMaterializer(store).tick(Instant.now());

        List<Job> instances = store.findByHandlerSignature(RecorderHandler.class.getName(), 100);
        assertThat(instances).hasSizeGreaterThanOrEqualTo(3);
        var fireTimes = instances.stream()
                .map(j -> j.metadata()
                        .get(JobExecutionContext.CRON_FIRE_TIME_META)
                        .orElseThrow())
                .map(Instant::parse)
                .collect(Collectors.toSet());
        // Every instance represents a distinct interval, starting at the
        // earliest missed fire — that's what makes CATCH_UP instances usable
        // for per-interval idempotency keys.
        assertThat(fireTimes).hasSameSizeAs(instances);
        assertThat(fireTimes).contains(base);
    }

    @Test
    void materializerSkipsATaskWhoseStateMutexIsHeld() {
        scheduler.defineIntervalTask("locked", Duration.ofMillis(100), new HelloPayload("tick"), RecorderHandler.class);
        var existing = store.findCronTaskState("locked").orElseThrow();
        store.upsertCronTaskState(new CronTaskScheduleState(
                existing.taskName(), null, null, Instant.now().minusSeconds(1), null));

        // Another holder (e.g. an upsertCron on a different node) owns the
        // task's schedule-state mutex: the tick must skip, not clobber.
        String mutex = RecurringMaterializer.taskMutexName("locked");
        assertThat(store.tryAcquireMutex(mutex, "other-holder", Duration.ofSeconds(10)))
                .isTrue();
        var materializer = new RecurringMaterializer(store);
        materializer.tick(Instant.now());
        assertThat(store.findByHandlerSignature(RecorderHandler.class.getName(), 10))
                .isEmpty();

        // Once released, the next tick materializes normally.
        store.releaseMutex(mutex, "other-holder");
        materializer.tick(Instant.now());
        assertThat(store.findByHandlerSignature(RecorderHandler.class.getName(), 10))
                .isNotEmpty();
    }

    @Test
    void upsertCronWaitsForTheTaskMutexAndPreservesInFlightTracking() {
        scheduler.defineIntervalTask(
                "guarded", Duration.ofMillis(100), new HelloPayload("tick"), RecorderHandler.class);
        var existing = store.findCronTaskState("guarded").orElseThrow();
        store.upsertCronTaskState(new CronTaskScheduleState(
                existing.taskName(), null, null, Instant.now().minusSeconds(1), null));
        new RecurringMaterializer(store).tick(Instant.now());
        var inFlight = store.findCronTaskState("guarded").orElseThrow().inFlightJobId();
        assertThat(inFlight).isNotNull();

        // A short-lived foreign hold delays re-registration instead of letting
        // it proceed concurrently; the tracked in-flight instance survives.
        String mutex = RecurringMaterializer.taskMutexName("guarded");
        assertThat(store.tryAcquireMutex(mutex, "other-holder", Duration.ofMillis(250)))
                .isTrue();
        long before = System.nanoTime();
        scheduler.defineIntervalTask(
                "guarded", Duration.ofMillis(200), new HelloPayload("tick"), RecorderHandler.class);
        long elapsedMillis = (System.nanoTime() - before) / 1_000_000;

        assertThat(elapsedMillis).isGreaterThanOrEqualTo(150);
        assertThat(store.findCronTaskState("guarded").orElseThrow().inFlightJobId())
                .isEqualTo(inFlight);
    }

    @Test
    void restartReRegistrationPreservesOverdueNextRunSoCatchUpRecoversMissedFires() {
        // Regression for github issue #105: startup re-registration used to
        // recompute next_run_at unconditionally, so firings missed while the
        // application was down were wiped before CATCH_UP could observe them —
        // the exact scenario the policy exists for.
        scheduler.defineIntervalTask(
                "restart-catchup",
                Duration.ofMillis(100),
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.CATCH_UP);
        var existing = store.findCronTaskState("restart-catchup").orElseThrow();
        Instant missed = Instant.now().minusMillis(350);
        store.upsertCronTaskState(
                new CronTaskScheduleState(existing.taskName(), null, null, missed, null, existing.timingFingerprint()));

        // Simulated restart: the application re-registers the identical task.
        scheduler.defineIntervalTask(
                "restart-catchup",
                Duration.ofMillis(100),
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.CATCH_UP);

        assertThat(store.findCronTaskState("restart-catchup").orElseThrow().nextRunAt())
                .as("unchanged re-registration must not wipe the overdue next run")
                .isEqualTo(missed);

        new RecurringMaterializer(store).tick(Instant.now());
        assertThat(store.findByHandlerSignature(RecorderHandler.class.getName(), 100))
                .hasSizeGreaterThanOrEqualTo(3);
    }

    @Test
    void restartReRegistrationWithDropCollapsesMissedFiresIntoASingleRun() {
        // The DROP counterpart of the github issue #105 restart scenario: an
        // overdue next run survives re-registration, and the materializer's
        // DROP semantics collapse the whole missed backlog into one instance
        // for the single most recent NOMINAL fire — phase-aligned, so the
        // schedule never drifts, and stamped with the nominal fire time.
        Duration interval = Duration.ofMillis(100);
        scheduler.defineIntervalTask(
                "restart-drop",
                interval,
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.DROP);
        var existing = store.findCronTaskState("restart-drop").orElseThrow();
        Instant missed = Instant.now().minus(Duration.ofSeconds(60));
        store.upsertCronTaskState(
                new CronTaskScheduleState(existing.taskName(), null, null, missed, null, existing.timingFingerprint()));

        scheduler.defineIntervalTask(
                "restart-drop",
                interval,
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.DROP);

        Instant tickTime = Instant.now();
        new RecurringMaterializer(store).tick(tickTime);

        // Exactly one make-up instance, representing the most recent nominal
        // fire on the original 100ms grid anchored at `missed`.
        long missedCount = Duration.between(missed, tickTime).dividedBy(interval);
        Instant nominalFire = missed.plus(interval.multipliedBy(missedCount));
        List<Job> instances = store.findByHandlerSignature(RecorderHandler.class.getName(), 100);
        assertThat(instances).hasSize(1);
        assertThat(instances.get(0).metadata().get(JobExecutionContext.CRON_FIRE_TIME_META))
                .contains(nominalFire.toString());
        // The next run advances from the nominal fire, not from the tick:
        // interval phase is preserved exactly.
        assertThat(store.findCronTaskState("restart-drop").orElseThrow().nextRunAt())
                .isEqualTo(nominalFire.plus(interval));
    }

    @Test
    void reRegistrationWithAChangedTriggerRecomputesNextRunFromNow() {
        // The original intent of the upsertCron recompute, kept for real
        // edits: a freshly edited schedule must not fire stale times.
        scheduler.defineIntervalTask(
                "edited",
                Duration.ofMillis(100),
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.CATCH_UP);
        var existing = store.findCronTaskState("edited").orElseThrow();
        store.upsertCronTaskState(new CronTaskScheduleState(
                existing.taskName(),
                null,
                null,
                Instant.now().minus(Duration.ofSeconds(60)),
                null,
                existing.timingFingerprint()));

        scheduler.defineIntervalTask(
                "edited",
                Duration.ofHours(1),
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.CATCH_UP);

        assertThat(store.findCronTaskState("edited").orElseThrow().nextRunAt()).isAfter(Instant.now());
        new RecurringMaterializer(store).tick(Instant.now());
        assertThat(store.findByHandlerSignature(RecorderHandler.class.getName(), 100))
                .isEmpty();
    }

    @Test
    void reRegistrationWithAChangedZoneRecomputesNextRun() {
        scheduler.defineCronTask(
                "zoned",
                "0 3 * * *",
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.CATCH_UP,
                ZoneId.of("UTC"));
        var existing = store.findCronTaskState("zoned").orElseThrow();
        store.upsertCronTaskState(new CronTaskScheduleState(
                existing.taskName(),
                null,
                null,
                Instant.now().minus(Duration.ofHours(2)),
                null,
                existing.timingFingerprint()));

        scheduler.defineCronTask(
                "zoned",
                "0 3 * * *",
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.CATCH_UP,
                ZoneId.of("America/New_York"));

        assertThat(store.findCronTaskState("zoned").orElseThrow().nextRunAt()).isAfter(Instant.now());
    }

    @Test
    void differentSystemZonesDoNotResetAnIntervalSchedule() {
        // An interval trigger ignores its zone (CronTask contract), so two
        // nodes with different system-default zones re-registering the same
        // interval must not treat it as a schedule edit — the timing
        // fingerprint deliberately excludes the zone for intervals.
        scheduler.defineIntervalTask(
                "cross-zone", Duration.ofHours(6), new HelloPayload("tick"), RecorderHandler.class);
        var existing = store.findCronTaskState("cross-zone").orElseThrow();
        Instant missed = Instant.now().minusSeconds(60);
        store.upsertCronTaskState(
                new CronTaskScheduleState(existing.taskName(), null, null, missed, null, existing.timingFingerprint()));
        // Simulate the earlier registration having happened on a node with a
        // different system-default zone.
        var task = store.findCronTask("cross-zone").orElseThrow();
        store.upsertCronTask(new CronTask(
                task.name(),
                task.trigger(),
                task.handlerType(),
                task.payloadArgument(),
                task.queue(),
                task.priority(),
                task.timeout(),
                task.maxAttempts(),
                task.missedRunPolicy(),
                ZoneId.of("Pacific/Auckland"),
                task.enabled()));

        scheduler.defineIntervalTask(
                "cross-zone", Duration.ofHours(6), new HelloPayload("tick"), RecorderHandler.class);

        assertThat(store.findCronTaskState("cross-zone").orElseThrow().nextRunAt())
                .isEqualTo(missed);
    }

    @Test
    void stateWriteFailureDuringTriggerEditCannotPairNewTriggerWithOldTiming() {
        // Review finding on github issue #105: the task definition and its
        // schedule state are two separate store writes. If the definition
        // write lands and the state write fails, a retry that compared the
        // stored trigger against the re-registered one would see them equal
        // and preserve the stale timing forever — firing the new definition
        // at the old trigger's overdue time. The timing fingerprint travels
        // atomically with nextRunAt in the state record, so the retry
        // detects the mismatch and recomputes.
        scheduler.defineIntervalTask(
                "crash-window",
                Duration.ofMillis(100),
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.CATCH_UP);
        var existing = store.findCronTaskState("crash-window").orElseThrow();
        Instant staleOverdue = Instant.now().minusSeconds(60);
        store.upsertCronTaskState(new CronTaskScheduleState(
                existing.taskName(), null, null, staleOverdue, null, existing.timingFingerprint()));

        var failing = new AtomicBoolean(true);
        var flaky = new ForwardingJobStore(store) {
            @Override
            public void upsertCronTaskState(CronTaskScheduleState state) {
                if (failing.get()) {
                    throw new IllegalStateException("simulated outage after the definition write");
                }
                super.upsertCronTaskState(state);
            }
        };
        var flakyScheduler = new Scheduler(flaky, serializer);
        assertThatThrownBy(() -> flakyScheduler.defineIntervalTask(
                        "crash-window",
                        Duration.ofHours(6),
                        new HelloPayload("tick"),
                        RecorderHandler.class,
                        "default",
                        0,
                        CronTask.MissedRunPolicy.CATCH_UP))
                .hasMessageContaining("simulated outage");

        // The definition write landed: the store now pairs the NEW trigger
        // with the OLD state. The registration retry must recompute.
        failing.set(false);
        flakyScheduler.defineIntervalTask(
                "crash-window",
                Duration.ofHours(6),
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.CATCH_UP);

        assertThat(store.findCronTaskState("crash-window").orElseThrow().nextRunAt())
                .isAfter(Instant.now());
        new RecurringMaterializer(store).tick(Instant.now());
        assertThat(store.findByHandlerSignature(RecorderHandler.class.getName(), 100))
                .isEmpty();
    }

    @Test
    void unchangedReRegistrationPreservesTheIntervalPhase() {
        // An interval trigger recomputed from "now" at every restart shifts
        // its whole schedule (an every-6h task restarted at hour 3 slides by
        // three hours). An unchanged re-registration keeps the phase.
        scheduler.defineIntervalTask("phased", Duration.ofHours(6), new HelloPayload("tick"), RecorderHandler.class);
        Instant firstNext = store.findCronTaskState("phased").orElseThrow().nextRunAt();

        try {
            Thread.sleep(5);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        scheduler.defineIntervalTask("phased", Duration.ofHours(6), new HelloPayload("tick"), RecorderHandler.class);

        assertThat(store.findCronTaskState("phased").orElseThrow().nextRunAt()).isEqualTo(firstNext);
    }

    @Test
    void reEnablingATaskRestartsTimingInsteadOfCatchingUpTheDisabledPeriod() {
        // A disabled task is an explicit "don't run", not downtime: flipping
        // it back on must not fire the disabled period's missed runs.
        scheduler.defineIntervalTask(
                "re-enabled",
                Duration.ofMillis(100),
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.CATCH_UP);
        var existing = store.findCronTaskState("re-enabled").orElseThrow();
        store.upsertCronTaskState(new CronTaskScheduleState(
                existing.taskName(),
                null,
                null,
                Instant.now().minus(Duration.ofSeconds(60)),
                null,
                existing.timingFingerprint()));
        var task = store.findCronTask("re-enabled").orElseThrow();
        store.upsertCronTask(new CronTask(
                task.name(),
                task.trigger(),
                task.handlerType(),
                task.payloadArgument(),
                task.queue(),
                task.priority(),
                task.timeout(),
                task.maxAttempts(),
                task.missedRunPolicy(),
                task.zone(),
                false));

        scheduler.defineIntervalTask(
                "re-enabled",
                Duration.ofMillis(100),
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.CATCH_UP);

        assertThat(store.findCronTaskState("re-enabled").orElseThrow().nextRunAt())
                .isAfter(Instant.now());
        new RecurringMaterializer(store).tick(Instant.now());
        assertThat(store.findByHandlerSignature(RecorderHandler.class.getName(), 100))
                .isEmpty();
    }

    @Test
    void exclusiveRecurringInstancesAreSerializedByClaimTimeAdmission() {
        // Regression for github issue #110 item 3. An exclusive task stamps
        // every instance with the derived recurring: key in EXCLUSIVE mode, so
        // the store — not the materializer's pile-up guard — is what stops a
        // second instance from running. Two instances are forced into the
        // queue simultaneously (the shape the guard cannot prevent: a manual
        // trigger beside a scheduled instance); claim-time admission must
        // release only one.
        var task = new CronTask(
                "nightly-sweep",
                new CronTask.Trigger.Interval(Duration.ofMinutes(5)),
                RecorderHandler.class.getName(),
                serializer.serializePayload(new HelloPayload("tick")),
                "default",
                0,
                null,
                null,
                true,
                CronTask.MissedRunPolicy.DROP,
                ZoneId.systemDefault(),
                true);
        scheduler.defineRecurring(
                task.name(),
                task.trigger(),
                new HelloPayload("tick"),
                task.handlerType(),
                task.queue(),
                task.priority(),
                null,
                null,
                true,
                task.missedRunPolicy());
        var initial = store.findCronTaskState("nightly-sweep").orElseThrow();
        store.upsertCronTaskState(new CronTaskScheduleState(
                initial.taskName(), null, null, Instant.now().minusSeconds(1), null));

        var materializer = new RecurringMaterializer(store);
        materializer.tick(Instant.now());
        var state = store.findCronTaskState("nightly-sweep").orElseThrow();
        Job first = store.findById(JobId.of(state.inFlightJobId())).orElseThrow();

        assertThat(first.concurrencyKey()).contains(CronTask.concurrencyKeyFor("nightly-sweep"));
        assertThat(first.concurrencyKey()).contains("recurring:nightly-sweep");
        assertThat(first.concurrencyMode()).contains(ConcurrencyMode.EXCLUSIVE);

        // A second instance for the same task, enqueued while the first is
        // still pending — exactly what a dashboard manual trigger produces.
        store.insert(Job.builder()
                .spec(first.spec())
                .queue("default")
                .cronTaskName("nightly-sweep")
                .concurrencyKey(CronTask.concurrencyKeyFor("nightly-sweep"))
                .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
                .build());

        assertThat(store.claimReady(NodeId.newId(), "default", 10, Instant.now()))
                .as("claim-time admission must let only one exclusive instance through")
                .hasSize(1);
    }

    @Test
    void nonExclusiveRecurringInstancesCarryNoConcurrencyKey() {
        scheduler.defineIntervalTask("loose", Duration.ofMinutes(5), new HelloPayload("tick"), RecorderHandler.class);
        var initial = store.findCronTaskState("loose").orElseThrow();
        store.upsertCronTaskState(new CronTaskScheduleState(
                initial.taskName(), null, null, Instant.now().minusSeconds(1), null));
        new RecurringMaterializer(store).tick(Instant.now());

        var state = store.findCronTaskState("loose").orElseThrow();
        Job instance = store.findById(JobId.of(state.inFlightJobId())).orElseThrow();
        assertThat(instance.concurrencyKey()).isEmpty();
        assertThat(instance.concurrencyMode()).isEmpty();
    }

    @Test
    void retryExhaustedFailedInstanceDoesNotBlockTheNextMaterialization() {
        // A FAILED instance whose retry budget is provably spent is genuinely
        // terminal — nothing is going to reschedule it — so it must never
        // deadlock the task, and must not even delay the next run.
        UUID priorInstance = failInstanceOfTaskWithBudget("exhausted", 1, Instant.now());

        new RecurringMaterializer(store).tick(Instant.now());

        assertThat(store.findCronTaskState("exhausted").orElseThrow().inFlightJobId())
                .isNotEqualTo(priorInstance);
    }

    @Test
    void failedInstanceAwaitingItsRetryBlocksTheNextMaterialization() {
        // Regression for github issue #110 item 2. FAILED is terminal in the
        // state machine but only terminal-PENDING while RetryInterceptor still
        // owes the job a SCHEDULED save, and those are two separate store
        // writes. The original guard treated every FAILED as non-blocking, so
        // a tick landing inside that window materialized a fresh instance
        // beside a retrying one. With budget left and the failure fresh, the
        // guard must hold.
        UUID priorInstance = failInstanceOfTaskWithBudget("retrying", 5, Instant.now());

        new RecurringMaterializer(store).tick(Instant.now());

        assertThat(store.findCronTaskState("retrying").orElseThrow().inFlightJobId())
                .isEqualTo(priorInstance);
    }

    @Test
    void failedInstanceStopsBlockingOnceTheRetryHandoffGraceElapses() {
        // The budget test is only approximate: the effective ceiling depends
        // on the exception, and per-exception-type policies live on the
        // interceptor rather than on the job. A job that is terminal under a
        // stricter policy therefore still looks budget-remaining here, so the
        // age bound — not the budget test — is what guarantees it cannot block
        // its task until recoverStrandedFailures happens to reach it.
        UUID priorInstance =
                failInstanceOfTaskWithBudget("stale-failure", 5, Instant.now().minus(Duration.ofMinutes(1)));

        new RecurringMaterializer(store).tick(Instant.now());

        assertThat(store.findCronTaskState("stale-failure").orElseThrow().inFlightJobId())
                .isNotEqualTo(priorInstance);
    }

    /**
     * Materialize one instance of a due interval task carrying {@code maxAttempts},
     * claim it (so the store stamps the claim-time attempt increment the budget
     * test reads), and fail it at {@code failedAt}. Returns the instance id, with
     * the task left due again so the next tick exercises the pile-up guard.
     */
    private UUID failInstanceOfTaskWithBudget(String task, int maxAttempts, Instant failedAt) {
        scheduler.defineIntervalTask(
                task,
                Duration.ofMillis(100),
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                null,
                maxAttempts,
                CronTask.MissedRunPolicy.DROP);
        var initial = store.findCronTaskState(task).orElseThrow();
        store.upsertCronTaskState(new CronTaskScheduleState(
                initial.taskName(), null, null, Instant.now().minusSeconds(1), null));
        var materializer = new RecurringMaterializer(store);
        materializer.tick(Instant.now());
        var state = store.findCronTaskState(task).orElseThrow();

        // Claim through the store so attempts carries the real claim-time
        // increment rather than a hand-set value.
        Job claimed = store.claimReady(NodeId.newId(), "default", 10, Instant.now()).stream()
                .filter(j -> j.id().asUuid().equals(state.inFlightJobId()))
                .findFirst()
                .orElseThrow();
        assertThat(claimed.attempts()).isEqualTo(1);
        long version = claimed.version();
        claimed.transitionTo(JobState.FAILED, failedAt, "test", "boom");
        claimed.clearOwner();
        store.saveAtomic(claimed, version);

        store.upsertCronTaskState(new CronTaskScheduleState(
                task,
                state.lastRunAt(),
                state.lastRunJobId(),
                Instant.now().minusMillis(50),
                state.inFlightJobId(),
                state.timingFingerprint()));
        return state.inFlightJobId();
    }

    @Test
    void catchUpBacklogIsCappedPerTickWithCarryOver() {
        scheduler.defineIntervalTask(
                "burst",
                Duration.ofMillis(100),
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.CATCH_UP);
        var existing = store.findCronTaskState("burst").orElseThrow();
        // ~150 missed intervals — more than one tick's materialization cap.
        store.upsertCronTaskState(new CronTaskScheduleState(
                existing.taskName(), null, null, Instant.now().minus(Duration.ofSeconds(15)), null));

        Instant now = Instant.now();
        new RecurringMaterializer(store).tick(now);

        // One tick materializes at most the per-tick cap; the remainder
        // carries over via nextRunAt, which stays in the past.
        List<Job> instances = store.findByHandlerSignature(RecorderHandler.class.getName(), 1_000);
        assertThat(instances).hasSize(100);
        var state = store.findCronTaskState("burst").orElseThrow();
        assertThat(state.nextRunAt()).isBefore(now);
    }

    @Test
    void recurringInstancesCarryTheirCronTaskName() {
        scheduler.defineIntervalTask("linked", Duration.ofMillis(100), new HelloPayload("tick"), RecorderHandler.class);
        var existing = store.findCronTaskState("linked").orElseThrow();
        store.upsertCronTaskState(new CronTaskScheduleState(
                existing.taskName(), null, null, Instant.now().minusSeconds(1), null));

        new RecurringMaterializer(store).tick(Instant.now());

        var state = store.findCronTaskState("linked").orElseThrow();
        Job instance = store.findById(JobId.of(state.lastRunJobId())).orElseThrow();
        // The materialized instance must back-link to its recurring definition.
        assertThat(instance.cronTaskName()).contains("linked");
    }

    @Test
    void recurringInstancesCarryTheTaskTimeoutAsPerJobTimeoutMetadata() {
        // Regression: a recurring definition's per-instance overrides (timeout,
        // max attempts) used to be dropped on materialization, so every
        // recurring instance silently ran under the engine defaults
        // (github issue #84).
        scheduler.defineIntervalTask(
                "timed",
                Duration.ofMillis(100),
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                Duration.ofMinutes(30),
                7,
                CronTask.MissedRunPolicy.DROP);
        var existing = store.findCronTaskState("timed").orElseThrow();
        store.upsertCronTaskState(new CronTaskScheduleState(
                existing.taskName(), null, null, Instant.now().minusSeconds(1), null));

        new RecurringMaterializer(store).tick(Instant.now());

        var state = store.findCronTaskState("timed").orElseThrow();
        Job instance = store.findById(JobId.of(state.lastRunJobId())).orElseThrow();
        assertThat(instance.metadata().get(JobRunner.META_TIMEOUT_SECONDS)).contains("1800");
        assertThat(instance.metadata().get(RetryInterceptor.META_MAX_ATTEMPTS)).contains("7");
    }

    @Test
    void recurringInstancesWithoutATaskTimeoutCarryNoTimeoutMetadata() {
        scheduler.defineIntervalTask(
                "untimed", Duration.ofMillis(100), new HelloPayload("tick"), RecorderHandler.class);
        var existing = store.findCronTaskState("untimed").orElseThrow();
        store.upsertCronTaskState(new CronTaskScheduleState(
                existing.taskName(), null, null, Instant.now().minusSeconds(1), null));

        new RecurringMaterializer(store).tick(Instant.now());

        var state = store.findCronTaskState("untimed").orElseThrow();
        Job instance = store.findById(JobId.of(state.lastRunJobId())).orElseThrow();
        // Absent metadata means "use the engine defaults".
        assertThat(instance.metadata().get(JobRunner.META_TIMEOUT_SECONDS)).isEmpty();
        assertThat(instance.metadata().get(RetryInterceptor.META_MAX_ATTEMPTS)).isEmpty();
    }

    @Test
    void systemLaneIsNotStarvedByAdHocFlood() {
        // Reserved capacity on the system lane prevents starvation.
        // Enqueue a flood of slow ad-hoc jobs, then a handful of system jobs.
        for (int i = 0; i < 200; i++) {
            scheduler.enqueue(new HelloPayload("ad" + i), AdHocHandler.class, "default", 0);
        }
        for (int i = 0; i < 5; i++) {
            scheduler.enqueue(new HelloPayload("sys" + i), SystemHandler.class, Scheduler.SYSTEM_QUEUE, 0);
        }

        node = ProcessingNode.builder(store)
                .config(fastConfig().toBuilder().workerCount(4).build())
                .lane(new QueueLane("default", 4))
                .lane(new QueueLane(Scheduler.SYSTEM_QUEUE, 2))
                .build();
        node.start();

        // The 5 system jobs should complete promptly, even though the default lane is saturated.
        await().atMost(Duration.ofSeconds(5)).until(() -> SystemHandler.RECORD.size() == 5);
    }

    @Test
    void higherPriorityWithinAQueueIsClaimedFirst() {
        // Insert two low-priority jobs, then one high-priority job AFTER them.
        // The high-priority job should be observed FIRST in the records.
        scheduler.enqueue(new HelloPayload("low-a"), PriorityHandler.class, "default", 0);
        scheduler.enqueue(new HelloPayload("low-b"), PriorityHandler.class, "default", 0);
        scheduler.enqueue(new HelloPayload("high"), PriorityHandler.class, "default", 9);

        node = ProcessingNode.builder(store)
                .config(fastConfig().toBuilder()
                        .workerCount(1)
                        .claimBatchSize(1)
                        .build())
                .build();
        node.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> PriorityHandler.RECORD.size() == 3);

        List<String> order = List.copyOf(PriorityHandler.RECORD);
        assertThat(order.get(0)).isEqualTo("high");
    }

    @Test
    void schedulerRestartDoesNotDoubleEnqueueACronTask() {
        // Audit §6.4 — when the maintenance lease handover gives mastership
        // to a new node mid-tick, the new master's RecurringMaterializer must
        // observe the previous master's in_flight_job_id and not enqueue a
        // second instance for the same fire. The pile-up guard in
        // RecurringMaterializer.tickOne is what makes this safe.
        scheduler.defineIntervalTask(
                "no-double-enqueue",
                Duration.ofSeconds(30),
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.DROP);
        // Backdate next_run_at so the materializer treats the task as due.
        var existing = store.findCronTaskState("no-double-enqueue").orElseThrow();
        store.upsertCronTaskState(new CronTaskScheduleState(
                existing.taskName(), null, null, Instant.now().minusSeconds(1), null));

        var materializer = new RecurringMaterializer(store);
        materializer.tick(Instant.now());
        long countAfterFirst = store.countsByState().getOrDefault(JobState.ENQUEUED, 0L);
        assertThat(countAfterFirst).isEqualTo(1);

        // Simulate master handover by constructing a fresh materializer and
        // ticking again with the previous instance still un-terminal.
        var freshMaster = new RecurringMaterializer(store);
        freshMaster.tick(Instant.now());
        long countAfterHandover = store.countsByState().getOrDefault(JobState.ENQUEUED, 0L);
        assertThat(countAfterHandover)
                .as("pile-up guard must block duplicate enqueue")
                .isEqualTo(1);
    }

    // -------- on-demand nudge (issue #108) --------

    @Test
    void nudgeMaterializesPromptlyWithoutTouchingTheSchedule() {
        scheduler.defineIntervalTask(
                "nudged-idle",
                Duration.ofHours(6),
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.DROP);
        Instant scheduledNext =
                store.findCronTaskState("nudged-idle").orElseThrow().nextRunAt();

        scheduler.nudgeRecurring("nudged-idle");
        new RecurringMaterializer(store).tick(Instant.now());

        List<Job> instances = store.findByHandlerSignature(RecorderHandler.class.getName(), 10);
        assertThat(instances).hasSize(1);
        Job instance = instances.get(0);
        assertThat(instance.currentState()).isEqualTo(JobState.ENQUEUED);
        assertThat(instance.metadata().get(JobExecutionContext.CRON_ORIGIN_META))
                .contains(JobExecutionContext.CRON_ORIGIN_NUDGE);
        assertThat(instance.metadata().get(JobExecutionContext.CRON_FIRE_TIME_META))
                .as("a nudge represents no schedule tick, so it carries no nominal fire time")
                .isEmpty();

        var after = store.findCronTaskState("nudged-idle").orElseThrow();
        assertThat(after.nextRunAt())
                .as("a nudge never moves the schedule — the interval phase is preserved")
                .isEqualTo(scheduledNext);
        assertThat(after.nudgeRequestedAt()).isNull();
        assertThat(after.inFlightJobId())
                .as("the nudged instance takes the pile-up guard like any other instance")
                .isEqualTo(instance.id().asUuid());
    }

    @Test
    void nudgeDuringAnInFlightRunProducesExactlyOneFollowUpAfterCompletion() {
        scheduler.defineIntervalTask(
                "nudged-inflight",
                Duration.ofHours(6),
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.DROP);
        scheduler.nudgeRecurring("nudged-inflight");
        var materializer = new RecurringMaterializer(store);
        materializer.tick(Instant.now());
        List<Job> first = store.findByHandlerSignature(RecorderHandler.class.getName(), 10);
        assertThat(first).hasSize(1);

        // A burst of nudges lands while that instance is in flight. The
        // in-flight run may have read its inputs before these nudges'
        // triggering writes committed, so it must NOT satisfy them — but the
        // whole burst collapses into a single follow-up.
        scheduler.nudgeRecurring("nudged-inflight");
        scheduler.nudgeRecurring("nudged-inflight");
        scheduler.nudgeRecurring("nudged-inflight");
        materializer.tick(Instant.now());
        assertThat(store.findByHandlerSignature(RecorderHandler.class.getName(), 10))
                .as("the pile-up guard defers the follow-up while the run is in flight")
                .hasSize(1);

        Job instance = first.get(0);
        long v = instance.version();
        instance.transitionTo(JobState.PROCESSING, Instant.now(), "test", null);
        instance.transitionTo(JobState.SUCCEEDED, Instant.now(), "test", null);
        store.saveAtomic(instance, v);

        materializer.tick(Instant.now());
        List<Job> after = store.findByHandlerSignature(RecorderHandler.class.getName(), 10);
        assertThat(after).as("exactly one follow-up for the whole burst").hasSize(2);
        Job followUp = after.stream()
                .filter(j -> !j.id().equals(instance.id()))
                .findFirst()
                .orElseThrow();
        assertThat(followUp.metadata().get(JobExecutionContext.CRON_ORIGIN_META))
                .contains(JobExecutionContext.CRON_ORIGIN_NUDGE);

        // Once the follow-up terminates, no further instance appears: the
        // burst was fully coalesced.
        long v2 = followUp.version();
        followUp.transitionTo(JobState.PROCESSING, Instant.now(), "test", null);
        followUp.transitionTo(JobState.SUCCEEDED, Instant.now(), "test", null);
        store.saveAtomic(followUp, v2);
        materializer.tick(Instant.now());
        assertThat(store.findByHandlerSignature(RecorderHandler.class.getName(), 10))
                .hasSize(2);
    }

    @Test
    void nudgeCoalescesIntoADueScheduledFire() {
        scheduler.defineIntervalTask(
                "nudged-due",
                Duration.ofSeconds(30),
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.DROP);
        scheduler.nudgeRecurring("nudged-due");
        // Backdate the schedule so the same tick has a due fire AND a pending
        // nudge. The blanket state upsert must leave the nudge cell intact.
        var existing = store.findCronTaskState("nudged-due").orElseThrow();
        store.upsertCronTaskState(new CronTaskScheduleState(
                existing.taskName(), null, null, Instant.now().minusSeconds(1), null, existing.timingFingerprint()));
        assertThat(store.findCronTaskState("nudged-due").orElseThrow().nudgeRequestedAt())
                .isNotNull();

        new RecurringMaterializer(store).tick(Instant.now());

        // One instance total: the scheduled fire was enqueued after the nudge
        // committed, so it satisfies the nudge instead of doubling it.
        List<Job> instances = store.findByHandlerSignature(RecorderHandler.class.getName(), 10);
        assertThat(instances).hasSize(1);
        assertThat(instances.get(0).metadata().get(JobExecutionContext.CRON_ORIGIN_META))
                .contains(JobExecutionContext.CRON_ORIGIN_SCHEDULE);
        assertThat(store.findCronTaskState("nudged-due").orElseThrow().nudgeRequestedAt())
                .isNull();
    }

    @Test
    void nudgeInstanceTakesThePileUpGuardSoAScheduledFireWaits() {
        // "Through the normal machinery" cuts both ways: while a nudged
        // instance runs, a scheduled fire coming due is deferred by the same
        // pile-up guard that protects scheduled instances from each other.
        scheduler.defineIntervalTask(
                "nudged-guard",
                Duration.ofSeconds(30),
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.DROP);
        scheduler.nudgeRecurring("nudged-guard");
        var materializer = new RecurringMaterializer(store);
        materializer.tick(Instant.now());
        List<Job> first = store.findByHandlerSignature(RecorderHandler.class.getName(), 10);
        assertThat(first).hasSize(1);

        var state = store.findCronTaskState("nudged-guard").orElseThrow();
        store.upsertCronTaskState(new CronTaskScheduleState(
                state.taskName(),
                state.lastRunAt(),
                state.lastRunJobId(),
                Instant.now().minusSeconds(1),
                state.inFlightJobId(),
                state.timingFingerprint()));
        materializer.tick(Instant.now());
        assertThat(store.findByHandlerSignature(RecorderHandler.class.getName(), 10))
                .as("the due scheduled fire waits behind the running nudged instance")
                .hasSize(1);

        Job instance = first.get(0);
        long v = instance.version();
        instance.transitionTo(JobState.PROCESSING, Instant.now(), "test", null);
        instance.transitionTo(JobState.SUCCEEDED, Instant.now(), "test", null);
        store.saveAtomic(instance, v);
        materializer.tick(Instant.now());
        List<Job> after = store.findByHandlerSignature(RecorderHandler.class.getName(), 10);
        assertThat(after).hasSize(2);
    }

    @Test
    void nudgeUnknownTaskFailsLoudly() {
        assertThatThrownBy(() -> scheduler.nudgeRecurring("never-registered"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("never-registered");
    }

    @Test
    void nudgeDisabledTaskFailsLoudlyAndDoesNotRun() {
        scheduler.defineIntervalTask(
                "nudged-disabled",
                Duration.ofHours(6),
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.DROP);
        store.upsertCronTask(
                disabledCopyOf(store.findCronTask("nudged-disabled").orElseThrow()));

        assertThatThrownBy(() -> scheduler.nudgeRecurring("nudged-disabled"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("disabled");
        new RecurringMaterializer(store).tick(Instant.now());
        assertThat(store.findByHandlerSignature(RecorderHandler.class.getName(), 10))
                .isEmpty();
    }

    @Test
    void reEnablingATaskClearsAPendingNudgeFromBeforeThePause() {
        // Consistent with re-enable-does-not-catch-up (#106): demand recorded
        // before an explicit pause is stale by the time an operator re-enables
        // the task, so the flip clears it instead of firing a surprise run.
        scheduler.defineIntervalTask(
                "nudged-reenabled",
                Duration.ofHours(6),
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.DROP);
        scheduler.nudgeRecurring("nudged-reenabled");
        store.upsertCronTask(
                disabledCopyOf(store.findCronTask("nudged-reenabled").orElseThrow()));

        scheduler.defineIntervalTask(
                "nudged-reenabled",
                Duration.ofHours(6),
                new HelloPayload("tick"),
                RecorderHandler.class,
                "default",
                0,
                CronTask.MissedRunPolicy.DROP);

        assertThat(store.findCronTaskState("nudged-reenabled").orElseThrow().nudgeRequestedAt())
                .isNull();
        new RecurringMaterializer(store).tick(Instant.now());
        assertThat(store.findByHandlerSignature(RecorderHandler.class.getName(), 10))
                .isEmpty();
    }

    @Test
    void materializerReloadsTheDefinitionUnderTheTaskMutexBeforeActing() {
        // tick() snapshots the task list BEFORE tickOne takes the per-task
        // mutex, so an edit can commit in between. Simulate exactly that
        // interleaving deterministically: the listing returns the stale
        // pre-edit definition while point reads return the committed truth.
        // The materializer must insert the fresh handler, not the stale one.
        scheduler.defineIntervalTask("edited", Duration.ofHours(6), new HelloPayload("tick"), RecorderHandler.class);
        CronTask stale = store.findCronTask("edited").orElseThrow();
        scheduler.nudgeRecurring("edited");
        store.upsertCronTask(new CronTask(
                stale.name(),
                stale.trigger(),
                AdHocHandler.class.getName(),
                stale.payloadArgument(),
                stale.queue(),
                stale.priority(),
                stale.timeout(),
                stale.maxAttempts(),
                stale.missedRunPolicy(),
                stale.zone(),
                true));

        var staleListing = new ForwardingJobStore(store) {
            @Override
            public List<CronTask> listCronTasks() {
                return List.of(stale);
            }
        };
        new RecurringMaterializer(staleListing).tick(Instant.now());

        assertThat(store.findByHandlerSignature(RecorderHandler.class.getName(), 10))
                .as("the stale pre-edit definition must not be materialized")
                .isEmpty();
        assertThat(store.findByHandlerSignature(AdHocHandler.class.getName(), 10))
                .hasSize(1);
        assertThat(store.findCronTaskState("edited").orElseThrow().nudgeRequestedAt())
                .isNull();
    }

    @Test
    void materializerRechecksEnabledUnderTheTaskMutexBeforeActing() {
        // The same list-then-mutex window, for the enabled bit: a disable
        // committing between the listing and the mutex must suppress the
        // materialization even though the listed snapshot says enabled.
        scheduler.defineIntervalTask(
                "raced-disable", Duration.ofHours(6), new HelloPayload("tick"), RecorderHandler.class);
        CronTask enabledSnapshot = store.findCronTask("raced-disable").orElseThrow();
        scheduler.nudgeRecurring("raced-disable");
        store.upsertCronTask(disabledCopyOf(enabledSnapshot));

        var staleListing = new ForwardingJobStore(store) {
            @Override
            public List<CronTask> listCronTasks() {
                return List.of(enabledSnapshot);
            }
        };
        new RecurringMaterializer(staleListing).tick(Instant.now());

        assertThat(store.findByHandlerSignature(RecorderHandler.class.getName(), 10))
                .isEmpty();
    }

    @Test
    void aFailedNudgeClearProducesAnExtraRunNeverALostOne() {
        // The coalescing bound is failure-free by design: job insert, state
        // upsert, and revision clear are independent durable writes. A clear
        // that fails leaves the nudge pending, and recovery materializes
        // again — an extra run, never a lost one.
        scheduler.defineIntervalTask(
                "flaky-clear", Duration.ofHours(6), new HelloPayload("tick"), RecorderHandler.class);
        scheduler.nudgeRecurring("flaky-clear");
        var failOnce = new AtomicBoolean(true);
        var flakyClear = new ForwardingJobStore(store) {
            @Override
            public void clearCronNudge(String taskName, long observedRevision) {
                if (failOnce.compareAndSet(true, false)) {
                    throw new IllegalStateException("store outage during clear");
                }
                super.clearCronNudge(taskName, observedRevision);
            }
        };
        var materializer = new RecurringMaterializer(flakyClear);
        materializer.tick(Instant.now());

        List<Job> first = store.findByHandlerSignature(RecorderHandler.class.getName(), 10);
        assertThat(first).hasSize(1);
        assertThat(store.findCronTaskState("flaky-clear").orElseThrow().nudgeRequestedAt())
                .as("the failed clear leaves the nudge pending")
                .isNotNull();

        // Once the instance terminates, the still-pending nudge produces the
        // extra run and the retried clear succeeds.
        Job instance = first.get(0);
        long v = instance.version();
        instance.transitionTo(JobState.PROCESSING, Instant.now(), "test", null);
        instance.transitionTo(JobState.SUCCEEDED, Instant.now(), "test", null);
        store.saveAtomic(instance, v);
        materializer.tick(Instant.now());

        assertThat(store.findByHandlerSignature(RecorderHandler.class.getName(), 10))
                .hasSize(2);
        assertThat(store.findCronTaskState("flaky-clear").orElseThrow().nudgeRequestedAt())
                .isNull();
    }

    private static CronTask disabledCopyOf(CronTask task) {
        return new CronTask(
                task.name(),
                task.trigger(),
                task.handlerType(),
                task.payloadArgument(),
                task.queue(),
                task.priority(),
                task.timeout(),
                task.maxAttempts(),
                task.missedRunPolicy(),
                task.zone(),
                false);
    }

    @Test
    void queueRoutingIsRespected() {
        scheduler.enqueue(new HelloPayload("d"), RecorderHandler.class, "default", 0);
        scheduler.enqueue(new HelloPayload("h"), RecorderHandler.class, "high", 0);
        node = ProcessingNode.builder(store)
                .config(fastConfig().toBuilder().workerCount(2).build())
                .lane(new QueueLane("default", 2))
                .lane(new QueueLane("high", 2))
                .build();
        node.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> RecorderHandler.RECORD.size() == 2);
    }

    @Test
    void reconcileRecurringForOneNamespaceLeavesOtherNamespaceAndManualTasksUntouched() {
        store.upsertCronTask(cronTaskNamed("task-A"));
        store.upsertCronTask(cronTaskNamed("task-B"));
        store.upsertCronTask(cronTaskNamed("task-M"));
        store.recordCronTaskOwnership("A", "task-A");
        store.recordCronTaskOwnership("B", "task-B");

        scheduler.reconcileRecurring("A", List.of(cronTaskNamed("task-A-new")));

        assertThat(store.findCronTask("task-A")).isEmpty();
        assertThat(store.findCronTask("task-A-new")).isPresent();
        assertThat(store.listCronTaskNamesOwnedBy("A")).containsExactly("task-A-new");

        assertThat(store.findCronTask("task-B")).isPresent();
        assertThat(store.listCronTaskNamesOwnedBy("B")).containsExactly("task-B");

        assertThat(store.findCronTask("task-M")).isPresent();
    }

    @Test
    void reconcileRecurringWithEmptyDesiredSetDeletesAllTasksOwnedByThatNamespace() {
        store.upsertCronTask(cronTaskNamed("a1"));
        store.upsertCronTask(cronTaskNamed("a2"));
        store.upsertCronTask(cronTaskNamed("unowned"));
        store.recordCronTaskOwnership("A", "a1");
        store.recordCronTaskOwnership("A", "a2");

        scheduler.reconcileRecurring("A", List.of());

        assertThat(store.findCronTask("a1")).isEmpty();
        assertThat(store.findCronTask("a2")).isEmpty();
        assertThat(store.listCronTaskNamesOwnedBy("A")).isEmpty();
        assertThat(store.findCronTask("unowned")).isPresent();
    }

    private static CronTask cronTaskNamed(String name) {
        return new CronTask(
                name,
                new CronTask.Trigger.CronExpr(CronExpression.parse("* * * * *")),
                "com.example.Handler",
                new JobArgument("com.example.Payload", "{}"),
                "default",
                0,
                CronTask.MissedRunPolicy.DROP,
                ZoneId.of("UTC"),
                true);
    }

    private ProcessingNodeConfig fastConfig() {
        return ProcessingNodeConfig.builder()
                .workerCount(4)
                .pollInterval(Duration.ofMillis(30))
                .claimHeartbeat(Duration.ofMillis(60))
                .heartbeatTimeout(Duration.ofSeconds(2))
                .jobTimeout(Duration.ofSeconds(2))
                .defaultMaxAttempts(2)
                .retryInitialBackoff(Duration.ofMillis(50))
                .storeOutagePollInterval(Duration.ofMillis(100))
                .claimBatchSize(20)
                .build();
    }
}
