package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.engine.QueueLane;
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
