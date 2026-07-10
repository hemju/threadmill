package com.hemju.threadmill.dashboard.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.JobRunner;
import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.engine.RetryInterceptor;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.schedule.RecurringMaterializer;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobSearch;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.core.store.JobStoreCapabilities;
import com.hemju.threadmill.core.store.NodeHeartbeat;
import com.hemju.threadmill.store.memory.InMemoryJobStore;
import com.hemju.threadmill.test.ForwardingJobStore;

class DashboardApiServiceTest {

    @Test
    void limitedSearchCapabilitiesFailWithDashboardException() {
        var store = new InMemoryJobStore(
                new JsonJobSerializer(),
                new JobStoreCapabilities(
                        JobStoreCapabilities.DEFAULT_MAX_SERIALIZED_BYTES,
                        JobStoreCapabilities.DEFAULT_MAX_JOB_LOG_BYTES,
                        JobStoreCapabilities.DEFAULT_MAX_FAILURE_METADATA_BYTES,
                        1000,
                        false,
                        true,
                        true,
                        true));
        var service = new DashboardApiService(store, new LocalWakeBus());

        assertThatThrownBy(() -> service.jobs(JobSearch.all()))
                .isInstanceOf(DashboardApiException.class)
                .satisfies(error -> assertThat(((DashboardApiException) error).code())
                        .isEqualTo(DashboardApiException.Code.BAD_REQUEST));
        assertThatThrownBy(() -> service.jobs(new JobSearch(JobState.ENQUEUED, "default", null, 50, 0)))
                .isInstanceOf(DashboardApiException.class)
                .satisfies(error -> assertThat(((DashboardApiException) error).code())
                        .isEqualTo(DashboardApiException.Code.BAD_REQUEST));
    }

    @Test
    void manualTriggerDoesNotStealThePileUpGuardFromARunningScheduledInstance() {
        var store = new InMemoryJobStore();
        var service = new DashboardApiService(store, new LocalWakeBus());
        var task = new CronTask(
                "report",
                new CronTask.Trigger.Interval(Duration.ofMinutes(5)),
                "com.example.ReportHandler",
                new JobArgument("com.hemju.threadmill.core.handler.NoPayload", "{}"),
                "default",
                0,
                CronTask.MissedRunPolicy.DROP,
                ZoneId.of("UTC"),
                true);
        store.upsertCronTask(task);

        // A scheduled instance materialized earlier is still PROCESSING.
        var materializer = new RecurringMaterializer(store);
        store.upsertCronTaskState(
                CronTaskScheduleState.initial("report", Instant.now().minusSeconds(60)));
        materializer.tick(Instant.now());
        var scheduledInstance =
                store.claimReady(NodeId.newId(), "default", 1, Instant.now()).getFirst();
        assertThat(scheduledInstance.currentState()).isEqualTo(JobState.PROCESSING);
        var guarded = store.findCronTaskState("report").orElseThrow();
        assertThat(guarded.inFlightJobId()).isEqualTo(scheduledInstance.id().asUuid());

        // Manual dashboard trigger: the job is enqueued, but the pile-up
        // guard stays with the still-running scheduled instance.
        service.triggerRecurring("report");
        var afterTrigger = store.findCronTaskState("report").orElseThrow();
        assertThat(afterTrigger.inFlightJobId())
                .isEqualTo(scheduledInstance.id().asUuid());
        assertThat(afterTrigger.lastRunJobId())
                .isNotEqualTo(scheduledInstance.id().asUuid());

        // The materializer must still refuse the next materialization while
        // the scheduled instance runs — a fire is due, but nothing new appears.
        store.upsertCronTaskState(new CronTaskScheduleState(
                "report",
                afterTrigger.lastRunAt(),
                afterTrigger.lastRunJobId(),
                Instant.now().minusSeconds(1),
                afterTrigger.inFlightJobId()));
        materializer.tick(Instant.now());
        assertThat(store.findByHandlerSignature("com.example.ReportHandler", 10))
                .hasSize(2); // scheduled instance + manual trigger, nothing else
    }

    @Test
    void manualTriggerTakesTheGuardWhenNoPriorInstanceIsRunning() {
        var store = new InMemoryJobStore();
        var service = new DashboardApiService(store, new LocalWakeBus());
        store.upsertCronTask(new CronTask(
                "report",
                new CronTask.Trigger.Interval(Duration.ofMinutes(5)),
                "com.example.ReportHandler",
                new JobArgument("com.hemju.threadmill.core.handler.NoPayload", "{}"),
                "default",
                0,
                CronTask.MissedRunPolicy.DROP,
                ZoneId.of("UTC"),
                true));

        service.triggerRecurring("report");

        var state = store.findCronTaskState("report").orElseThrow();
        assertThat(state.inFlightJobId()).isEqualTo(state.lastRunJobId());
        assertThat(state.inFlightJobId()).isNotNull();
    }

    @Test
    void manualTriggerCarriesTheTaskTimeoutAsPerJobTimeoutMetadata() {
        // Companion to github issue #84: a manually triggered instance of a
        // recurring task must run under the task's timeout and retry budget,
        // like a scheduled one.
        var store = new InMemoryJobStore();
        var service = new DashboardApiService(store, new LocalWakeBus());
        store.upsertCronTask(timedCronTask("report", Duration.ofMinutes(30), 7));

        service.triggerRecurring("report");

        var state = store.findCronTaskState("report").orElseThrow();
        var instance = store.findById(JobId.of(state.lastRunJobId())).orElseThrow();
        assertThat(instance.metadata().get(JobRunner.META_TIMEOUT_SECONDS)).contains("1800");
        assertThat(instance.metadata().get(RetryInterceptor.META_MAX_ATTEMPTS)).contains("7");
    }

    @Test
    void updateRecurringPreservesTheTaskTimeout() {
        // The update endpoint rebuilds the CronTask field-by-field; the
        // timeout and retry budget are not operator-editable yet and must
        // survive unchanged.
        var store = new InMemoryJobStore();
        var service = new DashboardApiService(store, new LocalWakeBus());
        store.upsertCronTask(timedCronTask("report", Duration.ofMinutes(30), 9));

        service.updateRecurring(
                "report",
                new DashboardPayloads.UpdateRecurringRequest(null, null, null, null, null, 7, null, null, null));

        var updated = store.findCronTask("report").orElseThrow();
        assertThat(updated.priority()).isEqualTo(7);
        assertThat(updated.timeout()).isEqualTo(Duration.ofMinutes(30));
        assertThat(updated.maxAttempts()).isEqualTo(9);
    }

    @Test
    void recurringDeletionReturnsConflictInsteadOfProceedingWithoutTheTaskMutex() {
        var store = new InMemoryJobStore();
        store.upsertCronTask(timedCronTask("report", Duration.ofMinutes(30), 9));
        store.upsertCronTaskState(CronTaskScheduleState.initial("report", Instant.now()));
        assertThat(store.tryAcquireMutex(
                        RecurringMaterializer.taskMutexName("report"), "materializer", Duration.ofMinutes(1)))
                .isTrue();
        var service = new DashboardApiService(store, new LocalWakeBus());

        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> service.deleteRecurring("report"))
                    .isInstanceOf(DashboardApiException.class)
                    .satisfies(error -> assertThat(((DashboardApiException) error).code())
                            .isEqualTo(DashboardApiException.Code.CONFLICT));
        } finally {
            Thread.interrupted();
        }

        assertThat(store.findCronTask("report")).isPresent();
        assertThat(store.findCronTaskState("report")).isPresent();
    }

    private static CronTask timedCronTask(String name, Duration timeout, Integer maxAttempts) {
        return new CronTask(
                name,
                new CronTask.Trigger.Interval(Duration.ofMinutes(5)),
                "com.example.ReportHandler",
                new JobArgument("com.hemju.threadmill.core.handler.NoPayload", "{}"),
                "default",
                0,
                timeout,
                maxAttempts,
                CronTask.MissedRunPolicy.DROP,
                ZoneId.of("UTC"),
                true);
    }

    @Test
    void nodesReadDoesNotBuildAFullEngineSnapshot() {
        var store = new CountingJobStore(new InMemoryJobStore());
        var service = new DashboardApiService(store, new LocalWakeBus());

        service.nodeHeartbeats();

        assertThat(store.nodeHeartbeatReads.get()).isEqualTo(1);
        assertThat(store.queueDepthReads.get()).isZero();
        assertThat(store.oldestEnqueuedReads.get()).isZero();
        assertThat(store.cronStateReads.get()).isZero();
    }

    @Test
    void snapshotCacheCoalescesDashboardPollsAndMutationsInvalidateIt() {
        var inner = new InMemoryJobStore();
        seedQueues(inner);
        seedCronTask(inner, "report-a");
        seedCronTask(inner, "report-b");
        var store = new CountingJobStore(inner);
        var service = new DashboardApiService(store, new LocalWakeBus(), Duration.ofMinutes(5));

        service.overview(false);
        service.queues();
        service.recurringTasks(false);
        service.overview(false);

        // One snapshot refresh serves all four reads: one queue-depth pass,
        // one oldest-enqueued probe per queue, one state lookup per task.
        assertThat(store.queueDepthReads.get()).isEqualTo(1);
        assertThat(store.oldestEnqueuedReads.get()).isEqualTo(2);
        assertThat(store.cronStateReads.get()).isEqualTo(2);

        // A dashboard mutation drops the cache so the operator sees it.
        service.pauseQueue("alpha", null);
        var queues = service.queues();
        assertThat(store.queueDepthReads.get()).isEqualTo(2);
        assertThat(queues.stream().filter(view -> view.queue().equals("alpha")).findFirst())
                .hasValueSatisfying(view -> assertThat(view.paused()).isTrue());
    }

    @Test
    void searchOffsetBeyondTheCapIsABadRequest() {
        var service = new DashboardApiService(new InMemoryJobStore(), new LocalWakeBus());

        assertThatThrownBy(() -> service.jobs(
                        new JobSearch(JobState.ENQUEUED, null, null, 50, DashboardApiService.MAX_SEARCH_OFFSET + 1)))
                .isInstanceOf(DashboardApiException.class)
                .satisfies(error -> assertThat(((DashboardApiException) error).code())
                        .isEqualTo(DashboardApiException.Code.BAD_REQUEST));
    }

    private static void seedQueues(InMemoryJobStore store) {
        store.insert(Job.builder()
                .spec(JobSpec.of("com.example.Handler"))
                .queue("alpha")
                .build());
        store.insert(Job.builder()
                .spec(JobSpec.of("com.example.Handler"))
                .queue("beta")
                .build());
    }

    private static void seedCronTask(InMemoryJobStore store, String name) {
        store.upsertCronTask(new CronTask(
                name,
                new CronTask.Trigger.Interval(Duration.ofMinutes(5)),
                "com.example.ReportHandler",
                new JobArgument("com.hemju.threadmill.core.handler.NoPayload", "{}"),
                "default",
                0,
                CronTask.MissedRunPolicy.DROP,
                ZoneId.of("UTC"),
                true));
        store.upsertCronTaskState(
                CronTaskScheduleState.initial(name, Instant.now().plusSeconds(300)));
    }

    private static final class CountingJobStore extends ForwardingJobStore {
        final AtomicInteger queueDepthReads = new AtomicInteger();
        final AtomicInteger oldestEnqueuedReads = new AtomicInteger();
        final AtomicInteger cronStateReads = new AtomicInteger();
        final AtomicInteger nodeHeartbeatReads = new AtomicInteger();

        CountingJobStore(JobStore delegate) {
            super(delegate);
        }

        @Override
        public Map<String, Long> queueDepths() {
            queueDepthReads.incrementAndGet();
            return super.queueDepths();
        }

        @Override
        public Optional<Instant> oldestEnqueuedAt(String queue) {
            oldestEnqueuedReads.incrementAndGet();
            return super.oldestEnqueuedAt(queue);
        }

        @Override
        public Optional<CronTaskScheduleState> findCronTaskState(String name) {
            cronStateReads.incrementAndGet();
            return super.findCronTaskState(name);
        }

        @Override
        public List<NodeHeartbeat> listNodeHeartbeats() {
            nodeHeartbeatReads.incrementAndGet();
            return super.listNodeHeartbeats();
        }
    }

    @Test
    void validationFailuresStayFrameworkNeutral() {
        var service = new DashboardApiService(new InMemoryJobStore(), new LocalWakeBus());

        assertThatThrownBy(() -> service.pauseQueue("default", "x".repeat(257)))
                .isInstanceOf(DashboardApiException.class)
                .satisfies(error -> assertThat(((DashboardApiException) error).code())
                        .isEqualTo(DashboardApiException.Code.BAD_REQUEST));
        assertThatThrownBy(() -> service.scheduleRetry(JobId.newId(), 1, Duration.ofSeconds(-1)))
                .isInstanceOf(DashboardApiException.class)
                .satisfies(error -> assertThat(((DashboardApiException) error).code())
                        .isEqualTo(DashboardApiException.Code.BAD_REQUEST));
    }
}
