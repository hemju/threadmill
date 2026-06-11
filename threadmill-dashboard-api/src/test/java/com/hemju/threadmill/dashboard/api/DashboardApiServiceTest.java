package com.hemju.threadmill.dashboard.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.schedule.RecurringMaterializer;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.store.JobSearch;
import com.hemju.threadmill.core.store.JobStoreCapabilities;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

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
