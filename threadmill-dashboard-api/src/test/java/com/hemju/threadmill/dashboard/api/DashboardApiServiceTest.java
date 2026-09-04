package com.hemju.threadmill.dashboard.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.JobRunner;
import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.engine.RetryInterceptor;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.schedule.RecurringMaterializer;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.ForwardingJobStore;
import com.hemju.threadmill.core.store.JobSearch;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.core.store.JobStore.NudgeOutcome;
import com.hemju.threadmill.core.store.JobStoreCapabilities;
import com.hemju.threadmill.core.store.NodeHeartbeat;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.ReplaceJobRequest;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.UpdateRecurringRequest;
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
            true,
            JobStoreCapabilities.DEFAULT_MAX_METADATA_BYTES,
            JobStoreCapabilities.DEFAULT_MAX_STATE_HISTORY_ENTRIES));
    var service = new DashboardApiService(
        store, new LocalWakeBus(), DashboardJobDefinitionValidator.denyAll());

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
    var service = new DashboardApiService(
        store, new LocalWakeBus(), DashboardJobDefinitionValidator.denyAll());
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
    store.upsertCronTaskState(CronTaskScheduleState.initial(
        "report", Instant.now().minusSeconds(60), CronTaskScheduleState.timingFingerprintOf(task)));
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
    assertThat(afterTrigger.inFlightJobId()).isEqualTo(scheduledInstance.id().asUuid());
    assertThat(afterTrigger.lastRunJobId()).isNotEqualTo(scheduledInstance.id().asUuid());

    // The materializer must still refuse the next materialization while
    // the scheduled instance runs — a fire is due, but nothing new appears.
    store.upsertCronTaskState(new CronTaskScheduleState(
        "report",
        afterTrigger.lastRunAt(),
        afterTrigger.lastRunJobId(),
        Instant.now().minusSeconds(1),
        afterTrigger.inFlightJobId(),
        afterTrigger.timingFingerprint()));
    materializer.tick(Instant.now());
    assertThat(store.findByHandlerSignature("com.example.ReportHandler", 10))
        .hasSize(2); // scheduled instance + manual trigger, nothing else
  }

  @Test
  void manualTriggerTakesTheGuardWhenNoPriorInstanceIsRunning() {
    var store = new InMemoryJobStore();
    var service = new DashboardApiService(
        store, new LocalWakeBus(), DashboardJobDefinitionValidator.denyAll());
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
  void manualTriggerStampsTheManualOriginMarker() {
    // Issue #108 observability: schedule-, nudge-, and operator-triggered
    // instances must be distinguishable. The dashboard's force lane
    // stamps `manual`.
    var store = new InMemoryJobStore();
    var service = new DashboardApiService(
        store, new LocalWakeBus(), DashboardJobDefinitionValidator.denyAll());
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

    var instances = store.findByHandlerSignature("com.example.ReportHandler", 10);
    assertThat(instances).hasSize(1);
    assertThat(instances.get(0).metadata().get(JobExecutionContext.CRON_ORIGIN_META))
        .contains(JobExecutionContext.CRON_ORIGIN_MANUAL);
  }

  @Test
  void updateRecurringEnabledFlipClearsAPendingNudge() {
    // Disabling wins over a nudge: an operator pausing a task expects it
    // to stay quiet, not to fire recorded-but-unserved demand — and
    // re-enabling must not fire demand from before the pause either.
    var store = new InMemoryJobStore();
    var service = new DashboardApiService(
        store, new LocalWakeBus(), DashboardJobDefinitionValidator.denyAll());
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
    store.upsertCronTaskState(CronTaskScheduleState.initial(
        "report",
        Instant.now().plus(Duration.ofMinutes(5)),
        CronTaskScheduleState.timingFingerprintOf(task)));
    assertThat(store.requestCronNudge("report", Instant.now())).isEqualTo(NudgeOutcome.ACCEPTED);

    service.updateRecurring(
        "report",
        new UpdateRecurringRequest(null, null, null, null, null, null, null, null, false));

    assertThat(store.findCronTaskState("report").orElseThrow().nudgeRequestedAt())
        .isNull();
  }

  @Test
  void redactedJobSummariesStillCarryTheCronOrigin() {
    // Read-level dashboard users see redacted summaries; the origin is a
    // closed three-value set carrying no payload data, so it stays
    // visible — otherwise nudged and scheduled runs would be
    // indistinguishable exactly where operators look for them.
    var store = new InMemoryJobStore();
    var service = new DashboardApiService(
        store, new LocalWakeBus(), DashboardJobDefinitionValidator.denyAll());
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

    var jobs = service.jobs(JobSearch.all()).jobs();
    assertThat(jobs).hasSize(1);
    assertThat(jobs.get(0).detailsRedacted()).isTrue();
    assertThat(jobs.get(0).cronOrigin()).isEqualTo(JobExecutionContext.CRON_ORIGIN_MANUAL);
  }

  @Test
  void updateRecurringWithoutAnEnabledFlipLeavesAPendingNudgeIntact() {
    // The clear belongs to the enabled-flip branches only. An ordinary
    // edit — a priority change here — must not discard demand a producer
    // already recorded; that would silently lose the follow-up run.
    var store = new InMemoryJobStore();
    var service = new DashboardApiService(
        store, new LocalWakeBus(), DashboardJobDefinitionValidator.denyAll());
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
    store.upsertCronTaskState(CronTaskScheduleState.initial(
        "report",
        Instant.now().plus(Duration.ofMinutes(5)),
        CronTaskScheduleState.timingFingerprintOf(task)));
    assertThat(store.requestCronNudge("report", Instant.now())).isEqualTo(NudgeOutcome.ACCEPTED);

    service.updateRecurring(
        "report", new UpdateRecurringRequest(null, null, null, null, null, 7, null, null, null));

    assertThat(store.findCronTaskState("report").orElseThrow().nudgeRequestedAt())
        .as("a non-flip edit must not clear a pending nudge")
        .isNotNull();
  }

  @Test
  void manualTriggerCarriesTheTaskTimeoutAsPerJobTimeoutMetadata() {
    // Companion to github issue #84: a manually triggered instance of a
    // recurring task must run under the task's timeout and retry budget,
    // like a scheduled one.
    var store = new InMemoryJobStore();
    var service = new DashboardApiService(
        store, new LocalWakeBus(), DashboardJobDefinitionValidator.denyAll());
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
    var service = new DashboardApiService(
        store, new LocalWakeBus(), DashboardJobDefinitionValidator.denyAll());
    store.upsertCronTask(timedCronTask("report", Duration.ofMinutes(30), 9));

    service.updateRecurring(
        "report",
        new DashboardPayloads.UpdateRecurringRequest(
            null, null, null, null, null, 7, null, null, null));

    var updated = store.findCronTask("report").orElseThrow();
    assertThat(updated.priority()).isEqualTo(7);
    assertThat(updated.timeout()).isEqualTo(Duration.ofMinutes(30));
    assertThat(updated.maxAttempts()).isEqualTo(9);
  }

  @Test
  void recurringDeletionReturnsConflictInsteadOfProceedingWithoutTheTaskMutex() {
    var store = new InMemoryJobStore();
    var task = timedCronTask("report", Duration.ofMinutes(30), 9);
    store.upsertCronTask(task);
    store.upsertCronTaskState(CronTaskScheduleState.initial(
        "report", Instant.now(), CronTaskScheduleState.timingFingerprintOf(task)));
    assertThat(store.tryAcquireMutex(
            RecurringMaterializer.taskMutexName("report"), "materializer", Duration.ofMinutes(1)))
        .isTrue();
    var service = new DashboardApiService(
        store, new LocalWakeBus(), DashboardJobDefinitionValidator.denyAll());

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

  @Test
  void fullySpecifiedRecurringDefinitionIsValidatedBeforeMutexContention() {
    var store = new InMemoryJobStore();
    var original = timedCronTask("report", Duration.ofMinutes(30), 9);
    store.upsertCronTask(original);
    assertThat(store.tryAcquireMutex(
            RecurringMaterializer.taskMutexName("report"), "materializer", Duration.ofMinutes(1)))
        .isTrue();
    var service = new DashboardApiService(store, new LocalWakeBus(), replacement -> {
      throw DashboardApiException.badRequest("definition rejected");
    });

    Thread.currentThread().interrupt();
    try {
      assertThatThrownBy(() -> service.updateRecurring(
              "report",
              new UpdateRecurringRequest(
                  null,
                  null,
                  "example.ReplacementHandler",
                  new JobArgument("example.ReplacementPayload", "{}"),
                  null,
                  null,
                  null,
                  null,
                  null)))
          .isInstanceOf(DashboardApiException.class)
          .satisfies(error -> {
            var failure = (DashboardApiException) error;
            assertThat(failure.code()).isEqualTo(DashboardApiException.Code.BAD_REQUEST);
            assertThat(failure.getMessage()).isEqualTo("definition rejected");
          });
    } finally {
      Thread.interrupted();
      store.releaseMutex(RecurringMaterializer.taskMutexName("report"), "materializer");
    }

    assertThat(store.findCronTask("report")).contains(original);
  }

  @Test
  void updateRecurringPreservesTheExclusiveFlag() {
    // Same trap as the timeout and retry budget before it: updateRecurring
    // rebuilds the CronTask field-by-field, so a field it forgets is
    // silently dropped. Dropping this one reverts an exclusive task to
    // overlapping execution the next time an operator edits anything.
    var store = new InMemoryJobStore();
    var service = new DashboardApiService(
        store, new LocalWakeBus(), DashboardJobDefinitionValidator.denyAll());
    store.upsertCronTask(exclusiveCronTask("sweep"));

    service.updateRecurring(
        "sweep",
        new DashboardPayloads.UpdateRecurringRequest(
            null, null, null, null, null, 7, null, null, null));

    var updated = store.findCronTask("sweep").orElseThrow();
    assertThat(updated.priority()).isEqualTo(7);
    assertThat(updated.exclusive()).isTrue();
  }

  @Test
  void manualTriggerOfAnExclusiveTaskCarriesTheDerivedConcurrencyKey() {
    // The operator's "run now" must serialize with the scheduled
    // instances rather than overlap them; the pile-up guard cannot do
    // that, only claim-time admission under the same key can.
    var store = new InMemoryJobStore();
    var service = new DashboardApiService(
        store, new LocalWakeBus(), DashboardJobDefinitionValidator.denyAll());
    var task = exclusiveCronTask("sweep");
    store.upsertCronTask(task);
    store.upsertCronTaskState(CronTaskScheduleState.initial(
        "sweep", Instant.now(), CronTaskScheduleState.timingFingerprintOf(task)));

    var response = service.triggerRecurring("sweep");

    assertThat(response.status()).isEqualTo("triggered");
    var triggered = store
        .findById(JobId.of(store.findCronTaskState("sweep").orElseThrow().inFlightJobId()))
        .orElseThrow();
    assertThat(triggered.concurrencyKey()).contains("recurring:sweep");
    assertThat(triggered.concurrencyMode()).contains(ConcurrencyMode.EXCLUSIVE);
  }

  @Test
  void manualTriggerOfANonExclusiveTaskCarriesNoConcurrencyKey() {
    var store = new InMemoryJobStore();
    var service = new DashboardApiService(
        store, new LocalWakeBus(), DashboardJobDefinitionValidator.denyAll());
    var task = timedCronTask("report", null, null);
    store.upsertCronTask(task);
    store.upsertCronTaskState(CronTaskScheduleState.initial(
        "report", Instant.now(), CronTaskScheduleState.timingFingerprintOf(task)));

    service.triggerRecurring("report");

    var triggered = store
        .findById(JobId.of(store.findCronTaskState("report").orElseThrow().inFlightJobId()))
        .orElseThrow();
    assertThat(triggered.concurrencyKey()).isEmpty();
  }

  private static CronTask exclusiveCronTask(String name) {
    return new CronTask(
        name,
        new CronTask.Trigger.Interval(Duration.ofMinutes(5)),
        "com.example.ReportHandler",
        new JobArgument("com.hemju.threadmill.core.handler.NoPayload", "{}"),
        "default",
        0,
        null,
        null,
        true,
        CronTask.MissedRunPolicy.DROP,
        ZoneId.of("UTC"),
        true);
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
    var service = new DashboardApiService(
        store, new LocalWakeBus(), DashboardJobDefinitionValidator.denyAll());

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
    var service = new DashboardApiService(
        store,
        new LocalWakeBus(),
        DashboardJobDefinitionValidator.denyAll(),
        Duration.ofMinutes(5));

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
  void definitionValidatorCanBeCombinedWithACustomSnapshotCacheTtl() {
    var inner = new InMemoryJobStore();
    seedQueues(inner);
    var store = new CountingJobStore(inner);
    var validations = new AtomicInteger();
    var service = new DashboardApiService(
        store, new LocalWakeBus(), replacement -> validations.incrementAndGet(), Duration.ZERO);
    var job = inner.findByHandlerSignature("com.example.Handler", 10).getFirst();

    service.replaceJob(
        job.id(),
        new ReplaceJobRequest(
            job.version(), null, null, null, "com.example.ReplacementHandler", null));
    service.overview(false);
    service.overview(false);

    assertThat(validations).hasValue(1);
    assertThat(store.queueDepthReads).hasValue(2);
  }

  @Test
  void searchOffsetBeyondTheCapIsABadRequest() {
    var service = new DashboardApiService(
        new InMemoryJobStore(), new LocalWakeBus(), DashboardJobDefinitionValidator.denyAll());

    assertThatThrownBy(() -> service.jobs(new JobSearch(
            JobState.ENQUEUED, null, null, 50, DashboardApiService.MAX_SEARCH_OFFSET + 1)))
        .isInstanceOf(DashboardApiException.class)
        .satisfies(error -> assertThat(((DashboardApiException) error).code())
            .isEqualTo(DashboardApiException.Code.BAD_REQUEST));
  }

  @Test
  void argumentOnlyReplacementPreservesTheHandlerAndIsValidatedBeforePersistence() {
    var store = new InMemoryJobStore();
    var originalArgument = new JobArgument("example.OriginalPayload", "{\"value\":\"old\"}");
    var replacementArgument = new JobArgument("example.ReplacementPayload", "{\"value\":\"new\"}");
    var job = Job.builder()
        .spec(JobSpec.of("example.RegisteredHandler", originalArgument))
        .build();
    store.insert(job);
    var validated = new ArrayList<JobSpec>();
    var service = new DashboardApiService(store, new LocalWakeBus(), validated::add);

    service.replaceJob(
        job.id(),
        new ReplaceJobRequest(job.version(), null, null, null, null, List.of(replacementArgument)));

    assertThat(validated).singleElement().satisfies(spec -> {
      assertThat(spec.handlerType()).isEqualTo("example.RegisteredHandler");
      assertThat(spec.arguments()).containsExactly(replacementArgument);
    });
    assertThat(store.findById(job.id()).orElseThrow().spec()).isEqualTo(validated.getFirst());
  }

  @Test
  void definitionReplacementPreservesDeduplicationMetadata() {
    var store = new InMemoryJobStore();
    var originalSpec = JobSpec.of(
            "example.OriginalHandler", new JobArgument("example.Payload", "{\"value\":\"old\"}"))
        .withDedup("original-key", Duration.ofHours(1));
    var job = Job.builder().spec(originalSpec).build();
    store.insert(job);
    var service = new DashboardApiService(store, new LocalWakeBus(), replacement -> {});

    service.replaceJob(
        job.id(),
        new ReplaceJobRequest(job.version(), null, null, null, "example.ReplacementHandler", null));

    assertThat(store.findById(job.id()).orElseThrow().spec()).satisfies(spec -> {
      assertThat(spec.handlerType()).isEqualTo("example.ReplacementHandler");
      assertThat(spec.dedupKey()).isEqualTo("original-key");
      assertThat(spec.dedupTtl()).isEqualTo(Duration.ofHours(1));
    });
  }

  @Test
  void definitionReplacementWithoutAValidatorIsRejectedBeforePersistence() {
    var store = new InMemoryJobStore();
    var originalSpec = JobSpec.of("example.RegisteredHandler");
    var job = Job.builder().spec(originalSpec).build();
    store.insert(job);
    var service = new DashboardApiService(
        store, new LocalWakeBus(), DashboardJobDefinitionValidator.denyAll());

    assertThatThrownBy(() -> service.replaceJob(
            job.id(),
            new ReplaceJobRequest(
                job.version(), null, null, null, "example.ArbitraryHandler", null)))
        .isInstanceOf(DashboardApiException.class)
        .satisfies(error -> assertThat(((DashboardApiException) error).code())
            .isEqualTo(DashboardApiException.Code.NOT_SUPPORTED));

    assertThat(store.findById(job.id()).orElseThrow().spec()).isEqualTo(originalSpec);
  }

  @Test
  void recurringDefinitionReplacementIsValidatedBeforePersistence() {
    var store = new InMemoryJobStore();
    seedCronTask(store, "report");
    var replacementArgument = new JobArgument("example.ReplacementPayload", "{\"value\":\"new\"}");
    var validated = new ArrayList<JobSpec>();
    var service = new DashboardApiService(store, new LocalWakeBus(), validated::add);

    service.updateRecurring(
        "report",
        new UpdateRecurringRequest(
            null,
            null,
            "example.ReplacementHandler",
            replacementArgument,
            null,
            null,
            null,
            null,
            null));

    assertThat(validated)
        .containsExactly(JobSpec.of("example.ReplacementHandler", replacementArgument));
    assertThat(store.findCronTask("report").orElseThrow()).satisfies(task -> {
      assertThat(task.handlerType()).isEqualTo("example.ReplacementHandler");
      assertThat(task.payloadArgument()).isEqualTo(replacementArgument);
    });
  }

  @Test
  void recurringDefinitionReplacementWithoutAValidatorIsRejectedBeforePersistence() {
    var store = new InMemoryJobStore();
    seedCronTask(store, "report");
    var original = store.findCronTask("report").orElseThrow();
    var service = new DashboardApiService(
        store, new LocalWakeBus(), DashboardJobDefinitionValidator.denyAll());

    assertThatThrownBy(() -> service.updateRecurring(
            "report",
            new UpdateRecurringRequest(
                null, null, "example.ArbitraryHandler", null, null, null, null, null, null)))
        .isInstanceOf(DashboardApiException.class)
        .satisfies(error -> assertThat(((DashboardApiException) error).code())
            .isEqualTo(DashboardApiException.Code.NOT_SUPPORTED));

    assertThat(store.findCronTask("report")).contains(original);
  }

  private static void seedQueues(InMemoryJobStore store) {
    store.insert(
        Job.builder().spec(JobSpec.of("com.example.Handler")).queue("alpha").build());
    store.insert(
        Job.builder().spec(JobSpec.of("com.example.Handler")).queue("beta").build());
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
    var task = store.findCronTask(name).orElseThrow();
    store.upsertCronTaskState(CronTaskScheduleState.initial(
        name, Instant.now().plusSeconds(300), CronTaskScheduleState.timingFingerprintOf(task)));
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
    var service = new DashboardApiService(
        new InMemoryJobStore(), new LocalWakeBus(), DashboardJobDefinitionValidator.denyAll());

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
