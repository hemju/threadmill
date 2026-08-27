package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.store.memory.InMemoryJobStore;
import com.hemju.threadmill.test.ForwardingJobStore;

/**
 * Drives the {@link TransactionAwareJobScheduler} contract directly by
 * managing {@link TransactionSynchronizationManager} from the test. Boots no
 * Spring context — the wrapper only checks
 * {@code isSynchronizationActive()} so the manager is sufficient.
 */
class TransactionAwareJobSchedulerTest {

  private InMemoryJobStore store;
  private TransactionAwareJobScheduler enqueuer;
  private CopyOnWriteArrayList<String> wakeCalls;

  public static final class GreetPayload implements JobPayload {
    public String tag;

    public GreetPayload() {}

    public GreetPayload(String tag) {
      this.tag = tag;
    }
  }

  public static final class GreetHandler implements JobHandler<GreetPayload> {
    @Override
    public void run(GreetPayload p, JobExecutionContext c) {}
  }

  public static final class OtherPayload implements JobPayload {}

  @BeforeEach
  void setUp() {
    store = new InMemoryJobStore();
    var serializer = new JsonJobSerializer();
    var registry = new TestRegistry();
    var wakeBus = new LocalWakeBus();
    wakeCalls = new CopyOnWriteArrayList<>();
    wakeBus.register(wakeCalls::add);
    enqueuer = new TransactionAwareJobScheduler(
        store, serializer, registry, ProcessingNodeConfig.builder().build(), wakeBus);
  }

  @AfterEach
  void tearDown() {
    if (TransactionSynchronizationManager.isSynchronizationActive()) {
      TransactionSynchronizationManager.clear();
    }
  }

  @Test
  void enqueueOutsideTransactionIsImmediate() {
    JobId id = enqueuer.enqueue(GreetHandler.class, new GreetPayload("immediate"));
    assertThat(store.findById(id)).isPresent();
  }

  @Test
  void enqueueInsideTransactionIsNotVisibleBeforeCommit() {
    TransactionSynchronizationManager.initSynchronization();
    try {
      JobId id = enqueuer.enqueue(GreetHandler.class, new GreetPayload("deferred"));
      assertThat(store.findById(id))
          .as("row must not exist until afterCommit fires")
          .isEmpty();
      triggerAfterCommit();
      assertThat(store.findById(id)).isPresent();
    } finally {
      TransactionSynchronizationManager.clear();
    }
  }

  @Test
  void enqueueInsideTransactionWakesOnlyAfterCommit() {
    TransactionSynchronizationManager.initSynchronization();
    try {
      enqueuer.enqueue(GreetHandler.class, new GreetPayload("deferred"));

      assertThat(wakeCalls).isEmpty();

      triggerAfterCommit();

      assertThat(wakeCalls).containsExactly("default");
    } finally {
      TransactionSynchronizationManager.clear();
    }
  }

  @Test
  void enqueueInsideTransactionIsRolledBackOnRollback() {
    TransactionSynchronizationManager.initSynchronization();
    try {
      JobId id = enqueuer.enqueue(GreetHandler.class, new GreetPayload("rolled-back"));
      assertThat(store.findById(id)).isEmpty();
      // No afterCommit triggered — simulating rollback.
      // The synchronization is dropped on clear() without firing afterCommit.
      TransactionSynchronizationManager.clear();
      assertThat(store.findById(id)).isEmpty();
    } finally {
      if (TransactionSynchronizationManager.isSynchronizationActive()) {
        TransactionSynchronizationManager.clear();
      }
    }
  }

  @Test
  void enqueueAllDefersTheBatchAndCommitsAtomically() {
    TransactionSynchronizationManager.initSynchronization();
    try {
      var ids = enqueuer.enqueueAll(
          GreetHandler.class,
          List.of(new GreetPayload("a"), new GreetPayload("b"), new GreetPayload("c")));
      for (JobId id : ids) {
        assertThat(store.findById(id)).isEmpty();
      }
      triggerAfterCommit();
      for (JobId id : ids) {
        assertThat(store.findById(id)).isPresent();
      }
    } finally {
      TransactionSynchronizationManager.clear();
    }
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void enqueueAllRejectsMixedPayloadsBeforeWritingAnything() {
    // Routing is now by handler class, so a wrong payload type for the chosen handler
    // is the failure mode (rather than the payload itself being unregistered).
    assertThatThrownBy(() -> enqueuer.enqueueAll(
            (Class) GreetHandler.class, List.of(new GreetPayload("a"), new OtherPayload())))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(OtherPayload.class.getName())
        .hasMessageContaining(GreetPayload.class.getName());

    assertThat(store.countsByState().values()).containsOnly(0L);
  }

  @Test
  void scheduledEnqueueIsAlsoDeferred() {
    TransactionSynchronizationManager.initSynchronization();
    try {
      JobId id =
          enqueuer.enqueueIn(GreetHandler.class, new GreetPayload("later"), Duration.ofMinutes(5));
      assertThat(store.findById(id)).isEmpty();
      triggerAfterCommit();
      assertThat(store.findById(id)).isPresent();
    } finally {
      TransactionSynchronizationManager.clear();
    }
  }

  @Test
  void scheduledEnqueueDoesNotWakeBeforePromotion() {
    TransactionSynchronizationManager.initSynchronization();
    try {
      enqueuer.enqueueIn(GreetHandler.class, new GreetPayload("later"), Duration.ofMinutes(5));
      triggerAfterCommit();

      assertThat(wakeCalls).isEmpty();
    } finally {
      TransactionSynchronizationManager.clear();
    }
  }

  @Test
  void firstFailingAfterCommitInsertDoesNotCancelLaterDeferredEnqueues() {
    // Spring invokes after-commit callbacks in a bare loop with no
    // per-item isolation (triggerAfterCommit mirrors that): without the
    // wrapper's own containment, the first failing insert would skip
    // every later-registered deferred enqueue in the same transaction.
    var failFor = new AtomicReference<JobId>();
    var failing = new ForwardingJobStore(store) {
      @Override
      public void insert(Job job) {
        if (job.id().equals(failFor.get())) {
          throw new RuntimeException("store outage at exactly after-commit time");
        }
        super.insert(job);
      }
    };
    var failingEnqueuer = new TransactionAwareJobScheduler(
        failing,
        new JsonJobSerializer(),
        new TestRegistry(),
        ProcessingNodeConfig.builder().build(),
        new LocalWakeBus());

    TransactionSynchronizationManager.initSynchronization();
    try {
      JobId first = failingEnqueuer.enqueue(GreetHandler.class, new GreetPayload("first"));
      failFor.set(first);
      JobId second = failingEnqueuer.enqueue(GreetHandler.class, new GreetPayload("second"));

      triggerAfterCommit();

      // The first job is lost (logged at ERROR); the second still lands.
      assertThat(store.findById(first)).isEmpty();
      assertThat(store.findById(second)).isPresent();
    } finally {
      TransactionSynchronizationManager.clear();
    }
  }

  // -------- recurring nudge (issue #108) --------

  @Test
  void nudgeOutsideTransactionIsImmediate() {
    registerRecurringTask("pump", true);
    enqueuer.nudgeRecurring("pump");
    assertThat(store.findCronTaskState("pump").orElseThrow().nudgeRequestedAt()).isNotNull();
  }

  @Test
  void nudgeInsideTransactionTakesEffectOnlyAfterCommit() {
    registerRecurringTask("pump", true);
    TransactionSynchronizationManager.initSynchronization();
    try {
      enqueuer.nudgeRecurring("pump");
      assertThat(store.findCronTaskState("pump").orElseThrow().nudgeRequestedAt())
          .as("the nudge write must not land before commit")
          .isNull();
      triggerAfterCommit();
      assertThat(store.findCronTaskState("pump").orElseThrow().nudgeRequestedAt())
          .isNotNull();
    } finally {
      TransactionSynchronizationManager.clear();
    }
  }

  @Test
  void nudgeInsideRolledBackTransactionIsDiscarded() {
    registerRecurringTask("pump", true);
    TransactionSynchronizationManager.initSynchronization();
    try {
      enqueuer.nudgeRecurring("pump");
      // No afterCommit triggered — simulating rollback.
      TransactionSynchronizationManager.clear();
      assertThat(store.findCronTaskState("pump").orElseThrow().nudgeRequestedAt())
          .isNull();
    } finally {
      if (TransactionSynchronizationManager.isSynchronizationActive()) {
        TransactionSynchronizationManager.clear();
      }
    }
  }

  @Test
  void nudgeValidationFailsFastInsideTheTransactionNotAtCommit() {
    registerRecurringTask("paused-pump", false);
    TransactionSynchronizationManager.initSynchronization();
    try {
      assertThatThrownBy(() -> enqueuer.nudgeRecurring("unknown-pump"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("unknown-pump");
      assertThatThrownBy(() -> enqueuer.nudgeRecurring("paused-pump"))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("disabled");
      assertThat(TransactionSynchronizationManager.getSynchronizations())
          .as("rejected nudges must not leave a deferred write behind")
          .isEmpty();
    } finally {
      TransactionSynchronizationManager.clear();
    }
  }

  private void registerRecurringTask(String name, boolean enabled) {
    var task = new CronTask(
        name,
        new CronTask.Trigger.Interval(Duration.ofHours(6)),
        GreetHandler.class.getName(),
        new JobArgument(GreetPayload.class.getName(), "{}"),
        "default",
        0,
        CronTask.MissedRunPolicy.DROP,
        ZoneId.of("UTC"),
        enabled);
    store.upsertCronTask(task);
    store.upsertCronTaskState(CronTaskScheduleState.initial(
        name,
        Instant.now().plus(Duration.ofHours(6)),
        CronTaskScheduleState.timingFingerprintOf(task)));
  }

  private static void triggerAfterCommit() {
    for (TransactionSynchronization s : TransactionSynchronizationManager.getSynchronizations()) {
      s.afterCommit();
    }
  }

  /** Tiny stand-in for ThreadmillJobRegistry that exposes one handler binding. */
  private static final class TestRegistry extends ThreadmillJobRegistry {
    TestRegistry() {
      super(new ThreadmillJobRegistry.Registration(
          GreetPayload.class, GreetHandler.class, "default", 0, 5, Duration.ofMinutes(5), null));
    }
  }
}
