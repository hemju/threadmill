package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import com.hemju.threadmill.core.EnqueueResult;
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
import com.hemju.threadmill.core.store.JobStoreCapabilities;
import com.hemju.threadmill.store.oracle.OracleJobStore;
import com.hemju.threadmill.store.oracle.OracleMigrationRunner;

/** {@code join_transaction} enqueue mode on Oracle: jobs commit and roll back with the caller. */
@EnabledIf("com.hemju.threadmill.spring.DockerAvailable#check")
class SpringOracleTransactionBoundaryTest {

  private OracleJobStore store;
  private TransactionTemplate transactions;
  private TransactionJoinedJobScheduler scheduler;
  private CopyOnWriteArrayList<String> wakes;

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

  @BeforeEach
  void setUp() throws Exception {
    DataSource dataSource = OracleTestDatabase.dataSource();
    new OracleMigrationRunner(dataSource).migrate();
    OracleTestDatabase.reset();
    store = new OracleJobStore(
        dataSource,
        new JsonJobSerializer(),
        JobStoreCapabilities.defaults(),
        new SpringOracleTransactionBoundary(dataSource));
    transactions = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    var wakeBus = new LocalWakeBus();
    wakes = new CopyOnWriteArrayList<>();
    wakeBus.register(wakes::add);
    scheduler = new TransactionJoinedJobScheduler(
        store,
        new JsonJobSerializer(),
        new TestRegistry(),
        ProcessingNodeConfig.builder().build(),
        wakeBus);
  }

  @Test
  void enqueueCommitsWithCallerTransactionAndWakesAfterCommit() {
    AtomicReference<JobId> id = new AtomicReference<>();

    transactions.executeWithoutResult(status -> {
      id.set(scheduler.enqueue(GreetHandler.class, new GreetPayload("commit")));
      assertThat(wakes).isEmpty();
    });

    assertThat(store.findById(id.get())).isPresent();
    assertThat(wakes).containsExactly("default");
  }

  @Test
  void enqueueRollsBackWithCallerTransaction() {
    AtomicReference<JobId> id = new AtomicReference<>();

    transactions.executeWithoutResult(status -> {
      id.set(scheduler.enqueue(GreetHandler.class, new GreetPayload("rollback")));
      status.setRollbackOnly();
    });

    assertThat(store.findById(id.get())).isEmpty();
    assertThat(store.queueDepths()).isEmpty();
    assertThat(wakes).isEmpty();
  }

  @Test
  void dedupRollsBackWithCallerTransaction() {
    AtomicReference<EnqueueResult> first = new AtomicReference<>();

    transactions.executeWithoutResult(status -> {
      first.set(scheduler.enqueueIfAbsent(
          GreetHandler.class, new GreetPayload("rollback"), "tenant:greet", Duration.ofMinutes(5)));
      status.setRollbackOnly();
    });

    assertThat(first.get()).isInstanceOf(EnqueueResult.Created.class);
    assertThat(scheduler.enqueueIfAbsent(
            GreetHandler.class, new GreetPayload("retry"), "tenant:greet", Duration.ofMinutes(5)))
        .isInstanceOf(EnqueueResult.Created.class);
  }

  @Test
  void dedupInsideAJoinedTransactionCoalescesOntoTheCommittedJob() {
    var committed = scheduler.enqueueIfAbsent(
        GreetHandler.class, new GreetPayload("first"), "tenant:once", Duration.ofMinutes(5));
    AtomicReference<EnqueueResult> second = new AtomicReference<>();

    transactions.executeWithoutResult(status -> second.set(scheduler.enqueueIfAbsent(
        GreetHandler.class, new GreetPayload("second"), "tenant:once", Duration.ofMinutes(5))));

    assertThat(second.get()).isInstanceOf(EnqueueResult.Coalesced.class);
    assertThat(((EnqueueResult.Coalesced) second.get()).existingId())
        .isEqualTo(((EnqueueResult.Created) committed).id());
  }

  @Test
  void nudgeTakesEffectOnCommitAndIsDiscardedOnRollback() {
    registerPumpTask("outbox-pump");

    transactions.executeWithoutResult(status -> {
      scheduler.nudgeRecurring("outbox-pump");
      status.setRollbackOnly();
    });
    assertThat(store.findCronTaskState("outbox-pump").orElseThrow().nudgeRequestedAt())
        .isNull();

    transactions.executeWithoutResult(status -> {
      scheduler.nudgeRecurring("outbox-pump");
      assertThat(store.findCronTaskState("outbox-pump").orElseThrow().nudgeRequestedAt())
          .as("the nudge write is deferred to after commit")
          .isNull();
    });
    assertThat(store.findCronTaskState("outbox-pump").orElseThrow().nudgeRequestedAt())
        .isNotNull();
  }

  @Test
  void nudgeIsCommittedEvenWhenThePoolHandsOutNonAutoCommitConnections() {
    // The nudge runs in afterCommit, when Spring has committed but not yet
    // unbound the caller's connection: it must run in Threadmill's own
    // committed transaction, not on that connection.
    DataSource nonAutoCommit = new NonAutoCommitDataSource(OracleTestDatabase.dataSource());
    var joiningStore = new OracleJobStore(
        nonAutoCommit,
        new JsonJobSerializer(),
        JobStoreCapabilities.defaults(),
        new SpringOracleTransactionBoundary(nonAutoCommit));
    var joiningScheduler = new TransactionJoinedJobScheduler(
        joiningStore,
        new JsonJobSerializer(),
        new TestRegistry(),
        ProcessingNodeConfig.builder().build(),
        new LocalWakeBus());
    registerPumpTask("pooled-pump");

    new TransactionTemplate(new DataSourceTransactionManager(nonAutoCommit))
        .executeWithoutResult(status -> joiningScheduler.nudgeRecurring("pooled-pump"));

    assertThat(store.findCronTaskState("pooled-pump").orElseThrow().nudgeRequestedAt())
        .isNotNull();
  }

  @Test
  void joinTransactionFailsFastWhenCallerTransactionUsesDifferentDataSource() {
    try (var other = OracleTestDatabase.newPool()) {
      new TransactionTemplate(new DataSourceTransactionManager(other))
          .executeWithoutResult(status -> assertThatThrownBy(
                  () -> scheduler.enqueue(GreetHandler.class, new GreetPayload("wrong-ds")))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining("same DataSource"));
    }
    assertThat(wakes).isEmpty();
  }

  private void registerPumpTask(String name) {
    var task = new CronTask(
        name,
        new CronTask.Trigger.Interval(Duration.ofHours(6)),
        GreetHandler.class.getName(),
        new JobArgument(GreetPayload.class.getName(), "{}"),
        "default",
        0,
        CronTask.MissedRunPolicy.DROP,
        ZoneId.of("UTC"),
        true);
    store.upsertCronTask(task);
    store.upsertCronTaskState(CronTaskScheduleState.initial(
        name,
        Instant.now().plus(Duration.ofHours(6)),
        CronTaskScheduleState.timingFingerprintOf(task)));
  }

  /** A pool-alike whose connections arrive with {@code autoCommit=false}. */
  private static final class NonAutoCommitDataSource extends DelegatingDataSource {
    NonAutoCommitDataSource(DataSource target) {
      super(target);
    }

    @Override
    public Connection getConnection() throws SQLException {
      Connection connection = super.getConnection();
      connection.setAutoCommit(false);
      return connection;
    }
  }

  private static final class TestRegistry extends ThreadmillJobRegistry {
    TestRegistry() {
      super(new ThreadmillJobRegistry.Registration(
          GreetPayload.class, GreetHandler.class, "default", 0, 5, Duration.ofMinutes(5), null));
    }
  }
}
