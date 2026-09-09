package com.hemju.threadmill.store.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import io.lettuce.core.cluster.api.sync.RedisClusterCommands;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.JobEngineFatalException;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.WorkflowInterceptor;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.test.Jobs;
import com.hemju.threadmill.test.LegacyJobFixtures;

/** One nonempty legacy-index fixture exercised through standalone and Cluster clients. */
final class RedisUpgradeFixtures {
  private RedisUpgradeFixtures() {}

  static void verify(
      JobStore seed,
      RedisClusterCommands<String, String> commands,
      LongSupplier migrate,
      Supplier<RedisJobStore> reopen) {
    var serializer = new JsonJobSerializer();
    for (var name : LegacyJobFixtures.NAMES) {
      var wire = LegacyJobFixtures.wire(name);
      var job = serializer.deserializeJob(wire.replace("\"version\":7", "\"version\":0"));
      seed.insert(job);
      commands.hset(RedisKeys.job(job.id()), "body", wire);
      commands.hset(RedisKeys.job(job.id()), "version", "7");
      commands.hdel(RedisKeys.job(job.id()), "execution_revision");
    }
    var root = Jobs.withConcurrency("example.Keyed", "upgrade-key", ConcurrencyMode.EXCLUSIVE);
    seed.insert(root);
    var child = Jobs.awaitingWorkflowStep("example.Child", root);
    seed.insert(child);
    var held = seed.claimReady(NodeId.newId(), "default", 1, Instant.now()).getFirst();
    assertThat(held.id()).isEqualTo(root.id());
    var pending = RedisKeys.concurrencyPending("upgrade-key");
    var member = RedisKeys.concurrencyPendingMember(ConcurrencyMode.EXCLUSIVE, child.id());
    var score = commands.zscore(pending, member);
    commands.zrem(pending, member);
    commands.zadd(pending, score, "EXCLUSIVE:" + child.id());
    var pendingRoot = RedisKeys.concurrencyPendingRoot("upgrade-key", root.id().toString());
    commands.zrem(pendingRoot, member);
    commands.zadd(pendingRoot, score, "EXCLUSIVE:" + child.id());
    commands.del(
        pending + ":exclusive",
        RedisKeys.concurrencyReady("upgrade-key", "default"),
        RedisKeys.orderedQueueKeys("default"),
        RedisKeys.CONCURRENCY_COUNTERS);

    var dedup = Jobs.onQueue("example.Dedup", "dedup");
    seed.enqueueIfAbsent(dedup, "legacy-dedup", Duration.ofHours(1), Instant.now());
    seed.pauseQueue("empty-paused", "upgrade");
    seed.upsertCronTask(new CronTask(
        "upgrade-cron",
        new CronTask.Trigger.Interval(Duration.ofHours(1)),
        "example.UpgradeHandler",
        new JobArgument("example.UpgradePayload", "{}"),
        "upgrade",
        0,
        null,
        null,
        true,
        CronTask.MissedRunPolicy.DROP,
        ZoneOffset.UTC,
        true));
    seed.upsertCronTaskState(new CronTaskScheduleState(
        "upgrade-cron",
        null,
        null,
        Instant.now(),
        UUID.fromString("01900000-0000-7000-8000-000000000003"),
        "legacy-fingerprint"));
    seed.requestCronNudge("upgrade-cron", Instant.now());
    seed.recordCronTaskOwnership("upgrade-app", "upgrade-cron");
    var oldCronState = seed.findCronTaskState("upgrade-cron").orElseThrow();
    for (var state : JobState.values()) commands.del(RedisKeys.byStateTime(state) + ":ids");
    commands.del(RedisJobStore.CRON_TASKS_ORDERED, RedisStorageFormat.KEY);
    assertThatThrownBy(reopen::get).isInstanceOf(JobEngineFatalException.class);
    assertThat(migrate.getAsLong()).isEqualTo(11);
    assertThat(migrate.getAsLong()).isZero();
    try (var upgraded = reopen.get()) {
      for (var name : LegacyJobFixtures.NAMES) {
        var original = serializer.deserializeJob(LegacyJobFixtures.wire(name));
        assertThat(commands.hget(RedisKeys.job(original.id()), "body"))
            .isEqualTo(LegacyJobFixtures.wire(name));
        assertThat(upgraded.findById(original.id())).hasValueSatisfying(job -> {
          assertThat(job.version()).isEqualTo(7);
          assertThat(job.executionRevision()).isZero();
          assertThat(job.currentState()).isEqualTo(original.currentState());
        });
      }
      assertThat(upgraded.listPausedQueues()).contains("empty-paused");
      assertThat(upgraded.scanCronTasks(null, 10)).hasSize(1);
      assertThat(upgraded.findCronTaskState("upgrade-cron")).contains(oldCronState);
      assertThat(upgraded.findCronTask("upgrade-cron").orElseThrow().exclusive())
          .isTrue();
      assertThat(upgraded.listCronTaskNamesOwnedBy("upgrade-app")).containsExactly("upgrade-cron");
      assertThat(upgraded.enqueueIfAbsent(
              Jobs.onQueue("example.Dedup", "dedup"),
              "legacy-dedup",
              Duration.ofHours(1),
              Instant.now()))
          .isEqualTo(new EnqueueResult.Coalesced(dedup.id()));
      assertThat(upgraded.deleteFinishedOlderThan(Instant.now(), JobState.FAILED, 100))
          .isZero();
      var parent = upgraded.findById(root.id()).orElseThrow();
      parent.transitionTo(JobState.SUCCEEDED, Instant.now());
      upgraded.saveAtomic(parent, parent.version());
      new WorkflowInterceptor(upgraded).onProcessingSucceeded(parent, null);
      assertThat(upgraded.claimReady(NodeId.newId(), "default", 1, Instant.now()))
          .extracting(job -> job.id())
          .containsExactly(child.id());
      assertThat(upgraded.findAwaitingByParent(
              JobId.parse("01900000-0000-7000-8000-000000000005"), 10))
          .hasSize(1);
    }
  }
}
