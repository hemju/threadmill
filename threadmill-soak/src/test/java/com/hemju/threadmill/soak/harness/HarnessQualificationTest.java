package com.hemju.threadmill.soak.harness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.hemju.threadmill.core.schedule.Scheduler;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.soak.harness.scenario.SoakRunContext;
import com.hemju.threadmill.store.memory.InMemoryJobStore;
import com.hemju.threadmill.store.redis.RedisStoreConfig;

class HarnessQualificationTest {
  @Test
  void workloadDurationIsNotConsumedByAnEarlierSetupTimestamp(@TempDir Path output)
      throws Exception {
    var config = new SoakHarnessConfig(
        "memory",
        "mixed-workload",
        Duration.ofSeconds(1),
        1,
        1,
        1,
        1,
        output,
        "duration",
        true,
        Optional.empty(),
        "standalone",
        Optional.empty(),
        false,
        Duration.ofSeconds(1),
        Optional.empty());
    try (var trace = new SoakTraceWriter(output.resolve("trace.jsonl"))) {
      var context =
          new SoakRunContext(config, new InMemoryJobStore(), trace, Instant.EPOCH, List::of);
      assertThat(context.runDeadline()).isAfter(Instant.now());
    }
  }

  @Test
  void lowRatePacingWaitsPastOneSleepChunk(@TempDir Path output) throws Exception {
    try (var trace = new SoakTraceWriter(output.resolve("trace.jsonl"));
        var latency = new LatencyTracker(output.resolve("latencies.jsonl"))) {
      var gen = new LoadGenerator(
          new Scheduler(new InMemoryJobStore(), new JsonJobSerializer()), trace, latency, 1);
      var deadline = Instant.now().plusMillis(180);
      gen.pace(deadline);
      assertThat(Instant.now()).isAfterOrEqualTo(deadline);
    }
  }

  @Test
  void clusterSeedsPreserveCredentialsAndVerifiedTls() {
    var config = (RedisStoreConfig.Cluster) RedisHarnessFixture.externalConfig(
        "cluster", "rediss://test:secret@localhost:7000,rediss://test:secret@localhost:7001");
    assertThat(config.nodes()).hasSize(2);
    assertThat(config.credentials()).isEqualTo(new RedisStoreConfig.Credentials("test", "secret"));
    assertThat(config.tls()).isEqualTo(RedisStoreConfig.Tls.verified());
    assertThatThrownBy(() -> RedisHarnessFixture.externalConfig(
            "cluster", "redis://localhost:7000,rediss://localhost:7001"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> RedisHarnessFixture.externalConfig("cluster", "redis://localhost:7000/1"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void sentinelUriRetainsDiscoveryNodesAndMasterName() {
    var config = (RedisStoreConfig.Standalone) RedisHarnessFixture.externalConfig(
        "sentinel", "redis-sentinel://localhost:26379,localhost:26380/0#soak-primary");
    assertThat(config.uri().getSentinels()).hasSize(2);
    assertThat(config.uri().getSentinelMasterId()).isEqualTo("soak-primary");
    assertThatThrownBy(
            () -> RedisHarnessFixture.externalConfig("sentinel", "redis://localhost:6379"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
