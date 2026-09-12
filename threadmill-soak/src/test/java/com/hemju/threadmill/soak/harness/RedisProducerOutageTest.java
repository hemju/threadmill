package com.hemju.threadmill.soak.harness;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/** Real Redis outage longer than the configured command timeout must not end the producer. */
@Tag("soak")
class RedisProducerOutageTest {
  @SuppressWarnings("resource")
  @ParameterizedTest
  @ValueSource(strings = {"mixed-workload", "retention-churn"})
  void producerResumesAfterOutageExceedsCommandTimeout(String scenario, @TempDir Path temporary)
      throws Exception {
    try (var redis = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
        .withExposedPorts(6379)
        .withCommand("redis-server", "--appendonly", "yes")) {
      redis.start();
      var output = temporary.resolve("run");
      var config = new SoakHarnessConfig(
          "redis",
          scenario,
          Duration.ofSeconds(60),
          30,
          1,
          8,
          2,
          output,
          "producer-outage",
          true,
          Optional.empty(),
          "standalone",
          Optional.of("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379)),
          false,
          Duration.ofSeconds(1),
          Optional.empty());
      try (var fixture = new RedisHarnessFixture("standalone", config.redisUrl());
          var executor = Executors.newVirtualThreadPerTaskExecutor()) {
        var fault = executor.submit(() -> {
          long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
          while (!Files.exists(output.resolve("progress.json"))) {
            if (System.nanoTime() > deadline)
              throw new IllegalStateException("harness never started");
            Thread.sleep(20);
          }
          Thread.sleep(500);
          redis.getDockerClient().pauseContainerCmd(redis.getContainerId()).exec();
          try {
            Thread.sleep(12_000);
          } finally {
            redis.getDockerClient().unpauseContainerCmd(redis.getContainerId()).exec();
          }
          return null;
        });
        var report = new SoakHarnessRunner(
                config, fixture, new OutputDir(output, false), "producer-outage")
            .run();
        fault.get(30, TimeUnit.SECONDS);
        assertThat(report.verdict()).as(report.invariantResults().toString()).isEqualTo("passed");
        assertThat(report.performance().totalEnqueued()).isGreaterThan(1500);
        assertThat(Files.readString(output.resolve("trace.jsonl")))
            .contains("producer_outage", "producer_recovered");
      }
    }
  }
}
