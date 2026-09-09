package com.hemju.threadmill.soak.harness;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.hemju.threadmill.core.JobState;

/** Runs enough retention cycles to prove that the sustained profile really reclaims records and keys. */
@Tag("soak")
class RetentionChurnSmokeTest {
  @ParameterizedTest
  @ValueSource(strings = {"memory", "postgres", "redis"})
  void retainedPopulationShrinksWhileRetriesAndWorkflowsComplete(
      String backend, @TempDir Path temporary) throws Exception {
    var output = temporary.resolve("run");
    var config = new SoakHarnessConfig(
        backend,
        "retention-churn",
        Duration.ofSeconds(24),
        30,
        1,
        8,
        2,
        output,
        "retention-regression",
        true,
        Optional.empty(),
        "standalone",
        Optional.empty(),
        false,
        Duration.ofSeconds(1),
        Optional.empty());
    try (var fixture =
        switch (backend) {
          case "postgres" -> new PostgresHarnessFixture(Optional.empty());
          case "redis" -> new RedisHarnessFixture("standalone");
          default -> new MemoryHarnessFixture();
        }) {
      var report = new SoakHarnessRunner(
              config, fixture, new OutputDir(output, false), "retention-test")
          .run();
      assertThat(report.verdict()).as(report.invariantResults().toString()).isEqualTo("passed");
      assertThat(report.performance().totalRetried()).isPositive();
      assertThat(fixture.store().countsByState().getOrDefault(JobState.SUCCEEDED, 0L))
          .isPositive()
          .isLessThan(report.performance().totalEnqueued());
      var mapper = new ObjectMapper();
      try (var lines = Files.lines(output.resolve("metrics.jsonl"))) {
        var rows = lines
            .map(line -> {
              try {
                return mapper.readTree(line);
              } catch (Exception failure) {
                throw new IllegalStateException(failure);
              }
            })
            .toList();
        assertThat(rows).isNotEmpty();
        var last = rows.getLast();
        assertThat(last.path("jobsDeleted").asLong()).isPositive();
        if (!backend.equals("memory"))
          assertThat(last.path("concurrencyGroupsDeleted").asLong()).isPositive();
        assertThat(last.path("operations").path("claim").path("calls").asLong()).isPositive();
        assertThat(last.path("operations").path("terminalSave").path("calls").asLong())
            .isPositive();
      }
    }
  }
}
