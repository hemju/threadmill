package com.hemju.threadmill.soak.harness;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Smoke test for the {@code nudge-pump} scenario (issue #108) against the
 * in-memory store.
 *
 * <p>Guards the thing an endurance run cannot: that the scenario's invariants
 * are actually <em>armed</em>. A checker that silently observes nothing passes
 * every run, so this asserts the nudge and drain events reach the trace, that
 * pump instances really are nudge-triggered rather than schedule-triggered
 * (the backstop is ten minutes — no schedule fire can occur in a run this
 * short), and that both new invariants report a pass rather than being absent.
 */
final class NudgePumpSmokeTest {

  @Test
  void nudgeDrivenPumpDrainsItsOutboxAndBothNudgeInvariantsPass(@TempDir Path tempDir)
      throws Exception {
    Path outputDir = tempDir.resolve("nudge-smoke");
    System.setProperty("threadmill.soak.scenario", "nudge-pump");
    System.setProperty("threadmill.soak.duration", "8s");
    System.setProperty("threadmill.soak.jobsPerSecond", "20");
    System.setProperty("threadmill.soak.nodes", "1");
    System.setProperty("threadmill.soak.workerCount", "4");
    System.setProperty("threadmill.soak.outputDir", outputDir.toString());
    System.setProperty("threadmill.soak.runId", "nudge-smoke");
    try {
      SoakHarnessConfig config = SoakHarnessConfig.fromSystemProperties("memory");
      OutputDir dir = new OutputDir(config.outputDir(), config.force());
      SummaryReport report;
      try (BackendFixture fixture = new MemoryHarnessFixture()) {
        report = new SoakHarnessRunner(config, fixture, dir, "soakMemory").run();
      }
      assertThat(report.verdict()).isEqualTo("passed");

      String trace = Files.readString(outputDir.resolve("trace.jsonl"));
      assertThat(trace).contains("\"event\":\"nudge_accepted\"");
      assertThat(trace).contains("\"event\":\"work_recorded\"");
      assertThat(trace).contains("\"event\":\"work_drained\"");
      assertThat(trace)
          .as(
              "pump runs must be nudge-triggered — the backstop schedule cannot fire in a run this short")
          .contains("\"cronOrigin\":\"nudge\"");
      assertThat(trace).doesNotContain("\"cronOrigin\":\"schedule\"");

      // The outbox must actually have drained: every recorded row is
      // covered by a drain, which is what the invariant asserts and
      // what a swallowed nudge would break.
      assertThat(SoakOutbox.pending()).isZero();

      ObjectMapper mapper = new ObjectMapper();
      JsonNode invariants = mapper.readTree(Files.readString(outputDir.resolve("invariants.json")));
      assertThat(namedResult(invariants, "nudgeRunAfterWake")).isEqualTo("passed");
      assertThat(namedResult(invariants, "outboxDrainedByLaterRun")).isEqualTo("passed");
    } finally {
      clearSoakSystemProps();
    }
  }

  private static String namedResult(JsonNode invariants, String name) {
    for (JsonNode node : invariants.isArray() ? invariants : invariants.path("invariants")) {
      if (name.equals(node.path("name").asText())) {
        return node.path("passed").asBoolean() ? "passed" : "failed: " + node.path("violations");
      }
    }
    return "absent — the invariant was never registered for this run";
  }

  private static void clearSoakSystemProps() {
    for (String k : new String[] {
      "scenario",
      "duration",
      "jobsPerSecond",
      "workerCount",
      "nodes",
      "outputDir",
      "runId",
      "failFast",
      "force"
    }) {
      System.clearProperty("threadmill.soak." + k);
    }
  }
}
