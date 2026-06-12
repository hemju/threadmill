package com.hemju.threadmill.soak.harness;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Cycles every shipped scenario at low duration against the in-memory store
 * and asserts each one produces a {@code passed} verdict against its own
 * invariants. Tagged {@code soak} — runs as part of the {@code :soakRegression}
 * gate, not on every {@code check}, because it adds ~25 s.
 *
 * <p>The fast version that runs in every {@code check} is
 * {@link SoakHarnessSmokeTest}, which only exercises {@code mixed-workload}.
 */
@Tag("soak")
final class SoakHarnessRunnerTest {

    @ParameterizedTest
    @ValueSource(
            strings = {
                "mixed-workload",
                "rw-lock-stress",
                "weighted-queues",
                "retry-storm",
                "long-running",
                "pause-resume",
                "bulk-enqueue",
                "crash-recover"
            })
    void scenarioPassesItsInvariantsOnInMemory(String scenario, @TempDir Path tempDir) throws Exception {
        Path outputDir = tempDir.resolve(scenario);
        System.setProperty("threadmill.soak.scenario", scenario);
        // 5 s + 80 jobs/sec — short enough to keep the suite snappy, long
        // enough that drain budgets cover the test reliably even with the
        // JUnit JVM's heavier baseline.
        System.setProperty("threadmill.soak.duration", "5s");
        System.setProperty("threadmill.soak.jobsPerSecond", "80");
        System.setProperty("threadmill.soak.workerCount", "4");
        System.setProperty("threadmill.soak.nodes", "crash-recover".equals(scenario) ? "2" : "1");
        System.setProperty("threadmill.soak.outputDir", outputDir.toString());
        System.setProperty("threadmill.soak.runId", scenario + "-test");
        try {
            SoakHarnessConfig config = SoakHarnessConfig.fromSystemProperties("memory");
            OutputDir dir = new OutputDir(config.outputDir(), config.force());
            SummaryReport report;
            try (BackendFixture fixture = new MemoryHarnessFixture()) {
                report = new SoakHarnessRunner(config, fixture, dir, "soakMemory").run();
            }
            assertThat(report.verdict())
                    .as(
                            "scenario %s produced verdict %s — violations: %s",
                            scenario, report.verdict(), report.invariantResults())
                    .isEqualTo("passed");

            // Validate schema once at the end of each scenario.
            ObjectMapper mapper = new ObjectMapper();
            JsonNode summary = mapper.readTree(Files.readString(outputDir.resolve("summary.json")));
            assertThat(summary.path("verdict").asText()).isEqualTo("passed");
        } finally {
            clearSoakProps();
        }
    }

    private static void clearSoakProps() {
        for (String k : new String[] {
            "scenario",
            "duration",
            "jobsPerSecond",
            "workerCount",
            "nodes",
            "outputDir",
            "runId",
            "failFast",
            "postgresUrl",
            "force",
            "redisTopology",
            "progressInterval",
            "nodeChurn"
        }) {
            System.clearProperty("threadmill.soak." + k);
        }
    }
}
