package com.hemju.threadmill.soak.harness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * -Pproducers fans the workload over N concurrent producer threads with
 * jobsPerSecond as the run's total target. The green path must keep every
 * invariant (concurrent producers on keyed work is exactly the contention
 * shape the stress runs want); scenarios with run-level side effects must be
 * rejected before the run starts.
 */
final class MultiProducerSmokeTest {

    @Test
    void concurrentProducersDrainCleanlyAndReportRunTotals(@TempDir Path tempDir) throws Exception {
        Path outputDir = tempDir.resolve("multi-producer-smoke");
        System.setProperty("threadmill.soak.scenario", "mixed-workload");
        System.setProperty("threadmill.soak.duration", "5s");
        System.setProperty("threadmill.soak.jobsPerSecond", "100");
        System.setProperty("threadmill.soak.producers", "4");
        System.setProperty("threadmill.soak.workerCount", "8");
        System.setProperty("threadmill.soak.nodes", "1");
        System.setProperty("threadmill.soak.outputDir", outputDir.toString());
        System.setProperty("threadmill.soak.runId", "multi-producer-smoke");
        try {
            SoakHarnessConfig config = SoakHarnessConfig.fromSystemProperties("memory");
            assertThat(config.producers()).isEqualTo(4);
            OutputDir dir = new OutputDir(config.outputDir(), config.force());
            SummaryReport report;
            try (BackendFixture fixture = new MemoryHarnessFixture()) {
                report = new SoakHarnessRunner(config, fixture, dir, "soakMemory").run();
            }
            assertThat(report.verdict())
                    .as("multi-producer run verdict — violations: %s", report.invariantResults())
                    .isEqualTo("passed");

            ObjectMapper mapper = new ObjectMapper();
            JsonNode summary = mapper.readTree(Files.readString(outputDir.resolve("summary.json")));
            long enqueued = summary.path("performance").path("totalEnqueued").asLong();
            // Four producers at 25/s each for 5s — meaningfully more than one
            // producer's share, and the run total is reported (not one stream's).
            assertThat(enqueued).isGreaterThan(200);
        } finally {
            clearProps();
        }
    }

    @Test
    void sideEffectScenariosRejectConcurrentProducers(@TempDir Path tempDir) {
        for (String scenario : new String[] {"crash-recover", "pause-resume"}) {
            System.setProperty("threadmill.soak.scenario", scenario);
            System.setProperty("threadmill.soak.duration", "5s");
            System.setProperty("threadmill.soak.producers", "2");
            System.setProperty("threadmill.soak.nodes", "2");
            System.setProperty(
                    "threadmill.soak.outputDir", tempDir.resolve(scenario).toString());
            try {
                SoakHarnessConfig config = SoakHarnessConfig.fromSystemProperties("memory");
                OutputDir dir = new OutputDir(config.outputDir(), config.force());
                assertThatThrownBy(() -> {
                            try (BackendFixture fixture = new MemoryHarnessFixture()) {
                                new SoakHarnessRunner(config, fixture, dir, "soakMemory").run();
                            }
                        })
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("producers");
            } finally {
                clearProps();
            }
        }
    }

    private static void clearProps() {
        for (String k : new String[] {
            "scenario",
            "duration",
            "jobsPerSecond",
            "producers",
            "workerCount",
            "nodes",
            "outputDir",
            "runId",
            "failFast",
            "postgresUrl",
            "force",
            "redisTopology",
            "redisUrl",
            "progressInterval",
            "nodeChurn"
        }) {
            System.clearProperty("threadmill.soak." + k);
        }
    }
}
