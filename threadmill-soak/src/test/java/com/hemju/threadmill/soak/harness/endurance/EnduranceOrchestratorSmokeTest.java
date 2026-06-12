package com.hemju.threadmill.soak.harness.endurance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end orchestrator contract: two real child harness JVMs run in
 * parallel (in-memory backends keep it container-free and fast), each writes
 * its full per-backend artifact directory, and the collated
 * endurance-summary pair lands at the top with a combined verdict. This is
 * the exact code path of a real postgres+redis endurance run — only the
 * fixture inside each child differs.
 */
final class EnduranceOrchestratorSmokeTest {

    @Test
    void parallelChildrenProduceCollatedSummaryAndPerBackendArtifacts(@TempDir Path tempDir) throws Exception {
        Path outputDir = tempDir.resolve("endurance-smoke");
        System.setProperty("threadmill.soak.backends", "memory,memory");
        System.setProperty("threadmill.soak.scenario", "rw-lock-stress");
        System.setProperty("threadmill.soak.duration", "5s");
        System.setProperty("threadmill.soak.jobsPerSecond", "20");
        System.setProperty("threadmill.soak.workerCount", "4");
        System.setProperty("threadmill.soak.nodes", "1");
        System.setProperty("threadmill.soak.nodeChurn", "off");
        System.setProperty("threadmill.soak.progressInterval", "1s");
        System.setProperty("threadmill.soak.outputDir", outputDir.toString());
        System.setProperty("threadmill.soak.runId", "endurance-smoke-test");
        try {
            EnduranceConfig config = EnduranceConfig.fromSystemProperties();
            assertThat(config.childLabels()).containsExactly("memory", "memory-2");

            int exitCode = EnduranceMain.run(config);
            assertThat(exitCode).isZero();

            for (String label : config.childLabels()) {
                assertThat(outputDir.resolve(label).resolve("summary.json")).exists();
                assertThat(outputDir.resolve(label).resolve("trace.jsonl")).exists();
                assertThat(outputDir.resolve(label).resolve("invariants.json")).exists();
                assertThat(outputDir.resolve(label).resolve("progress.json")).exists();
                assertThat(outputDir.resolve(label + "-console.log")).exists();
            }
            assertThat(outputDir.resolve("endurance-config.json")).exists();
            assertThat(outputDir.resolve("endurance-summary.md")).exists();

            ObjectMapper mapper = new ObjectMapper();
            JsonNode summary = mapper.readTree(Files.readString(outputDir.resolve("endurance-summary.json")));
            assertThat(summary.path("verdict").asText()).isEqualTo("passed");
            assertThat(summary.path("backends").path("memory").path("verdict").asText())
                    .isEqualTo("passed");
            assertThat(summary.path("backends")
                            .path("memory-2")
                            .path("totalEnqueued")
                            .asLong())
                    .isPositive();
        } finally {
            clearProps();
        }
    }

    @Test
    void unknownBackendIsRejectedAtConfigTime() {
        System.setProperty("threadmill.soak.backends", "postgres,mysql");
        try {
            assertThatThrownBy(EnduranceConfig::fromSystemProperties)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("mysql");
        } finally {
            clearProps();
        }
    }

    @Test
    void enduranceDefaultsMatchTheSignOffProfile() {
        try {
            EnduranceConfig config = EnduranceConfig.fromSystemProperties();
            assertThat(config.backends()).containsExactly("postgres", "redis");
            assertThat(config.duration().toHours()).isEqualTo(8);
            assertThat(config.jobsPerSecond()).isEqualTo(50);
            assertThat(config.nodes()).isEqualTo(3);
            assertThat(config.nodeChurn()).hasValue(Duration.ofMinutes(10));
            assertThat(config.scenario()).isEqualTo("mixed-workload");
            assertThat(config.failFast()).isTrue();
        } finally {
            clearProps();
        }
    }

    private static void clearProps() {
        for (String k : new String[] {
            "backends",
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
            "redisUrl",
            "progressInterval",
            "nodeChurn"
        }) {
            System.clearProperty("threadmill.soak." + k);
        }
    }
}
