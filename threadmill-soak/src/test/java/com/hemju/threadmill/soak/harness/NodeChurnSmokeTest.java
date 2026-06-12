package com.hemju.threadmill.soak.harness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Node churn must lose no work: jobs in flight on the churned node are
 * retried on the survivors, and the replacement node joins claiming. The
 * red path — churn configured with a single node — must be rejected before
 * the run starts, because churning the only node would silently pause the
 * whole cluster between replacement cycles.
 */
final class NodeChurnSmokeTest {

    @Test
    void churnedRunDrainsCompletelyAndRecordsTheChurnCycles(@TempDir Path tempDir) throws Exception {
        Path outputDir = tempDir.resolve("churn-smoke");
        System.setProperty("threadmill.soak.scenario", "rw-lock-stress");
        System.setProperty("threadmill.soak.duration", "6s");
        System.setProperty("threadmill.soak.jobsPerSecond", "25");
        System.setProperty("threadmill.soak.workerCount", "4");
        System.setProperty("threadmill.soak.nodes", "2");
        System.setProperty("threadmill.soak.nodeChurn", "2s");
        System.setProperty("threadmill.soak.outputDir", outputDir.toString());
        System.setProperty("threadmill.soak.runId", "churn-smoke-test");
        try {
            SoakHarnessConfig config = SoakHarnessConfig.fromSystemProperties("memory");
            OutputDir dir = new OutputDir(config.outputDir(), config.force());
            SummaryReport report;
            try (BackendFixture fixture = new MemoryHarnessFixture()) {
                report = new SoakHarnessRunner(config, fixture, dir, "soakMemory").run();
            }
            assertThat(report.verdict())
                    .as("churned run verdict — violations: %s", report.invariantResults())
                    .isEqualTo("passed");

            String trace = Files.readString(outputDir.resolve("trace.jsonl"));
            assertThat(trace).contains("\"event\":\"node_churn_stop\"");
            assertThat(trace).contains("\"reason\":\"churn\"");
            assertThat(trace).contains("\"reason\":\"churn-replacement\"");
        } finally {
            clearSoakProps();
        }
    }

    @Test
    void churnWithASingleNodeIsRejectedAtConfigTime() {
        System.setProperty("threadmill.soak.nodes", "1");
        System.setProperty("threadmill.soak.nodeChurn", "10s");
        try {
            assertThatThrownBy(() -> SoakHarnessConfig.fromSystemProperties("memory"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("nodeChurn");
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
            "redisUrl",
            "progressInterval",
            "nodeChurn"
        }) {
            System.clearProperty("threadmill.soak." + k);
        }
    }
}
