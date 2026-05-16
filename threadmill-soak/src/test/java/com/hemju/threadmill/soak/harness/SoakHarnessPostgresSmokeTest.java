package com.hemju.threadmill.soak.harness;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Optional;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end smoke against Postgres 18 via Testcontainers. Tagged {@code soak}
 * — runs in the fixed {@code :soakRegression} task, not on every {@code check}.
 */
@Tag("soak")
final class SoakHarnessPostgresSmokeTest {

    @Test
    void postgresSmokeProducesPassedVerdict(@TempDir Path tempDir) throws Exception {
        Path outputDir = tempDir.resolve("postgres-smoke");
        System.setProperty("threadmill.soak.scenario", "mixed-workload");
        System.setProperty("threadmill.soak.duration", "10s");
        System.setProperty("threadmill.soak.jobsPerSecond", "60");
        System.setProperty("threadmill.soak.workerCount", "4");
        System.setProperty("threadmill.soak.nodes", "1");
        System.setProperty("threadmill.soak.outputDir", outputDir.toString());
        System.setProperty("threadmill.soak.runId", "pg-smoke");
        try {
            SoakHarnessConfig config = SoakHarnessConfig.fromSystemProperties("postgres");
            OutputDir dir = new OutputDir(config.outputDir(), config.force());
            SummaryReport report;
            try (BackendFixture fixture = new PostgresHarnessFixture(Optional.empty())) {
                report = new SoakHarnessRunner(config, fixture, dir, "soakPostgres").run();
            }
            assertThat(report.verdict()).isEqualTo("passed");
            assertThat(outputDir.resolve("summary.json")).exists();
        } finally {
            clearProps();
        }
    }

    private static void clearProps() {
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
            "redisTopology"
        }) {
            System.clearProperty("threadmill.soak." + k);
        }
    }
}
