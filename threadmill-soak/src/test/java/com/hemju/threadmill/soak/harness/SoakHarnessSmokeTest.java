package com.hemju.threadmill.soak.harness;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Smoke test for the load soak harness against the in-memory store.
 *
 * <p>Calls {@link SoakHarnessMain#main(String[])} via the same system-property
 * contract the Gradle tasks use, asserts the run completes with a {@code passed}
 * verdict, and confirms every output file is present.
 *
 * <p>Untagged so it runs under {@code ./gradlew check} — any regression that
 * breaks the harness output contract fails CI immediately.
 */
final class SoakHarnessSmokeTest {

    @Test
    void smokeRunOnMemoryProducesPassedVerdictAndAllArtifacts(@TempDir Path tempDir) throws Exception {
        Path outputDir = tempDir.resolve("soak-smoke");
        System.setProperty("threadmill.soak.scenario", "rw-lock-stress");
        // Keep this untagged smoke focused on the harness output contract. The
        // scenario still exercises lock events, but its producer is bounded so
        // the check gate does not inherit mixed-workload churn variance.
        System.setProperty("threadmill.soak.duration", "5s");
        System.setProperty("threadmill.soak.jobsPerSecond", "25");
        System.setProperty("threadmill.soak.nodes", "1");
        System.setProperty("threadmill.soak.workerCount", "4");
        System.setProperty("threadmill.soak.outputDir", outputDir.toString());
        System.setProperty("threadmill.soak.runId", "smoke-test");
        try {
            int exitCode = runHarnessInProcess("memory");
            assertThat(exitCode).isEqualTo(0);

            assertThat(outputDir.resolve("config.json")).exists();
            assertThat(outputDir.resolve("trace.jsonl")).exists();
            assertThat(outputDir.resolve("lock-events.jsonl")).exists();
            assertThat(outputDir.resolve("metrics.jsonl")).exists();
            assertThat(outputDir.resolve("latencies.jsonl")).exists();
            assertThat(outputDir.resolve("invariants.json")).exists();
            assertThat(outputDir.resolve("summary.json")).exists();
            assertThat(outputDir.resolve("summary.md")).exists();
            assertThat(outputDir.resolve("progress.json")).exists();

            ObjectMapper progressMapper = new ObjectMapper();
            JsonNode progress = progressMapper.readTree(Files.readString(outputDir.resolve("progress.json")));
            assertThat(progress.path("phase").asText()).isEqualTo("finished");
            assertThat(progress.path("counts").path("enqueued").asLong()).isPositive();
            assertThat(progress.path("invariants").isArray()).isTrue();
            assertThat(progress.path("aborted").asBoolean()).isFalse();

            ObjectMapper mapper = new ObjectMapper();
            JsonNode summary = mapper.readTree(Files.readString(outputDir.resolve("summary.json")));
            assertThat(summary.path("verdict").asText()).isEqualTo("passed");
            assertThat(summary.path("scenario").asText()).isEqualTo("rw-lock-stress");
            assertThat(summary.path("backend").asText()).isEqualTo("memory");

            String trace = Files.readString(outputDir.resolve("trace.jsonl"));
            assertThat(trace).contains("\"event\":\"enqueued\"");
            assertThat(trace).contains("\"event\":\"claimed\"");
            assertThat(trace).contains("\"event\":\"started\"");
            assertThat(trace).contains("\"event\":\"succeeded\"");
            assertThat(trace).contains("\"event\":\"lock_acquired\"");
            assertThat(trace).contains("\"event\":\"lock_released\"");
            assertThat(trace).contains("\"event\":\"node_started\"");
            assertThat(trace).contains("\"event\":\"node_stopped\"");
        } finally {
            clearSoakSystemProps();
        }
    }

    /**
     * Runs SoakHarnessMain without letting it call System.exit — we intercept
     * the exit code via a SecurityManager-free shim. Since SoakHarnessMain
     * calls System.exit directly, we run it via a child-process-style helper:
     * construct the runner and fixture by hand and exercise the same path.
     */
    private int runHarnessInProcess(String backend) throws Exception {
        SoakHarnessConfig config = SoakHarnessConfig.fromSystemProperties(backend);
        OutputDir dir = new OutputDir(config.outputDir(), config.force());
        try (BackendFixture fixture = new MemoryHarnessFixture()) {
            SoakHarnessRunner runner = new SoakHarnessRunner(config, fixture, dir, "soakMemory");
            SummaryReport report = runner.run();
            return "passed".equals(report.verdict()) ? 0 : 1;
        }
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
            "postgresUrl",
            "force",
            "redisTopology",
            "progressInterval"
        }) {
            System.clearProperty("threadmill.soak." + k);
        }
    }
}
