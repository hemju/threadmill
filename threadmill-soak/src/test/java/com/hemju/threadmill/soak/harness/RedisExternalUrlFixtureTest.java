package com.hemju.threadmill.soak.harness;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Optional;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * The {@code -PredisUrl} path must treat the instance as shared
 * infrastructure: reset only the {@code {threadmill}:*} namespace and never
 * {@code FLUSHDB} — an endurance operator points this at a long-lived Redis
 * that may hold unrelated data. Tagged {@code soak} (needs a container
 * runtime), like the other Redis harness tests.
 */
@Tag("soak")
final class RedisExternalUrlFixtureTest {

    @Test
    @SuppressWarnings("resource")
    void externalUrlRunResetsOnlyTheThreadmillNamespace(@TempDir Path tempDir) throws Exception {
        try (GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                .withExposedPorts(6379)
                .withCommand("redis-server", "--appendonly", "yes")
                .waitingFor(Wait.forListeningPort())) {
            redis.start();
            String url = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);

            RedisClient probe = RedisClient.create(RedisURI.create(url));
            try (var connection = probe.connect()) {
                // Unrelated tenant data plus a leftover Threadmill key from a
                // previous run — only the latter may be removed.
                connection.sync().set("unrelated:app:key", "must-survive");
                connection.sync().set("{threadmill}:stale:leftover", "must-be-removed");

                Path outputDir = tempDir.resolve("redis-external-smoke");
                System.setProperty("threadmill.soak.scenario", "rw-lock-stress");
                System.setProperty("threadmill.soak.duration", "5s");
                System.setProperty("threadmill.soak.jobsPerSecond", "25");
                System.setProperty("threadmill.soak.workerCount", "4");
                System.setProperty("threadmill.soak.nodes", "1");
                System.setProperty("threadmill.soak.outputDir", outputDir.toString());
                System.setProperty("threadmill.soak.runId", "redis-external-smoke");
                try {
                    SoakHarnessConfig config = SoakHarnessConfig.fromSystemProperties("redis");
                    OutputDir dir = new OutputDir(config.outputDir(), config.force());
                    SummaryReport report;
                    try (BackendFixture fixture = new RedisHarnessFixture("standalone", Optional.of(url))) {
                        assertThat(connection.sync().get("{threadmill}:stale:leftover"))
                                .as("stale Threadmill namespace is reset on fixture open")
                                .isNull();
                        report = new SoakHarnessRunner(config, fixture, dir, "soakRedis").run();
                    }
                    assertThat(report.verdict()).isEqualTo("passed");
                    assertThat(connection.sync().get("unrelated:app:key"))
                            .as("non-Threadmill keys on the shared instance survive the run")
                            .isEqualTo("must-survive");
                } finally {
                    clearProps();
                }
            } finally {
                probe.shutdown();
            }
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
            "redisTopology",
            "redisUrl",
            "progressInterval",
            "nodeChurn"
        }) {
            System.clearProperty("threadmill.soak." + k);
        }
    }
}
