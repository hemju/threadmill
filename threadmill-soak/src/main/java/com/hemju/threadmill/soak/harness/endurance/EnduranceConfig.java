package com.hemju.threadmill.soak.harness.endurance;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import com.hemju.threadmill.soak.harness.SoakHarnessConfig;

/**
 * Resolved configuration for one endurance run: the same {@code -P} knobs as
 * the per-backend harness, with defaults sized for an overnight
 * production-readiness sign-off — eight hours of mixed workload at a moderate
 * 50 jobs/second on three nodes per backend, churning one node every ten
 * minutes, against PostgreSQL and Redis in parallel.
 *
 * <p>{@code -Pbackends} exists mainly so the orchestrator can be exercised
 * cheaply ({@code memory,memory} in tests); production sign-off uses the
 * default pair.
 */
public record EnduranceConfig(
        List<String> backends,
        String scenario,
        Duration duration,
        int jobsPerSecond,
        int producers,
        int workerCount,
        int nodes,
        Optional<Duration> nodeChurn,
        boolean failFast,
        Duration progressInterval,
        Optional<String> postgresUrl,
        Optional<String> redisUrl,
        Path outputDir,
        String runId,
        boolean force) {

    private static final Set<String> KNOWN_BACKENDS = Set.of("memory", "postgres", "redis");

    public static EnduranceConfig fromSystemProperties() {
        List<String> backends = new ArrayList<>();
        for (String raw : prop("backends", "postgres,redis").split(",")) {
            String backend = raw.trim().toLowerCase(Locale.ROOT);
            if (backend.isEmpty()) continue;
            if (!KNOWN_BACKENDS.contains(backend)) {
                throw new IllegalArgumentException(
                        "unknown backend in -Pbackends: '" + backend + "' — valid: " + KNOWN_BACKENDS);
            }
            backends.add(backend);
        }
        if (backends.isEmpty()) {
            throw new IllegalArgumentException("-Pbackends must name at least one backend");
        }

        String scenario = prop("scenario", "mixed-workload");
        Duration duration = SoakHarnessConfig.parseDuration(prop("duration", "8h"));
        int jobsPerSecond = Integer.parseInt(prop("jobsPerSecond", "50"));
        int producers = Integer.parseInt(prop("producers", "1"));
        int workerCount = Integer.parseInt(prop("workerCount", "8"));
        int nodes = Integer.parseInt(prop("nodes", "3"));
        String churnText = prop("nodeChurn", "10m");
        Optional<Duration> nodeChurn = "off".equalsIgnoreCase(churnText)
                ? Optional.empty()
                : Optional.of(SoakHarnessConfig.parseDuration(churnText));
        if (nodeChurn.isPresent() && nodes < 2) {
            throw new IllegalArgumentException(
                    "-PnodeChurn requires -Pnodes=2 or more — pass -PnodeChurn=off to disable churn");
        }
        boolean failFast = Boolean.parseBoolean(prop("failFast", "true"));
        Duration progressInterval = SoakHarnessConfig.parseDuration(prop("progressInterval", "30s"));
        Optional<String> postgresUrl = Optional.ofNullable(prop("postgresUrl", null));
        Optional<String> redisUrl = Optional.ofNullable(prop("redisUrl", null));
        boolean force = Boolean.parseBoolean(prop("force", "false"));
        String runId = Optional.ofNullable(prop("runId", null))
                .orElseGet(() -> "endurance-" + SoakHarnessConfig.RUN_TIMESTAMP.format(Instant.now()));
        Path outputDir = Optional.ofNullable(prop("outputDir", null))
                .map(Path::of)
                .orElseGet(() -> Path.of("build", "soak", runId));
        return new EnduranceConfig(
                List.copyOf(backends),
                scenario,
                duration,
                jobsPerSecond,
                producers,
                workerCount,
                nodes,
                nodeChurn,
                failFast,
                progressInterval,
                postgresUrl,
                redisUrl,
                outputDir,
                runId,
                force);
    }

    /**
     * One child label per backend, deduplicated in order ({@code memory},
     * {@code memory-2}, …) so a doubled backend never collides on its
     * artifact subdirectory.
     */
    public List<String> childLabels() {
        var labels = new ArrayList<String>(backends.size());
        for (String backend : backends) {
            String label = backend;
            int n = 2;
            while (labels.contains(label)) label = backend + "-" + n++;
            labels.add(label);
        }
        return labels;
    }

    private static String prop(String name, String defaultValue) {
        String value = System.getProperty("threadmill.soak." + name);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }
}
