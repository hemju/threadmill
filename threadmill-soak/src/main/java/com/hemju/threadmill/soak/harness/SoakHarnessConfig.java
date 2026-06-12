package com.hemju.threadmill.soak.harness;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.Optional;

/**
 * Resolved configuration for one harness run.
 *
 * <p>Built by {@link #fromSystemProperties(String)} which reads every supported
 * {@code -P} property forwarded by the Gradle task as a {@code threadmill.soak.*}
 * system property. The JUnit smoke tests use the same code path by setting the
 * system properties directly before calling {@link SoakHarnessMain#main}.
 */
public record SoakHarnessConfig(
        String backend,
        String scenario,
        Duration duration,
        int jobsPerSecond,
        int workerCount,
        int nodes,
        Path outputDir,
        String runId,
        boolean failFast,
        Optional<String> postgresUrl,
        String redisTopology,
        Optional<String> redisUrl,
        boolean force,
        Duration progressInterval,
        Optional<Duration> nodeChurn) {

    public static final DateTimeFormatter RUN_TIMESTAMP = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4)
            .appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .appendLiteral('T')
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral('-')
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendLiteral('-')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .appendLiteral('Z')
            .toFormatter(Locale.ROOT)
            .withZone(ZoneOffset.UTC);

    public static SoakHarnessConfig fromSystemProperties(String backend) {
        String scenario = prop("scenario", "mixed-workload");
        Duration duration = parseDuration(prop("duration", "120s"));
        // Defaults are chosen to drain reliably on a developer laptop with
        // the in-memory store under any of the eight scenarios. Operators on
        // real backends or larger hardware will want to push -PjobsPerSecond
        // and -Pnodes higher; both are direct knobs.
        int jobsPerSecond = Integer.parseInt(prop("jobsPerSecond", "100"));
        int workerCount = Integer.parseInt(prop("workerCount", "8"));
        int nodes = Integer.parseInt(prop("nodes", "1"));
        boolean failFast = Boolean.parseBoolean(prop("failFast", "true"));
        boolean force = Boolean.parseBoolean(prop("force", "false"));
        Optional<String> postgresUrl = Optional.ofNullable(prop("postgresUrl", null));
        String redisTopology = prop("redisTopology", "standalone");
        Optional<String> redisUrl = Optional.ofNullable(prop("redisUrl", null));
        String runId = Optional.ofNullable(prop("runId", null))
                .orElseGet(() -> defaultRunId(scenario, backend, Instant.now()));
        Path outputDir = Optional.ofNullable(prop("outputDir", null))
                .map(Path::of)
                .orElseGet(() -> Path.of("build", "soak", runId));
        Duration progressInterval = parseDuration(prop("progressInterval", "30s"));
        Optional<Duration> nodeChurn =
                Optional.ofNullable(prop("nodeChurn", null)).map(SoakHarnessConfig::parseDuration);
        if (nodeChurn.isPresent() && nodes < 2) {
            throw new IllegalArgumentException(
                    "-PnodeChurn requires -Pnodes=2 or more — churn always keeps at least one node alive");
        }
        return new SoakHarnessConfig(
                backend,
                scenario,
                duration,
                jobsPerSecond,
                workerCount,
                nodes,
                outputDir,
                runId,
                failFast,
                postgresUrl,
                redisTopology,
                redisUrl,
                force,
                progressInterval,
                nodeChurn);
    }

    public static String defaultRunId(String scenario, String backend, Instant now) {
        return scenario + "-" + backend + "-" + RUN_TIMESTAMP.format(now);
    }

    private static String prop(String name, String defaultValue) {
        String value = System.getProperty("threadmill.soak." + name);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }

    /** Parses the harness duration shape: {@code 90ms} / {@code 30s} / {@code 5m} / {@code 8h} / bare seconds. */
    public static Duration parseDuration(String text) {
        String t = text.trim().toLowerCase(Locale.ROOT);
        if (t.endsWith("ms")) return Duration.ofMillis(Long.parseLong(t.substring(0, t.length() - 2)));
        if (t.endsWith("s")) return Duration.ofSeconds(Long.parseLong(t.substring(0, t.length() - 1)));
        if (t.endsWith("m")) return Duration.ofMinutes(Long.parseLong(t.substring(0, t.length() - 1)));
        if (t.endsWith("h")) return Duration.ofHours(Long.parseLong(t.substring(0, t.length() - 1)));
        // Bare number is seconds for ergonomics (matches gradle's -Pduration=30 expectation).
        return Duration.ofSeconds(Long.parseLong(t));
    }
}
