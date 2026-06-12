package com.hemju.threadmill.soak.harness.endurance;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.soak.harness.OutputDir;
import com.hemju.threadmill.soak.harness.SoakHarnessMain;

/**
 * Entry point for the {@code soakEndurance} Gradle task: one harness JVM per
 * backend, running the same scenario for the same wall-clock window
 * <em>in parallel</em> — the production-readiness shape of "run it against
 * Redis and PostgreSQL simultaneously for hours".
 *
 * <p>Each child is the unmodified {@link SoakHarnessMain}, so every
 * per-backend artifact contract (trace, metrics, latencies, invariants,
 * progress, summary) holds verbatim under {@code <outputDir>/<backend>/}. A
 * child crash or fail-fast abort cannot disturb the other backend's run; the
 * orchestrator keeps supervising the survivor. While the run is live the
 * orchestrator prints a combined status line built from the children's
 * {@code progress.json} files, and at the end it collates both summaries into
 * {@code endurance-summary.json} / {@code endurance-summary.md}.
 *
 * <p>Note on performance numbers: both stacks share one machine, so the
 * throughput/latency figures are comparable <em>to each other under equal
 * contention</em>, not absolute capacity measurements.
 *
 * <p>Exit codes: 0 when every backend passes, 1 when any fails, 2 on bad
 * input, 3 on orchestrator-internal error.
 */
public final class EnduranceMain {

    private static final Logger LOG = LoggerFactory.getLogger(EnduranceMain.class);
    private static final Duration STATUS_PRINT_INTERVAL = Duration.ofSeconds(60);

    private EnduranceMain() {}

    static void main(String[] args) {
        try {
            EnduranceConfig config = EnduranceConfig.fromSystemProperties();
            System.exit(run(config));
        } catch (IllegalArgumentException e) {
            System.err.println("endurance harness: " + e.getMessage());
            System.exit(2);
        } catch (Exception e) {
            LOG.error("endurance harness failed", e);
            System.exit(3);
        }
    }

    /** Orchestrates one endurance run; separated from {@code main} so tests avoid {@code System.exit}. */
    public static int run(EnduranceConfig config) throws IOException, InterruptedException {
        Instant startedAt = Instant.now();
        new OutputDir(config.outputDir(), config.force()).prepare();
        writeConfig(config);

        List<ChildRun> children = new ArrayList<>();
        List<String> labels = config.childLabels();
        for (int i = 0; i < config.backends().size(); i++) {
            children.add(startChild(config, config.backends().get(i), labels.get(i)));
        }
        Thread killChildren = new Thread(() -> children.forEach(c -> c.process.destroy()), "endurance-shutdown");
        Runtime.getRuntime().addShutdownHook(killChildren);

        try {
            supervise(children);
        } finally {
            Runtime.getRuntime().removeShutdownHook(killChildren);
        }

        EnduranceSummary summary = EnduranceSummary.collate(config, children, startedAt);
        summary.write(config.outputDir());
        LOG.info("Endurance run finished. Output: {} verdict={}", config.outputDir(), summary.verdict());
        return "passed".equals(summary.verdict()) ? 0 : 1;
    }

    private static ChildRun startChild(EnduranceConfig config, String backend, String label) throws IOException {
        Path childDir = config.outputDir().resolve(label);
        var command = new ArrayList<String>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        addProp(command, "scenario", config.scenario());
        addProp(command, "duration", config.duration().toMillis() + "ms");
        addProp(command, "jobsPerSecond", Integer.toString(config.jobsPerSecond()));
        addProp(command, "workerCount", Integer.toString(config.workerCount()));
        addProp(command, "nodes", Integer.toString(config.nodes()));
        addProp(command, "failFast", Boolean.toString(config.failFast()));
        addProp(command, "progressInterval", config.progressInterval().toMillis() + "ms");
        addProp(command, "outputDir", childDir.toString());
        addProp(command, "runId", config.runId() + "-" + label);
        config.nodeChurn().ifPresent(churn -> addProp(command, "nodeChurn", churn.toMillis() + "ms"));
        if ("postgres".equals(backend)) {
            config.postgresUrl().ifPresent(url -> addProp(command, "postgresUrl", url));
        }
        if ("redis".equals(backend)) {
            config.redisUrl().ifPresent(url -> addProp(command, "redisUrl", url));
        }
        command.add(SoakHarnessMain.class.getName());
        command.add("--backend");
        command.add(backend);

        File consoleLog = config.outputDir().resolve(label + "-console.log").toFile();
        Process process = new ProcessBuilder(command)
                .redirectOutput(consoleLog)
                .redirectErrorStream(true)
                .start();
        LOG.info("started {} child (pid {}) → {}", label, process.pid(), childDir);
        return new ChildRun(label, backend, childDir, process);
    }

    private static void addProp(List<String> command, String name, String value) {
        command.add("-Dthreadmill.soak." + name + "=" + value);
    }

    private static void supervise(List<ChildRun> children) throws InterruptedException {
        var mapper = new ObjectMapper();
        Instant nextStatusAt = Instant.now().plus(STATUS_PRINT_INTERVAL);
        while (children.stream().anyMatch(c -> c.process.isAlive())) {
            Thread.sleep(1_000);
            for (ChildRun child : children) {
                if (!child.process.isAlive() && !child.exitLogged) {
                    child.exitLogged = true;
                    int code = child.process.exitValue();
                    LOG.info("{} child exited with code {}", child.label, code);
                }
            }
            if (!Instant.now().isBefore(nextStatusAt)) {
                nextStatusAt = Instant.now().plus(STATUS_PRINT_INTERVAL);
                for (ChildRun child : children) {
                    LOG.info("{}", statusLine(mapper, child));
                }
            }
        }
    }

    private static String statusLine(ObjectMapper mapper, ChildRun child) {
        if (!child.process.isAlive()) {
            return child.label + ": exited (code " + child.process.exitValue() + ")";
        }
        Optional<JsonNode> progress = readJson(mapper, child.outputDir.resolve("progress.json"));
        if (progress.isEmpty()) {
            return child.label + ": starting (no progress.json yet)";
        }
        JsonNode p = progress.get();
        JsonNode counts = p.path("counts");
        long invariantViolations = 0;
        for (JsonNode inv : p.path("invariants")) {
            invariantViolations += inv.path("violationCount").asLong(0);
        }
        return String.format(
                "%s: %s %dm/%dm enqueued=%d succeeded=%d failed=%d inflight=%d p99=%dms invariantViolations=%d",
                child.label,
                p.path("phase").asText("?"),
                Duration.ofMillis(p.path("elapsedMs").asLong(0)).toMinutes(),
                Duration.ofMillis(p.path("targetDurationMs").asLong(0)).toMinutes(),
                counts.path("enqueued").asLong(0),
                counts.path("succeeded").asLong(0),
                counts.path("failed").asLong(0),
                p.path("inflight").asInt(0),
                p.path("endToEndP99Ms").asLong(0),
                invariantViolations);
    }

    static Optional<JsonNode> readJson(ObjectMapper mapper, Path file) {
        if (!Files.exists(file)) return Optional.empty();
        try {
            return Optional.of(mapper.readTree(Files.readString(file, StandardCharsets.UTF_8)));
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    private static void writeConfig(EnduranceConfig config) throws IOException {
        var mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("runId", config.runId());
        m.put("backends", config.backends());
        m.put("scenario", config.scenario());
        m.put("duration", config.duration().toString());
        m.put("durationMillis", config.duration().toMillis());
        m.put("jobsPerSecond", config.jobsPerSecond());
        m.put("workerCount", config.workerCount());
        m.put("nodes", config.nodes());
        m.put("nodeChurn", config.nodeChurn().map(Duration::toString).orElse(null));
        m.put("failFast", config.failFast());
        m.put("progressInterval", config.progressInterval().toString());
        m.put("postgresUrl", config.postgresUrl().orElse(null));
        m.put("redisUrl", config.redisUrl().orElse(null));
        m.put("outputDir", config.outputDir().toString());
        Files.writeString(
                config.outputDir().resolve("endurance-config.json"),
                mapper.writeValueAsString(m),
                StandardCharsets.UTF_8);
    }

    /** One supervised child harness process. */
    static final class ChildRun {
        final String label;
        final String backend;
        final Path outputDir;
        final Process process;
        boolean exitLogged;

        ChildRun(String label, String backend, Path outputDir, Process process) {
            this.label = label;
            this.backend = backend;
            this.outputDir = outputDir;
            this.process = process;
        }
    }
}
