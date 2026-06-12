package com.hemju.threadmill.soak.harness;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Owns the on-disk artifact directory for one run.
 *
 * <p>A default output dir is timestamped per run so multiple manual invocations
 * never collide. An explicit {@code -PoutputDir} that already exists is
 * rejected unless {@code -Pforce=true} — protecting the operator from
 * accidentally overwriting a previous run's artifacts.
 */
public final class OutputDir {

    private final Path root;
    private final boolean force;

    public OutputDir(Path root, boolean force) {
        this.root = Objects.requireNonNull(root, "root");
        this.force = force;
    }

    public void prepare() throws IOException {
        if (Files.exists(root)) {
            if (!force) {
                throw new IOException("output dir already exists: " + root
                        + " — pass -Pforce=true to overwrite, or choose a different -PoutputDir");
            }
            deleteRecursively(root);
        }
        Files.createDirectories(root);
    }

    public Path root() {
        return root;
    }

    public Path configJson() {
        return root.resolve("config.json");
    }

    public Path traceJsonl() {
        return root.resolve("trace.jsonl");
    }

    public Path lockEventsJsonl() {
        return root.resolve("lock-events.jsonl");
    }

    public Path metricsJsonl() {
        return root.resolve("metrics.jsonl");
    }

    public Path latenciesJsonl() {
        return root.resolve("latencies.jsonl");
    }

    public Path invariantsJson() {
        return root.resolve("invariants.json");
    }

    public Path progressJson() {
        return root.resolve("progress.json");
    }

    public Path summaryJson() {
        return root.resolve("summary.json");
    }

    public Path summaryMd() {
        return root.resolve("summary.md");
    }

    public Path stdoutLog() {
        return root.resolve("stdout.log");
    }

    public Path stderrLog() {
        return root.resolve("stderr.log");
    }

    private static void deleteRecursively(Path path) throws IOException {
        try (Stream<Path> entries = Files.walk(path)) {
            entries.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    throw new RuntimeException("could not delete " + p, e);
                }
            });
        }
    }
}
