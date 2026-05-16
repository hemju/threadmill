package com.hemju.threadmill.soak.harness;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import com.hemju.threadmill.soak.harness.invariant.InvariantResult;

/**
 * Writes the two summary artifacts: {@code summary.json} (machine-readable,
 * matches the schema) and {@code summary.md} (human-readable, mirrors the
 * JSON shape in Markdown).
 */
public final class SummaryWriter {

    private final ObjectMapper mapper;

    public SummaryWriter() {
        this.mapper =
                new ObjectMapper().registerModule(new JavaTimeModule()).enable(SerializationFeature.INDENT_OUTPUT);
    }

    public void write(SummaryReport report, OutputDir dir) throws IOException {
        Objects.requireNonNull(report, "report");
        Objects.requireNonNull(dir, "dir");
        Files.writeString(dir.summaryJson(), mapper.writeValueAsString(report.asMap()), StandardCharsets.UTF_8);
        Files.writeString(dir.summaryMd(), renderMarkdown(report), StandardCharsets.UTF_8);
    }

    private static String renderMarkdown(SummaryReport r) {
        var sb = new StringBuilder();
        sb.append("# Soak run summary\n\n");
        sb.append("- **Run id:** `").append(r.runId()).append("`\n");
        sb.append("- **Scenario:** `").append(r.scenario()).append("`\n");
        sb.append("- **Backend:** `").append(r.backend()).append("`\n");
        sb.append("- **Verdict:** **").append(r.verdict()).append("**\n\n");

        sb.append("## Invariants\n\n");
        sb.append("| Check | Outcome | Violations |\n|---|---|---|\n");
        for (InvariantResult inv : r.invariantResults()) {
            sb.append("| `")
                    .append(inv.name())
                    .append("` | ")
                    .append(inv.passed() ? "passed" : "**FAILED**")
                    .append(" | ")
                    .append(inv.violations().size())
                    .append(" |\n");
        }
        for (InvariantResult inv : r.invariantResults()) {
            if (!inv.passed()) {
                sb.append("\n### ! ").append(inv.name()).append("\n\n");
                for (String v : inv.violations()) {
                    sb.append("- ").append(v).append("\n");
                }
            }
        }

        sb.append("\n## Performance\n\n");
        var p = r.performance();
        sb.append("- **Wall clock:** ").append(p.wallClockDurationMs()).append(" ms\n");
        sb.append("- **Enqueued:** ").append(p.totalEnqueued()).append("\n");
        sb.append("- **Succeeded:** ").append(p.totalSucceeded()).append("\n");
        sb.append("- **Failed / timed-out / quarantined:** ")
                .append(p.totalFailed())
                .append(" / ")
                .append(p.totalTimedOut())
                .append(" / ")
                .append(p.totalQuarantined())
                .append("\n");
        sb.append("- **Retried:** ").append(p.totalRetried()).append("\n");
        sb.append("- **Throughput overall:** ")
                .append(String.format("%.1f", p.throughputJobsPerSec().overall()))
                .append(" jobs/sec\n");

        sb.append("\n### Latency (ms)\n\n");
        sb.append("| Stage | p50 | p95 | p99 | max | count |\n|---|---|---|---|---|---|\n");
        appendRow(sb, "enqueueToClaim", p.latencyMs().enqueueToClaim());
        appendRow(sb, "claimToStart", p.latencyMs().claimToStart());
        appendRow(sb, "startToComplete", p.latencyMs().startToComplete());
        appendRow(sb, "endToEnd", p.latencyMs().endToEnd());

        if (!p.throughputJobsPerSec().byQueue().isEmpty()) {
            sb.append("\n### Throughput by queue\n\n");
            sb.append("| Queue | Jobs/sec |\n|---|---|\n");
            for (Map.Entry<String, Double> e :
                    p.throughputJobsPerSec().byQueue().entrySet()) {
                sb.append("| `")
                        .append(e.getKey())
                        .append("` | ")
                        .append(String.format("%.2f", e.getValue()))
                        .append(" |\n");
            }
        }
        if (!p.lockContention().byKey().isEmpty()) {
            sb.append("\n### Lock contention by key\n\n");
            sb.append("| Key | Acquires | Max shared | Exclusive | Avg wait (ms) | p99 wait (ms) |\n");
            sb.append("|---|---|---|---|---|---|\n");
            for (var e : p.lockContention().byKey().entrySet()) {
                var s = e.getValue();
                sb.append("| `")
                        .append(e.getKey())
                        .append("` | ")
                        .append(s.acquires())
                        .append(" | ")
                        .append(s.maxConcurrentShared())
                        .append(" | ")
                        .append(s.exclusiveCount())
                        .append(" | ")
                        .append(s.avgWaitMs())
                        .append(" | ")
                        .append(s.p99WaitMs())
                        .append(" |\n");
            }
        }
        if (!r.notes().isEmpty()) {
            sb.append("\n## Notes\n\n");
            for (String n : r.notes()) sb.append("- ").append(n).append("\n");
        }
        return sb.toString();
    }

    private static void appendRow(StringBuilder sb, String name, Percentiles.Summary s) {
        sb.append("| ")
                .append(name)
                .append(" | ")
                .append(s.p50())
                .append(" | ")
                .append(s.p95())
                .append(" | ")
                .append(s.p99())
                .append(" | ")
                .append(s.max())
                .append(" | ")
                .append(s.count())
                .append(" |\n");
    }

    public static void writeInvariants(OutputDir dir, List<InvariantResult> results) throws IOException {
        var mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        Files.writeString(dir.invariantsJson(), mapper.writeValueAsString(results), StandardCharsets.UTF_8);
    }
}
