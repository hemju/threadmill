package com.hemju.threadmill.soak.harness;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.schedule.Scheduler;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.soak.harness.invariant.InvariantResult;
import com.hemju.threadmill.soak.harness.invariant.SoakInvariant;
import com.hemju.threadmill.soak.harness.invariant.TraceCorpus;
import com.hemju.threadmill.soak.harness.scenario.Scenarios;
import com.hemju.threadmill.soak.harness.scenario.SoakRunContext;
import com.hemju.threadmill.soak.harness.scenario.SoakScenario;

/**
 * Orchestrator for one soak run.
 *
 * <p>Build the store via the {@link BackendFixture}, start N
 * {@link ProcessingNode}s, instantiate the scenario, run its load generator
 * for {@code duration}, wait for drain, stop nodes, then load the trace,
 * run invariants, and write all artifacts.
 */
public final class SoakHarnessRunner {

    private static final Logger LOG = LoggerFactory.getLogger(SoakHarnessRunner.class);

    private final SoakHarnessConfig config;
    private final BackendFixture fixture;
    private final OutputDir outputDir;
    private final String taskLabel;

    public SoakHarnessRunner(SoakHarnessConfig config, BackendFixture fixture, OutputDir outputDir, String taskLabel) {
        this.config = Objects.requireNonNull(config, "config");
        this.fixture = Objects.requireNonNull(fixture, "fixture");
        this.outputDir = Objects.requireNonNull(outputDir, "outputDir");
        this.taskLabel = Objects.requireNonNull(taskLabel, "taskLabel");
    }

    public SummaryReport run() throws Exception {
        outputDir.prepare();
        RunConfigWriter.write(config, outputDir);

        SoakScenario scenario = Scenarios.of(config.scenario());
        JobStore store = fixture.store();
        Scheduler scheduler = new Scheduler(store, new JsonJobSerializer());
        Instant runStart = Instant.now();

        List<ProcessingNode> nodes = new ArrayList<>();
        SoakTraceWriter trace = new SoakTraceWriter(outputDir.traceJsonl());
        LatencyTracker latency = new LatencyTracker(outputDir.latenciesJsonl());
        MetricsSampler metrics = new MetricsSampler(outputDir.metricsJsonl(), store, latency);
        SoakInterceptor interceptor = new SoakInterceptor(trace, latency);
        LoadGenerator gen = new LoadGenerator(scheduler, trace, latency, config.jobsPerSecond());
        StdoutStatusPrinter printer = new StdoutStatusPrinter(
                taskLabel, config.scenario(), config.runId(), metrics, interceptor, gen.enqueuedCount());

        try {
            ProcessingNodeConfig baseConfig = scenario.tuneConfig(ProcessingNodeConfig.builder()
                            .workerCount(config.workerCount())
                            .pollInterval(Duration.ofMillis(25))
                            .claimHeartbeat(Duration.ofMillis(200))
                            .heartbeatTimeout(Duration.ofSeconds(5))
                            .maintenanceLeaseDuration(Duration.ofSeconds(2))
                            .jobTimeout(Duration.ofSeconds(30))
                            .defaultMaxAttempts(3)
                            .retryInitialBackoff(Duration.ofMillis(50))
                            .claimBatchSize(16)
                            .storeOutagePollInterval(Duration.ofMillis(200)))
                    .build();

            for (int i = 0; i < config.nodes(); i++) {
                var nb = ProcessingNode.builder(store).config(baseConfig).interceptor(interceptor);
                scenario.configureNode(nb);
                ProcessingNode node = nb.build();
                trace.emit(
                        "node_started",
                        Map.of(
                                "nodeId", node.nodeId().toString(),
                                "workerCount", config.workerCount(),
                                "backend", config.backend()));
                node.start();
                nodes.add(node);
            }

            metrics.start();
            printer.start();

            SoakRunContext ctx = new SoakRunContext(config, store, trace, runStart, () -> List.copyOf(nodes));
            scenario.runWorkload(gen, ctx);
            waitForDrain(store, scenario.drainBudget());

            for (ProcessingNode node : new ArrayList<>(nodes)) {
                trace.emit("node_stopped", Map.of("nodeId", node.nodeId().toString()));
                node.close();
            }
            nodes.clear();
        } finally {
            printer.close();
            try {
                metrics.close();
            } catch (IOException ignore) {
                // best effort
            }
            for (ProcessingNode node : nodes) {
                try {
                    node.close();
                } catch (RuntimeException ignore) {
                    // best effort — the runner is exiting
                }
            }
            try {
                trace.close();
            } catch (IOException ignore) {
                // best effort
            }
            try {
                latency.close();
            } catch (IOException ignore) {
                // best effort
            }
        }

        return finishRun(interceptor, runStart);
    }

    private SummaryReport finishRun(SoakInterceptor interceptor, Instant runStart) throws IOException {
        TraceCorpus corpus = TraceCorpus.load(outputDir.traceJsonl());

        SoakScenario scenario = Scenarios.of(config.scenario());
        List<SoakInvariant> invariants = scenario.invariants();
        List<InvariantResult> results = new ArrayList<>();
        for (SoakInvariant inv : invariants) {
            try {
                results.add(inv.check(corpus));
            } catch (RuntimeException e) {
                results.add(InvariantResult.fail(
                        inv.name(),
                        List.of("invariant raised " + e.getClass().getSimpleName() + ": " + e.getMessage()),
                        List.of()));
            }
        }
        SummaryWriter.writeInvariants(outputDir, results);

        SummaryReport.LockContention lockContention = LockEventsWriter.write(corpus, outputDir);

        SummaryReport.Performance perf =
                buildPerformance(corpus, interceptor, Duration.between(runStart, Instant.now()), lockContention);

        boolean passed = results.stream().allMatch(InvariantResult::passed);
        SummaryReport report = new SummaryReport(
                config.runId(),
                config.scenario(),
                config.backend(),
                RunConfigWriter.asMap(config),
                passed ? "passed" : "failed",
                results,
                perf,
                List.of("Generated by threadmill-soak harness. Drop this directory into an AI agent to analyse."));

        JsonSchema schema = SummarySchema.load();
        List<String> schemaErrors = SummarySchema.validate(schema, report);
        if (!schemaErrors.isEmpty()) {
            LOG.error("summary.json failed schema validation: {}", schemaErrors);
        }
        new SummaryWriter().write(report, outputDir);
        return report;
    }

    private SummaryReport.Performance buildPerformance(
            TraceCorpus corpus,
            SoakInterceptor interceptor,
            Duration wallClock,
            SummaryReport.LockContention contention) {
        long enqueued = corpus.events().stream()
                .filter(e -> "enqueued".equals(e.path("event").asText()))
                .count();
        long durationMs = Math.max(1L, wallClock.toMillis());
        double overall = interceptor.succeeded() * 1000.0 / durationMs;

        Map<String, Double> byQueue = new LinkedHashMap<>();
        Map<String, Long> succeededPerQueue = new LinkedHashMap<>();
        Map<String, Long> succeededPerHandler = new LinkedHashMap<>();
        Map<String, String> handlerByJobId = new HashMap<>();
        for (JsonNode e : corpus.events()) {
            String event = e.path("event").asText();
            if ("enqueued".equals(event)) {
                String h = e.path("handler").asText("");
                String j = e.path("jobId").asText("");
                if (!h.isEmpty() && !j.isEmpty()) handlerByJobId.put(j, h);
            } else if ("succeeded".equals(event)) {
                String q = e.path("queue").asText("");
                String j = e.path("jobId").asText("");
                if (!q.isEmpty()) succeededPerQueue.merge(q, 1L, Long::sum);
                String h = handlerByJobId.get(j);
                if (h != null) succeededPerHandler.merge(h, 1L, Long::sum);
            }
        }
        succeededPerQueue.forEach((q, c) -> byQueue.put(q, c * 1000.0 / durationMs));
        Map<String, Double> byHandler = new LinkedHashMap<>();
        succeededPerHandler.forEach((h, c) -> byHandler.put(h, c * 1000.0 / durationMs));

        // Read latency percentiles from latencies.jsonl after it's been closed.
        var latencyByStage = latencyPercentilesFromFile();
        return new SummaryReport.Performance(
                enqueued,
                interceptor.succeeded(),
                interceptor.failed(),
                interceptor.timedOut(),
                interceptor.quarantined(),
                interceptor.retried(),
                wallClock.toMillis(),
                new SummaryReport.ThroughputBreakdown(overall, byQueue, byHandler),
                new SummaryReport.LatencyBreakdown(
                        latencyByStage.getOrDefault("enqueueToClaimMs", Percentiles.Summary.empty()),
                        latencyByStage.getOrDefault("claimToStartMs", Percentiles.Summary.empty()),
                        latencyByStage.getOrDefault("startToCompleteMs", Percentiles.Summary.empty()),
                        latencyByStage.getOrDefault("endToEndMs", Percentiles.Summary.empty())),
                contention);
    }

    /**
     * Read latencies.jsonl from disk and compute percentiles per stage. We
     * do this after the file is closed (the in-memory tracker was already
     * closed by run()'s finally block) so the summary always reflects the
     * exact file an AI agent would see.
     */
    private Map<String, Percentiles.Summary> latencyPercentilesFromFile() {
        Map<String, Percentiles.Summary> result = new LinkedHashMap<>();
        ArrayList<Long> enqueueToClaim = new ArrayList<>();
        ArrayList<Long> claimToStart = new ArrayList<>();
        ArrayList<Long> startToComplete = new ArrayList<>();
        ArrayList<Long> endToEnd = new ArrayList<>();
        try (var lines = Files.lines(outputDir.latenciesJsonl())) {
            var mapper = new ObjectMapper();
            lines.forEach(line -> {
                if (line.isBlank()) return;
                try {
                    JsonNode n = mapper.readTree(line);
                    pushIfPresent(n, "enqueueToClaimMs", enqueueToClaim);
                    pushIfPresent(n, "claimToStartMs", claimToStart);
                    pushIfPresent(n, "startToCompleteMs", startToComplete);
                    pushIfPresent(n, "endToEndMs", endToEnd);
                } catch (IOException e) {
                    // Skip malformed lines; the file is append-only and a
                    // truncated write at process end would be the only source.
                }
            });
        } catch (IOException ignore) {
            // Empty file or missing — leave all percentiles at zero.
        }
        result.put("enqueueToClaimMs", Percentiles.summarise(toArray(enqueueToClaim)));
        result.put("claimToStartMs", Percentiles.summarise(toArray(claimToStart)));
        result.put("startToCompleteMs", Percentiles.summarise(toArray(startToComplete)));
        result.put("endToEndMs", Percentiles.summarise(toArray(endToEnd)));
        return result;
    }

    private static void pushIfPresent(JsonNode n, String field, ArrayList<Long> dest) {
        JsonNode v = n.get(field);
        if (v != null && !v.isNull()) dest.add(v.asLong());
    }

    private static long[] toArray(ArrayList<Long> list) {
        long[] arr = new long[list.size()];
        for (int i = 0; i < list.size(); i++) arr[i] = list.get(i);
        return arr;
    }

    private void waitForDrain(JobStore store, Duration drainBudget) throws InterruptedException {
        Instant deadline = Instant.now().plus(drainBudget);
        while (Instant.now().isBefore(deadline)) {
            long active = activeCount(store);
            if (active == 0) return;
            Thread.sleep(200);
        }
        long active = activeCount(store);
        if (active > 0) {
            LOG.warn("drain budget elapsed with {} active jobs remaining", active);
        }
    }

    private static long activeCount(JobStore store) {
        Map<JobState, Long> c = store.countsByState();
        long a = 0;
        for (JobState s : List.of(JobState.ENQUEUED, JobState.SCHEDULED, JobState.AWAITING, JobState.PROCESSING)) {
            a += c.getOrDefault(s, 0L);
        }
        return a;
    }
}
