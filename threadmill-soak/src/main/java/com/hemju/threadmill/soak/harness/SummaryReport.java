package com.hemju.threadmill.soak.harness;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.hemju.threadmill.soak.harness.invariant.InvariantResult;

/**
 * Machine-readable summary of one run. Serializes to {@code summary.json},
 * which the JSON schema in {@code summary.schema.json} pins down. An AI
 * agent reads this file first to answer the two questions the harness exists
 * to answer.
 */
public record SummaryReport(
        String runId,
        String scenario,
        String backend,
        Map<String, Object> config,
        String verdict,
        List<InvariantResult> invariantResults,
        Performance performance,
        List<String> notes) {

    public SummaryReport {
        // `config` may carry null values (e.g. postgresUrl when unset) so we
        // cannot use Map.copyOf — caller passes a freshly-built map.
        invariantResults = List.copyOf(invariantResults);
        notes = List.copyOf(notes);
    }

    public record Performance(
            long totalEnqueued,
            long totalSucceeded,
            long totalFailed,
            long totalTimedOut,
            long totalQuarantined,
            long totalRetried,
            long wallClockDurationMs,
            ThroughputBreakdown throughputJobsPerSec,
            LatencyBreakdown latencyMs,
            LockContention lockContention) {}

    public record ThroughputBreakdown(double overall, Map<String, Double> byQueue, Map<String, Double> byHandler) {

        public ThroughputBreakdown {
            byQueue = Map.copyOf(byQueue);
            byHandler = Map.copyOf(byHandler);
        }
    }

    public record LatencyBreakdown(
            Percentiles.Summary enqueueToClaim,
            Percentiles.Summary claimToStart,
            Percentiles.Summary startToComplete,
            Percentiles.Summary endToEnd) {}

    public record LockContention(Map<String, LockStats> byKey) {

        public LockContention {
            byKey = Map.copyOf(byKey);
        }
    }

    public record LockStats(
            long acquires, int maxConcurrentShared, long exclusiveCount, long avgWaitMs, long p99WaitMs) {}

    /** Convenience: convert this report to a {@code LinkedHashMap} suitable for Jackson serialisation. */
    public Map<String, Object> asMap() {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("runId", runId);
        root.put("scenario", scenario);
        root.put("backend", backend);
        root.put("config", config);
        root.put("verdict", verdict);
        List<Map<String, Object>> invariants = new ArrayList<>();
        for (InvariantResult r : invariantResults) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", r.name());
            m.put("passed", r.passed());
            m.put("violations", r.violations());
            m.put("sampleChains", r.sampleChains());
            invariants.add(m);
        }
        root.put("invariants", invariants);
        Map<String, Object> perf = new LinkedHashMap<>();
        perf.put("totalEnqueued", performance.totalEnqueued());
        perf.put("totalSucceeded", performance.totalSucceeded());
        perf.put("totalFailed", performance.totalFailed());
        perf.put("totalTimedOut", performance.totalTimedOut());
        perf.put("totalQuarantined", performance.totalQuarantined());
        perf.put("totalRetried", performance.totalRetried());
        perf.put("wallClockDurationMs", performance.wallClockDurationMs());
        Map<String, Object> tp = new LinkedHashMap<>();
        tp.put("overall", performance.throughputJobsPerSec().overall());
        tp.put("byQueue", performance.throughputJobsPerSec().byQueue());
        tp.put("byHandler", performance.throughputJobsPerSec().byHandler());
        perf.put("throughputJobsPerSec", tp);
        Map<String, Object> lat = new LinkedHashMap<>();
        lat.put("enqueueToClaim", asMap(performance.latencyMs().enqueueToClaim()));
        lat.put("claimToStart", asMap(performance.latencyMs().claimToStart()));
        lat.put("startToComplete", asMap(performance.latencyMs().startToComplete()));
        lat.put("endToEnd", asMap(performance.latencyMs().endToEnd()));
        perf.put("latencyMs", lat);
        Map<String, Object> contention = new LinkedHashMap<>();
        Map<String, Object> byKey = new LinkedHashMap<>();
        performance.lockContention().byKey().forEach((k, s) -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("acquires", s.acquires());
            m.put("maxConcurrentShared", s.maxConcurrentShared());
            m.put("exclusiveCount", s.exclusiveCount());
            m.put("avgWaitMs", s.avgWaitMs());
            m.put("p99WaitMs", s.p99WaitMs());
            byKey.put(k, m);
        });
        contention.put("byKey", byKey);
        perf.put("lockContention", contention);
        root.put("performance", perf);
        root.put("notes", notes);
        return root;
    }

    private static Map<String, Object> asMap(Percentiles.Summary s) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("p50", s.p50());
        m.put("p95", s.p95());
        m.put("p99", s.p99());
        m.put("max", s.max());
        m.put("count", s.count());
        return m;
    }
}
