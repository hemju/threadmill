package com.hemju.threadmill.soak.harness;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.networknt.schema.JsonSchema;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.soak.harness.invariant.InvariantResult;

/**
 * Sanity-checks the run-summary JSON schema: a valid {@link SummaryReport}
 * validates clean; an obviously broken report fails with at least one error.
 *
 * <p>Untagged — runs in {@code ./gradlew check} so a schema change that
 * doesn't update the validator (or vice versa) trips immediately.
 */
final class SummarySchemaTest {

    @Test
    void validSummaryReportValidatesAgainstSchema() {
        JsonSchema schema = SummarySchema.load();
        SummaryReport report = sampleReport(true);
        List<String> errors = SummarySchema.validate(schema, report);
        assertThat(errors).as("validation errors on a well-formed report").isEmpty();
    }

    @Test
    void brokenSummaryFailsValidation() {
        JsonSchema schema = SummarySchema.load();
        SummaryReport bad = sampleReport(true);
        Map<String, Object> raw = bad.asMap();
        raw.put("verdict", "maybe");
        List<String> errors = schema.validate(toJson(raw)).stream()
                .map(com.networknt.schema.ValidationMessage::getMessage)
                .toList();
        assertThat(errors)
                .as("an invalid verdict enum value must be rejected by the schema")
                .isNotEmpty();
    }

    private static com.fasterxml.jackson.databind.JsonNode toJson(Map<String, Object> raw) {
        return new com.fasterxml.jackson.databind.ObjectMapper().valueToTree(raw);
    }

    private static SummaryReport sampleReport(boolean passed) {
        Percentiles.Summary empty = Percentiles.Summary.empty();
        SummaryReport.Performance perf = new SummaryReport.Performance(
                100,
                95,
                3,
                2,
                0,
                4,
                3_000,
                new SummaryReport.ThroughputBreakdown(31.7, Map.of(), Map.of()),
                new SummaryReport.LatencyBreakdown(empty, empty, empty, empty),
                new SummaryReport.LockContention(Map.of()));
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("runId", "test");
        return new SummaryReport(
                "test",
                "mixed-workload",
                "memory",
                config,
                passed ? "passed" : "failed",
                List.of(InvariantResult.pass("atLeastOnce")),
                perf,
                List.of("sample"));
    }
}
