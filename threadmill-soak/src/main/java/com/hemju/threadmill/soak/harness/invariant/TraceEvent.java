package com.hemju.threadmill.soak.harness.invariant;

import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * One parsed trace line handed to {@link StreamingInvariantCheck#onEvent}.
 *
 * <p>Carries both the parsed JSON and the raw line text so violation sample
 * chains can quote the exact bytes an AI agent would see in the on-disk
 * {@code trace.jsonl} artifact.
 */
public record TraceEvent(JsonNode json, String rawLine) {

    public TraceEvent {
        Objects.requireNonNull(json, "json");
        Objects.requireNonNull(rawLine, "rawLine");
    }

    /** The {@code event} field; empty string when missing. */
    public String event() {
        return text("event");
    }

    /** A string field; empty string when missing or null. */
    public String text(String field) {
        return json.path(field).asText("");
    }

    /** An int field with a default for missing/null. */
    public int intField(String field, int defaultValue) {
        return json.path(field).asInt(defaultValue);
    }

    /** A boolean field with a default for missing/null. */
    public boolean boolField(String field, boolean defaultValue) {
        return json.path(field).asBoolean(defaultValue);
    }
}
