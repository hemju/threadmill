package com.hemju.threadmill.soak.harness;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

/**
 * Loads the run-summary JSON schema and validates {@link SummaryReport}
 * outputs against it. Surfaced as a static helper because the schema file is
 * a single resource shared by every test and runtime call.
 */
public final class SummarySchema {

    private static final String RESOURCE = "/com/hemju/threadmill/soak/harness/summary.schema.json";

    private SummarySchema() {}

    public static JsonSchema load() {
        try (InputStream in = SummarySchema.class.getResourceAsStream(RESOURCE)) {
            if (in == null) throw new IllegalStateException("schema resource not found: " + RESOURCE);
            JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
            return factory.getSchema(in, schemaConfig());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static SchemaValidatorsConfig schemaConfig() {
        // Networknt 1.5.x: builder style (the no-arg constructor is deprecated).
        return SchemaValidatorsConfig.builder().build();
    }

    public static List<String> validate(JsonSchema schema, SummaryReport report) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.valueToTree(report.asMap());
        Set<ValidationMessage> messages = schema.validate(node);
        return messages.stream().map(ValidationMessage::getMessage).collect(Collectors.toList());
    }
}
