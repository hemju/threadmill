package com.hemju.threadmill.soak.harness;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/** Writes the run's effective configuration to {@code config.json}. */
public final class RunConfigWriter {

    private RunConfigWriter() {}

    public static Map<String, Object> asMap(SoakHarnessConfig c) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("runId", c.runId());
        m.put("backend", c.backend());
        m.put("scenario", c.scenario());
        m.put("duration", c.duration().toString());
        m.put("durationMillis", c.duration().toMillis());
        m.put("jobsPerSecond", c.jobsPerSecond());
        m.put("producers", c.producers());
        m.put("workerCount", c.workerCount());
        m.put("nodes", c.nodes());
        m.put("outputDir", c.outputDir().toString());
        m.put("failFast", c.failFast());
        m.put("force", c.force());
        m.put("postgresUrl", c.postgresUrl().orElse(null));
        m.put("redisTopology", c.redisTopology());
        m.put("redisUrl", c.redisUrl().orElse(null));
        m.put("progressInterval", c.progressInterval().toString());
        m.put("nodeChurn", c.nodeChurn().map(Duration::toString).orElse(null));
        return m;
    }

    public static void write(SoakHarnessConfig config, OutputDir dir) throws IOException {
        var mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        Files.writeString(dir.configJson(), mapper.writeValueAsString(asMap(config)), StandardCharsets.UTF_8);
    }
}
