package com.hemju.threadmill.soak.harness.invariant;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * In-memory view of {@code trace.jsonl} as a list of {@link JsonNode}s.
 *
 * <p>Loaded once per run and passed to every {@link SoakInvariant} so each
 * checker can walk the events in order without re-reading the file. The
 * raw line text is preserved so violation chains can quote the exact bytes
 * an AI agent would see in the on-disk artifact.
 */
public final class TraceCorpus {

    private final List<JsonNode> events;
    private final List<String> rawLines;

    private TraceCorpus(List<JsonNode> events, List<String> rawLines) {
        this.events = List.copyOf(events);
        this.rawLines = List.copyOf(rawLines);
    }

    public static TraceCorpus load(Path traceFile) throws IOException {
        Objects.requireNonNull(traceFile, "traceFile");
        ObjectMapper mapper = new ObjectMapper();
        List<JsonNode> events = new ArrayList<>();
        List<String> raw = new ArrayList<>();
        try (var lines = Files.lines(traceFile)) {
            lines.forEach(line -> {
                if (line.isBlank()) return;
                try {
                    events.add(mapper.readTree(line));
                    raw.add(line);
                } catch (IOException e) {
                    throw new IllegalStateException("malformed trace line: " + line, e);
                }
            });
        }
        return new TraceCorpus(events, raw);
    }

    public List<JsonNode> events() {
        return events;
    }

    public List<String> rawLines() {
        return rawLines;
    }

    public int size() {
        return events.size();
    }
}
