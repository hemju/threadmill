package com.hemju.threadmill.soak.harness;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * JSON-lines trace writer for the soak harness.
 *
 * <p>Mirrors the format used by {@code threadmill-simulation}'s
 * {@code TraceWriter}: every line is a complete JSON object whose first two
 * fields are {@code timestamp} (ISO-8601 UTC) and {@code event}. Trailing
 * fields preserve insertion order so a diff over two runs is meaningful.
 *
 * <p>Event vocabulary: {@code enqueued}, {@code claimed}, {@code started},
 * {@code check_in}, {@code succeeded}, {@code failed}, {@code retried},
 * {@code timed_out}, {@code quarantined}, {@code lock_acquired},
 * {@code lock_released}, {@code queue_paused}, {@code queue_resumed},
 * {@code node_started}, {@code node_stopped}.
 *
 * <p>Each write is guarded by a {@link ReentrantLock} so concurrent emitters
 * never tear a line.
 */
public final class SoakTraceWriter implements AutoCloseable {

    private final ObjectMapper mapper;
    private final BufferedWriter writer;
    private final ReentrantLock lock = new ReentrantLock();

    public SoakTraceWriter(Path file) throws IOException {
        Objects.requireNonNull(file, "file");
        if (file.getParent() != null) Files.createDirectories(file.getParent());
        this.writer = Files.newBufferedWriter(
                file,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public void emit(String event, Map<String, Object> fields) {
        Objects.requireNonNull(event, "event");
        Map<String, Object> ordered = new LinkedHashMap<>();
        ordered.put("timestamp", Instant.now().toString());
        ordered.put("event", event);
        if (fields != null) ordered.putAll(fields);
        lock.lock();
        try {
            writer.write(mapper.writeValueAsString(ordered));
            writer.write('\n');
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Trace value not JSON-serialisable for event " + event, e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            writer.flush();
            writer.close();
        } finally {
            lock.unlock();
        }
    }
}
