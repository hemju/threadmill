package com.hemju.threadmill.simulation;

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
 * JSON-lines trace writer for the simulation. Every lifecycle event the
 * verifier needs is recorded here: enqueued, claimed, started, succeeded,
 * failed, retried, timed_out, quarantined, lock_acquired, lock_released,
 * node_started, node_stopped, queue_paused, queue_resumed, transactional_enqueued.
 *
 * <p>Append-only with a lock around each line so concurrent writers don't
 * tear records. Each line is a complete JSON object with at least
 * {@code timestamp} and {@code event} fields.
 */
public final class TraceWriter implements AutoCloseable {

    private final ObjectMapper mapper;
    private final BufferedWriter writer;
    private final ReentrantLock lock = new ReentrantLock();

    public TraceWriter(Path file) throws IOException {
        Objects.requireNonNull(file, "file");
        Files.createDirectories(file.getParent());
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

    /**
     * Emit one event. Field order is deterministic for diff-readability:
     * {@code timestamp}, {@code event}, then the supplied fields in insertion order.
     */
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
