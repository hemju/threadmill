package com.hemju.threadmill.simulation.nudge;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Cross-process JSON-lines trace used by the nudge correctness simulation. */
final class NudgeSimulationTrace {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final ConcurrentHashMap<Path, Object> PROCESS_LOCKS = new ConcurrentHashMap<>();

    private NudgeSimulationTrace() {}

    static void append(Path path, String event, Map<String, ?> fields) {
        var absolute = path.toAbsolutePath().normalize();
        var processLock = PROCESS_LOCKS.computeIfAbsent(absolute, ignored -> new Object());
        synchronized (processLock) {
            appendLocked(absolute, event, fields);
        }
    }

    private static void appendLocked(Path path, String event, Map<String, ?> fields) {
        try {
            var parent = path.getParent();
            if (parent != null) Files.createDirectories(parent);
            var bytes = line(event, fields).getBytes(StandardCharsets.UTF_8);
            try (var channel = FileChannel.open(
                            path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
                    var lock = channel.lock()) {
                if (!lock.isValid()) throw new IllegalStateException("trace lock is not valid: " + path);
                var buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) channel.write(buffer);
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed to append nudge simulation trace: " + path, e);
        }
    }

    private static String line(String event, Map<String, ?> fields) throws JsonProcessingException {
        var document = new LinkedHashMap<String, Object>();
        document.put("timestamp", Instant.now().toString());
        document.put("event", event);
        document.putAll(fields);
        return JSON.writeValueAsString(document) + '\n';
    }
}
