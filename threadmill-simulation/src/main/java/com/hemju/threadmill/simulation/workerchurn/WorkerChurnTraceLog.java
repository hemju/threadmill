package com.hemju.threadmill.simulation.workerchurn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** JSON-lines trace writer shared by the worker-churn simulation. */
final class WorkerChurnTraceLog {

    private static final ConcurrentHashMap<Path, Object> PROCESS_LOCKS = new ConcurrentHashMap<>();

    private WorkerChurnTraceLog() {}

    static void append(Path path, String event, Map<String, ?> fields) {
        Path absolute = path.toAbsolutePath().normalize();
        Object processLock = PROCESS_LOCKS.computeIfAbsent(absolute, ignored -> new Object());
        synchronized (processLock) {
            appendLocked(absolute, event, fields);
        }
    }

    private static void appendLocked(Path path, String event, Map<String, ?> fields) {
        try {
            Path parent = path.getParent();
            if (parent != null) Files.createDirectories(parent);
            String line = line(event, fields);
            try (FileChannel channel = FileChannel.open(
                            path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
                    FileLock lock = channel.lock()) {
                if (!lock.isValid()) throw new IllegalStateException("trace lock is not valid: " + path);
                channel.write(ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8)));
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed to append worker-churn trace: " + path, e);
        }
    }

    private static String line(String event, Map<String, ?> fields) {
        var out = new StringBuilder(256);
        out.append("{\"ts\":\"")
                .append(Instant.now())
                .append("\",\"event\":\"")
                .append(escape(event))
                .append('"');
        for (var entry : fields.entrySet()) {
            out.append(",\"").append(escape(entry.getKey())).append("\":");
            Object value = entry.getValue();
            if (value instanceof Number || value instanceof Boolean) {
                out.append(value);
            } else {
                out.append('"').append(escape(String.valueOf(value))).append('"');
            }
        }
        return out.append("}\n").toString();
    }

    private static String escape(String value) {
        var out = new StringBuilder(value.length() + 16);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.toString();
    }
}
