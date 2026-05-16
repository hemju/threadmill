package com.hemju.threadmill.core;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Append-only, thread-safe per-job log.
 *
 * <p>User code calls {@link #info(String)} / {@link #warn(String)} /
 * {@link #error(String)} from inside a handler. The engine takes a
 * {@link #snapshot()} before serialization so the persisted form is never
 * inconsistent with concurrent appends.
 */
public final class JobLog {

    public static final int DEFAULT_MAX_ENTRIES = 1000;
    public static final int DEFAULT_MAX_BYTES = 256 * 1024;

    /** Severity attached to a log entry. */
    public enum Level {
        INFO,
        WARN,
        ERROR
    }

    /** One entry in the log. */
    public record Entry(Instant at, Level level, String message) {
        public Entry {
            Objects.requireNonNull(at, "at");
            Objects.requireNonNull(level, "level");
            Objects.requireNonNull(message, "message");
        }
    }

    private final Clock clock;
    private final List<Entry> entries = new ArrayList<>();
    private int maxEntries = DEFAULT_MAX_ENTRIES;
    private int maxBytes = DEFAULT_MAX_BYTES;

    public JobLog() {
        this(Clock.systemUTC());
    }

    public JobLog(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public synchronized void info(String message) {
        append(Level.INFO, message);
    }

    public synchronized void warn(String message) {
        append(Level.WARN, message);
    }

    public synchronized void error(String message) {
        append(Level.ERROR, message);
    }

    public synchronized int size() {
        return entries.size();
    }

    public synchronized void configureBounds(int maxEntries, int maxBytes) {
        if (maxEntries <= 0) throw new IllegalArgumentException("maxEntries must be positive");
        if (maxBytes <= 0) throw new IllegalArgumentException("maxBytes must be positive");
        this.maxEntries = maxEntries;
        this.maxBytes = maxBytes;
        trimToBounds();
    }

    private void append(Level level, String message) {
        entries.add(new Entry(clock.instant(), level, message));
        trimToBounds();
    }

    private void trimToBounds() {
        while (entries.size() > maxEntries || byteSize() > maxBytes) {
            entries.remove(0);
        }
    }

    private int byteSize() {
        int total = 0;
        for (Entry entry : entries) {
            total += entry.message().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        }
        return total;
    }

    /**
     * Returns an immutable point-in-time snapshot of the log entries.
     */
    public synchronized List<Entry> snapshot() {
        return Collections.unmodifiableList(new ArrayList<>(entries));
    }

    /**
     * Replaces the log contents — only intended for the deserialization path
     * inside the engine, never for user code.
     */
    public synchronized void replaceAll(List<Entry> newEntries) {
        Objects.requireNonNull(newEntries, "newEntries");
        entries.clear();
        entries.addAll(newEntries);
        trimToBounds();
    }
}
