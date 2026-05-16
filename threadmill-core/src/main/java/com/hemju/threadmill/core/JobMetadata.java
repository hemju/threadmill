package com.hemju.threadmill.core;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe key/value metadata attached to a job.
 *
 * <p>Metadata is user-touchable during a job's execution. The engine never
 * serializes the live map directly: a snapshot via {@link #snapshot()} is
 * taken before serialization to ensure concurrent mutation cannot corrupt
 * the on-disk representation.
 *
 * <p>Keys and values are stored as plain strings; richer values should be
 * serialized by the caller. Full Unicode (including 4-byte characters) is
 * supported and must round-trip through every store.
 */
public final class JobMetadata {

    private final ConcurrentHashMap<String, String> entries = new ConcurrentHashMap<>();

    public JobMetadata() {}

    public JobMetadata(Map<String, String> initial) {
        Objects.requireNonNull(initial, "initial");
        initial.forEach((k, v) -> {
            Objects.requireNonNull(k, "metadata key");
            Objects.requireNonNull(v, "metadata value for key " + k);
            entries.put(k, v);
        });
    }

    public void put(String key, String value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        entries.put(key, value);
    }

    public Optional<String> get(String key) {
        return Optional.ofNullable(entries.get(key));
    }

    public boolean containsKey(String key) {
        return entries.containsKey(key);
    }

    public int size() {
        return entries.size();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    /**
     * Returns an immutable, point-in-time snapshot of the metadata. Intended
     * for the engine: a serialization that reads from the snapshot cannot
     * observe partial state caused by concurrent mutation.
     */
    public Map<String, String> snapshot() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(entries));
    }
}
