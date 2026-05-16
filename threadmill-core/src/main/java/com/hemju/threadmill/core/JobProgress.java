package com.hemju.threadmill.core;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe per-job progress indicator.
 *
 * <p>Handlers may publish progress between {@code 0.0} and {@code 1.0} with
 * an optional human-readable message. The engine snapshots the current value
 * before serialization.
 */
public final class JobProgress {

    /** Immutable point-in-time progress value. */
    public record Snapshot(double fraction, String message) {
        public Snapshot {
            if (Double.isNaN(fraction) || fraction < 0.0 || fraction > 1.0) {
                throw new IllegalArgumentException("fraction must be in [0.0, 1.0], got " + fraction);
            }
            // message may be null
        }
    }

    private final AtomicReference<Snapshot> current = new AtomicReference<>();

    public void update(double fraction, String message) {
        current.set(new Snapshot(fraction, message));
    }

    public void update(double fraction) {
        update(fraction, null);
    }

    public Optional<Snapshot> snapshot() {
        return Optional.ofNullable(current.get());
    }

    /**
     * Replaces the current value — only intended for the deserialization path
     * inside the engine, never for user code.
     */
    public void replace(Snapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        current.set(snapshot);
    }

    public void clear() {
        current.set(null);
    }
}
