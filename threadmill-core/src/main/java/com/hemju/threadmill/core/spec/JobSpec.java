package com.hemju.threadmill.core.spec;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Serializable description of the work a job will run.
 *
 * <p>A {@code JobSpec} is structurally immutable. It carries:
 * <ul>
 *   <li>the fully-qualified type name of the {@code JobHandler} that will run
 *       the job, and</li>
 *   <li>the typed argument list to pass to that handler — exactly one
 *       {@link JobArgument} per payload position. The Threadmill job model is
 *       command + handler: there is a single argument carrying the
 *       {@code JobPayload}, but the list is kept open to allow handlers to
 *       evolve their signatures.</li>
 * </ul>
 *
 * @param handlerType  fully-qualified handler type name
 * @param arguments    immutable list of arguments (never {@code null}, may be empty)
 * @param dedupKey     optional producer-side deduplication key
 * @param dedupTtl     optional deduplication window; required when {@code dedupKey} is set
 */
public record JobSpec(String handlerType, List<JobArgument> arguments, String dedupKey, Duration dedupTtl) {

    public static final int MAX_DEDUP_KEY_BYTES = 256;
    /** Maximum UTF-8 bytes accepted for a handler type name. */
    public static final int MAX_HANDLER_TYPE_BYTES = 1024;

    public JobSpec(String handlerType, List<JobArgument> arguments) {
        this(handlerType, arguments, null, null);
    }

    public JobSpec {
        Objects.requireNonNull(handlerType, "handlerType");
        if (handlerType.isBlank()) {
            throw new IllegalArgumentException("handlerType must not be blank");
        }
        if (handlerType.getBytes(StandardCharsets.UTF_8).length > MAX_HANDLER_TYPE_BYTES) {
            throw new IllegalArgumentException("handlerType must be at most 1024 UTF-8 bytes");
        }
        Objects.requireNonNull(arguments, "arguments");
        arguments = List.copyOf(arguments);
        if (dedupKey != null) {
            if (dedupKey.isBlank()) {
                throw new IllegalArgumentException("dedupKey must not be blank");
            }
            int bytes = dedupKey.getBytes(StandardCharsets.UTF_8).length;
            if (bytes > MAX_DEDUP_KEY_BYTES) {
                throw new IllegalArgumentException("dedupKey must be at most 256 UTF-8 bytes");
            }
            if (dedupTtl == null) {
                throw new IllegalArgumentException("dedupTtl is required when dedupKey is set");
            }
            if (dedupTtl.isZero() || dedupTtl.isNegative()) {
                throw new IllegalArgumentException("dedupTtl must be positive");
            }
        } else if (dedupTtl != null) {
            throw new IllegalArgumentException("dedupTtl requires dedupKey");
        }
    }

    public static JobSpec of(String handlerType, JobArgument... args) {
        return new JobSpec(handlerType, List.of(args));
    }

    public JobSpec withDedup(String key, Duration ttl) {
        return new JobSpec(handlerType, arguments, key, ttl);
    }

    public Optional<String> dedupKeyValue() {
        return Optional.ofNullable(dedupKey);
    }

    public Optional<Duration> dedupTtlValue() {
        return Optional.ofNullable(dedupTtl);
    }
}
