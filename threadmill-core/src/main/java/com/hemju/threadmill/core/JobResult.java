package com.hemju.threadmill.core;

import java.util.Objects;
import java.util.Optional;

/**
 * The optional, post-execution result slot on a {@link Job}.
 *
 * <p>The job model reserves space for a serialized result <strong>from day
 * one</strong>. Workflows and batch operations in later phases consume the
 * result; retrofitting this into the model later is the expensive mistake
 * the project is designed to avoid.
 *
 * <p>The value is stored as a serialized payload together with a type tag
 * so the deserializer can pick the correct concrete type.
 *
 * @param typeTag     a fully-qualified type name describing the serialized value
 * @param serialized  the serialized form (interpretation belongs to the active
 *                    {@code JobSerializer})
 */
public record JobResult(String typeTag, String serialized) {

    public JobResult {
        Objects.requireNonNull(typeTag, "typeTag");
        Objects.requireNonNull(serialized, "serialized");
    }

    public Optional<String> typeTagValue() {
        return Optional.of(typeTag);
    }
}
