package com.hemju.threadmill.core;

import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Identifier for a {@code ProcessingNode} — one running application instance
 * participating in the Threadmill cluster.
 *
 * <p>A {@code NodeId} is opaque and stable for the lifetime of the node.
 *
 * <p>Serializes as the bare UUID string via Jackson's {@link JsonValue} so
 * dashboard / API consumers see {@code "node-id-uuid-string"} rather than a
 * nested {@code {"value":...}} object.
 */
public final class NodeId {

    private final UUID value;

    private NodeId(UUID value) {
        this.value = Objects.requireNonNull(value, "value");
    }

    public static NodeId newId() {
        return new NodeId(UUID.randomUUID());
    }

    public static NodeId of(UUID value) {
        return new NodeId(value);
    }

    @JsonCreator
    public static NodeId parse(String text) {
        return new NodeId(UUID.fromString(text));
    }

    public UUID asUuid() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof NodeId other && value.equals(other.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    @JsonValue
    public String toString() {
        return value.toString();
    }
}
