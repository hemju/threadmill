package com.hemju.threadmill.core.schedule;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Identifier returned when registering a recurring Threadmill job.
 *
 * <p>Serializes as the bare task name string via Jackson's {@link JsonValue}
 * so API consumers see {@code "task-name"} rather than {@code {"name":"..."}}.
 */
public record CronTaskId(@JsonValue String name) {
    @JsonCreator
    public CronTaskId {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) throw new IllegalArgumentException("name must not be blank");
    }
}
