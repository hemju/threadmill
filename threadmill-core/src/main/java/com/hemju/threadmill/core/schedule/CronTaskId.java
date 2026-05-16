package com.hemju.threadmill.core.schedule;

import java.util.Objects;

/** Identifier returned when registering a recurring Threadmill job. */
public record CronTaskId(String name) {
    public CronTaskId {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) throw new IllegalArgumentException("name must not be blank");
    }
}
