package com.hemju.threadmill.core;

import java.util.Objects;

/**
 * Validation for user-supplied Threadmill names that become storage keys,
 * database values, or routing labels.
 */
public final class Names {

    /** Maximum length for queue, cron task, mutex, metadata, and tag names. */
    public static final int MAX_LENGTH = 128;

    private Names() {}

    /**
     * Validate a user-supplied name and return it unchanged.
     *
     * @throws IllegalArgumentException if the value is blank, too long, or
     *     contains ISO control characters
     */
    public static String requireName(String field, String value) {
        Objects.requireNonNull(field, "field");
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        if (value.length() > MAX_LENGTH) {
            throw new IllegalArgumentException(field + " must be at most " + MAX_LENGTH + " characters");
        }
        for (int i = 0; i < value.length(); i++) {
            if (Character.isISOControl(value.charAt(i))) {
                throw new IllegalArgumentException(field + " must not contain control characters");
            }
        }
        return value;
    }
}
