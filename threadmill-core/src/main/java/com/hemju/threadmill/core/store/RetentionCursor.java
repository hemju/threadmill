package com.hemju.threadmill.core.store;

import java.util.Objects;

/**
 * Opaque continuation of one store's retention pass. Pass it back unchanged
 * with the same store, state and cutoff; do not derive it from a job id.
 * Tokens are ephemeral and need not survive a backend or library upgrade.
 *
 * @param value store-defined, nonblank continuation token
 */
public record RetentionCursor(String value) {
  public RetentionCursor {
    Objects.requireNonNull(value, "value");
    if (value.isBlank()) throw new IllegalArgumentException("Retention cursor must not be blank");
  }
}
