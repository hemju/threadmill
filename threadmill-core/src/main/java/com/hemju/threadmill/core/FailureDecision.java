package com.hemju.threadmill.core;

import java.time.Instant;

/**
 * Durable disposition of a failed attempt, recorded in the same write as FAILED.
 * A null retry time means final failure; a refund preserves shutdown-neutral
 * attempt accounting. Recovery follows this decision rather than reconstructing
 * an exception-specific policy after the original exception has disappeared.
 */
public record FailureDecision(Instant retryAt, boolean refundAttempt) {
  public FailureDecision {
    if (refundAttempt && retryAt == null) {
      throw new IllegalArgumentException("An attempt refund requires a retry");
    }
  }

  public static FailureDecision finalFailure() {
    return new FailureDecision(null, false);
  }

  public boolean willRetry() {
    return retryAt != null;
  }
}
