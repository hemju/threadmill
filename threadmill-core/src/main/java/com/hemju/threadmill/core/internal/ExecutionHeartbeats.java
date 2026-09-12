package com.hemju.threadmill.core.internal;

import java.util.Map;

import com.hemju.threadmill.core.JobId;

/** Internal validation shared by the bounded execution-heartbeat implementations. */
public final class ExecutionHeartbeats {
  public static final int MAX_BATCH = 500;

  private ExecutionHeartbeats() {}

  public static Map<JobId, Long> snapshot(Map<JobId, Long> activeClaims) {
    if (activeClaims.size() > MAX_BATCH)
      throw new IllegalArgumentException("Execution heartbeat exceeds " + MAX_BATCH + " claims");
    var snapshot = Map.copyOf(activeClaims);
    if (snapshot.values().stream().anyMatch(version -> version <= 0))
      throw new IllegalArgumentException("Execution heartbeat requires persisted claim versions");
    return snapshot;
  }
}
