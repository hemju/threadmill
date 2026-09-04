package com.hemju.threadmill.simulation;

import java.time.Duration;

/**
 * Tunables for one simulation run. Defaults are calibrated for a developer
 * laptop — ~10–30 seconds wall time, every invariant exercised, no flakiness.
 */
public record SimulationConfig(
    int projectCount,
    int totalJobs,
    double importFraction,
    double failureRate,
    double hangRate,
    double poisonRate,
    Duration importDuration,
    Duration exportDuration,
    Duration jobTimeout,
    int workerCount,
    Duration runBudget) {

  public static SimulationConfig defaults() {
    return new SimulationConfig(
        50, // projectCount
        400, // totalJobs (small enough to finish in seconds, big enough to exercise contention)
        0.10, // importFraction
        0.05, // failureRate
        0.005, // hangRate
        0.0, // poisonRate (kept at 0 — quarantine fires deterministically from missing-handler
        // payloads)
        Duration.ofMillis(30), // importDuration (slower than export, like real workloads)
        Duration.ofMillis(8), // exportDuration
        Duration.ofSeconds(2), // jobTimeout
        8, // workerCount
        // runBudget — the drain deadline. This is a LIVENESS bound, not a
        // throughput assertion: a correct engine drains these jobs in seconds,
        // a wedged one never drains at all. Keep it generous. The former 45s
        // was tight enough that a shared 4-core CI runner executing this
        // simulation alongside the browser suite, the soak regression, and
        // javadoc timed out with 227/400 jobs done and none failed — a slow
        // machine reading as a hang. Throughput regressions belong to the soak
        // suite, which measures them directly.
        Duration.ofMinutes(4));
  }
}
