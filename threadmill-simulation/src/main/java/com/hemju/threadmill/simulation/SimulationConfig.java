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
                0.0, // poisonRate (kept at 0 — quarantine fires deterministically from missing-handler payloads)
                Duration.ofMillis(30), // importDuration (slower than export, like real workloads)
                Duration.ofMillis(8), // exportDuration
                Duration.ofSeconds(2), // jobTimeout
                8, // workerCount
                Duration.ofSeconds(45)); // runBudget — drain deadline
    }
}
