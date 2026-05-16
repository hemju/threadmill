/**
 * Manual, per-backend soak harness for Threadmill.
 *
 * <p>The harness produces a self-contained artifact directory per run that an
 * AI agent (or human) can ingest cold to answer two questions: did the engine
 * behave correctly under the chosen scenario, and how fast was it. The entry
 * points are the Gradle tasks {@code soakMemory}, {@code soakPostgres},
 * {@code soakRedis}, and {@code soakAll}, defined in this module's build.
 *
 * <p>Distinct from the {@code :soakRegression} JUnit task (fixed sustained
 * throughput, recurring no-skip, container-pause recovery) and from
 * {@code threadmill-simulation} (correctness verification and worker-process
 * churn). The harness covers a library of operator-driven scenarios with
 * tunable duration and rate.
 */
package com.hemju.threadmill.soak.harness;
