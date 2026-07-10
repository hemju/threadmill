# Threadmill

[![Maven Central](https://img.shields.io/maven-central/v/com.hemju.threadmill/threadmill-core.svg?label=Maven%20Central)](https://central.sonatype.com/namespace/com.hemju.threadmill)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java 25](https://img.shields.io/badge/Java-25-orange.svg)](https://openjdk.org/projects/jdk/25/)

A modern, lightweight background-job-processing library for Java 25.

Threadmill runs your idempotent background work durably, on a cluster of
application nodes, against a real datastore (PostgreSQL or Redis) — or an
in-memory store for local development. The design uses well-understood
techniques: a durable job store, optimistic concurrency control, polling
worker loops, an explicit state machine, and a small, deliberate public
API.

## A modern replacement for Quartz and JobRunr

Threadmill is built to replace **[Quartz](https://www.quartz-scheduler.org/)**
and **[JobRunr](https://www.jobrunr.io/)** on Java 25. It gives you both what
Quartz gives you (durable cron and interval scheduling, clustering, misfire
handling) and what JobRunr gives you (fire-and-forget background jobs, a
dashboard, retries) behind one small, idempotency-first API — built on virtual
threads and scoped values, with first-class PostgreSQL **and** Redis backends
held to a single shared contract test suite. If you are choosing between the
two, or want to consolidate both onto one library, Threadmill is the
replacement.

See the side-by-side migration and feature guides:
[Threadmill vs. Quartz](docs/threadmill-vs-quartz.md) ·
[Threadmill vs. JobRunr](docs/threadmill-vs-jobrunr.md).

## Delivery guarantee

Threadmill provides **at-least-once delivery**. A job may run more than
once — after a node crash, after a long GC pause that makes a heartbeat
look expired, after a transient store outage. **Job handlers must be
idempotent.** This is the single most important fact about the library.
Design your handlers as if they will run twice for the same logical job —
because, occasionally, they will.

## Installation

Threadmill is published to Maven Central under the `com.hemju.threadmill`
group. Pick the core plus the store you run against (and the Spring Boot
starter if you use Spring):

```kotlin
// build.gradle.kts
implementation("com.hemju.threadmill:threadmill-core:0.1.2")
implementation("com.hemju.threadmill:threadmill-store-postgres:0.1.2") // or -store-redis / -store-memory
implementation("com.hemju.threadmill:threadmill-spring-boot:0.1.2")    // optional Spring Boot integration
```

```xml
<!-- Maven -->
<dependency>
  <groupId>com.hemju.threadmill</groupId>
  <artifactId>threadmill-core</artifactId>
  <version>0.1.2</version>
</dependency>
```

Optional modules: `threadmill-metrics` (Micrometer), `threadmill-tracing`
(OpenTelemetry), and `threadmill-dashboard-spring` + `threadmill-dashboard-ui`
(operations console).

## Quick start

```java
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.spring.Job;
import com.hemju.threadmill.spring.JobScheduler;
import org.springframework.stereotype.Component;

// 1. Define a payload (a simple Jackson-serializable command object).
public record SendEmail(String to, String subject) implements JobPayload {}

// 2. Define a Spring bean handler.
@Component
@Job(queue = "email", timeout = "PT2M")
public final class SendEmailHandler implements JobHandler<SendEmail> {
    @Override
    public void run(SendEmail p, JobExecutionContext ctx) {
        ctx.log("Sending to " + p.to());
        // ...
    }
}

// 3. Enqueue by payload type. Threadmill routes to the annotated handler.
@Component
public final class MailController {
    private final JobScheduler jobs;

    public MailController(JobScheduler jobs) {
        this.jobs = jobs;
    }

    public void send() {
        jobs.enqueue(SendEmailHandler.class, new SendEmail("a@b.com", "hi"));
    }
}
```

See [docs/quickstart.md](docs/quickstart.md) for a complete Spring walkthrough, and
[docs/getting-started.md](docs/getting-started.md) for the manual core API.

## Modules

| Module | Purpose |
|---|---|
| `threadmill-core` | Job model, state machine, `JobStore` SPI, serialization, engine (`ProcessingNode`, `Dispatcher`, `MaintenanceCycle`), interceptors, scheduling API. **No storage, no framework code.** |
| `threadmill-store-memory` | In-memory `JobStore` for tests and local dev. Held to the same contract as the real backends. |
| `threadmill-store-postgres` | PostgreSQL backend (auto-migrations, `SELECT … FOR UPDATE SKIP LOCKED` claim, per-state counter table with trigger). |
| `threadmill-store-redis` | Redis backend (reliable-fetch via Lua, per-job HASH + per-queue ZSET + counts HASH). |
| `threadmill-spring-boot` | Spring Boot 4.x auto-configuration. |
| `threadmill-metrics` | Micrometer integration: jobs-per-state gauges, processed/failed counters, processing-time timer. |
| `threadmill-tracing` | Optional OpenTelemetry API integration for processing and store-operation spans. |
| `threadmill-dashboard-api` | Spring-free dashboard contract: `EngineSnapshot`, DTOs, service logic, permissions, and audit contracts. |
| `threadmill-dashboard-ui` | Static React/Tailwind/shadcn operations console packaged as reusable assets under `/threadmill`. |
| `threadmill-dashboard-spring` | Spring MVC/Security adapter exposing `/threadmill/api/**` and mounting UI assets when present; the auto-configured security chain covers both the API and the `/threadmill/**` UI mount. |
| `threadmill-test-support` | The abstract `JobStore` contract test every backend passes. |

## Storage backends

**PostgreSQL** is the primary production backend. Indexed scalar columns
denormalize the indexed job state; the body column holds the
JSON-serialized job. Per-state counts come from a counter table maintained
by a trigger (so the observability path never contends with the claim
path). Migrations are applied automatically on startup; an
`emitPendingSql()` method produces pending SQL for teams that prefer
Flyway/Liquibase, and `emitCleanInstallSql()` emits the full clean-install DDL.

**Redis** is a fully supported first-class backend. Every multi-key state
transition is a single atomic Lua script. Standalone, Sentinel, and Cluster
topologies are configured through one factory path. Redis Cluster uses a
single `{threadmill}` hash slot for correctness; it is topology/failover
support, not sharded job-key distribution in this version. Reliable-fetch:
a worker crash between claim and completion leaves the job in the per-node processing
ZSET for the orphan reaper. Run Redis with AOF persistence
(`appendonly yes`) for the durability needed by a job store — out of the
box Redis is less durable than PostgreSQL.

**In-memory** is for tests and local dev. It is held to the exact same
abstract contract suite as the real backends — not a simplified fake.

## Engine

The processing engine has one job: claim → process → complete.

- The **dispatcher** loop (per node, per configured queue lane) claims
  ready jobs and submits them to a virtual-thread worker pool.
- The **maintenance** loop (master-only) promotes due `SCHEDULED` jobs to
  `ENQUEUED`, reclaims orphans, runs retention
  deletes, and materializes recurring instances.
- A **single failure code path** handles every failure mode — thrown
  exception, per-job timeout, orphan-reclaim, payload poisoning. All four
  go through the same state transition and fire the same
  `JobInterceptor.onProcessingFailed` hook.
- **Retry is an interceptor**, not engine special-case code. The precedence
  model is per-job > per-exception-type > global default.
- The dispatcher's **circuit breaker** decays on success — it pauses the
  loop on repeated infrastructure failure (e.g. a store outage), probes
  for recovery, and resumes. A single poison job is quarantined; it
  cannot trip shutdown.

## Job states

```
                         AWAITING
                            │
                            ▼
SCHEDULED ◄─── (retry) ─── FAILED ──► DELETED
   │                        ▲
   │                        │
   ▼                        │
ENQUEUED ──────────► PROCESSING ──► SUCCEEDED
   │                                  │
   └──────► DELETED ◄──────────────── ┘

QUARANTINED  (terminal — poison jobs only)
PROCESSED    (reserved for the external-jobs feature)
```

Illegal transitions throw `IllegalJobTransitionException`. The transition
table is the single source of truth and is exhaustively tested.

## Configuration

Configuration namespace: `threadmill.*`. Sensible defaults are provided
for every option; override via the framework integration's properties
binder or directly via `ProcessingNodeConfig.builder()`.

| Key | Default | Meaning |
|---|---|---|
| `threadmill.workerCount` | 10 | Workers per default lane |
| `threadmill.pollInterval` | 500ms | Dispatcher fallback poll cadence; scheduled promotion and same-JVM producers sharing a `LocalWakeBus` wake local dispatchers sooner |
| `threadmill.claimHeartbeat` | 15s | Owned-job heartbeat refresh cadence |
| `threadmill.maintenancePollInterval` | 1s | Recurring materialization, scheduled promotion, and orphan-scan cadence |
| `threadmill.retentionInterval` | 1h | Succeeded-job, dedup-key, and stale-node retention cadence |
| `threadmill.heartbeatTimeout` | 60s | Orphan threshold |
| `threadmill.jobTimeout` | 5m | Per-job runtime cap |
| `threadmill.defaultMaxAttempts` | 5 | Retry budget for the global default policy |
| `threadmill.retryInitialBackoff` | 5s | First retry's backoff |
| `threadmill.claimBatchSize` | 10 | Maximum claim per dispatcher tick |
| `threadmill.defaultQueue` | `"default"` | Queue a lane-less node polls; not the queue `Scheduler` convenience methods write to |
| `threadmill.remote-wake.enabled` | `true` | Spring auto-configured durable stores publish cross-node wake hints; dispatcher polling remains the fallback |
| `threadmill.remote-wake.channel` | backend default | Optional channel override to isolate multiple Threadmill deployments sharing one datastore |
| `threadmill.checkInMinInterval` | 5s | Minimum persisted check-in/progress flush interval |
| `threadmill.noProgressTimeout` | 15m | Timeout after the last check-in |
| `threadmill.maxDedupTtl` | 30d | Maximum producer-side deduplication window |

## Java 25

Threadmill is Java-25-only. It uses **virtual threads** for the worker
pool and **scoped values** (final in Java 25) — not `ThreadLocal` — for
per-execution context propagation. No preview features in the public
stable API.

## Status

Shipped in v1:

- Job model with append-only state history, optimistic-lock versioning,
  relationship and result fields, and bounded size.
- Centralised state machine; illegal transitions throw.
- `JobStore` SPI plus three backends (in-memory, PostgreSQL, Redis), each
  held to one shared abstract contract test.
- Processing engine: virtual-thread workers, scoped-value context
  propagation, per-job timeout, single failure code path, circuit-breaker
  decay, store-outage tolerance, orphan-reclaim, poison-job quarantine.
- Scheduling API: enqueue, schedule-at, recurring (5-field cron + interval)
  with an explicit missed-run policy (`DROP` default / `CATCH_UP` opt-in).
- Named queues with per-queue worker lanes (starvation-safe); priority
  within a queue honoured by all three stores.
- Queue-family lanes for dynamically discovered queues such as
  `project:*`, with stride-scheduled weights and empty-queue retention.
- Claim-time per-key concurrency with shared/exclusive modes, including
  workflow-root inheritance so a workflow can hold a resource until its
  last descendant terminates.
- Advanced features: results, workflows (chaining), custom retry policies
  with explicit precedence (per-job > per-exception-type > default),
  cross-cluster mutexes with lease semantics, node tags, atomic job
  replacement, producer-side deduplication, and long-running job check-ins.
- Framework adapter: Spring Boot auto-configuration with `@Job`
  handler discovery, `JobScheduler`, transaction-aware enqueue modes, and
  remote wake hints for durable stores.
- Observability: Micrometer integration; optional OpenTelemetry tracing;
  data-first dashboard snapshot.
- Operations dashboard: adding `threadmill-dashboard-spring` and
  `threadmill-dashboard-ui` to a Spring Boot app mounts the console under
  `/threadmill`, secured by default — see
  [threadmill-dashboard-spring/README.md](threadmill-dashboard-spring/README.md).
- Soak / load suite (separate from `check`) for sustained throughput,
  recurring no-skip, and induced store-outage recovery on all three
  backends.

Not yet in v1 (design-compatible, additive when needed):

- Batches; external-trigger jobs (the `PROCESSED` state is reserved);
  rate limiters; richer cron expressions (business-day, last-of-month).
- Additional dashboard adapters beyond Spring MVC.
- Reproducible production-grade benchmarks.

## Soak regression reference results

Run with `./gradlew :threadmill-soak:soakRegression`. Numbers from a developer laptop
(Apple Silicon, Docker Desktop, single node, 8 workers, 32-batch claim):

These are smoke-test reference results, not portable capacity claims or a
production benchmark. Hardware, container runtime, datastore configuration,
payload size, handler duration, and concurrency-key distribution materially
change throughput. Run the checked-in harness in the target environment for
capacity planning.

| Backend | Throughput (5–10k jobs end-to-end) | Contention / queue-family soak | Recurring (100 ms interval over 5 s) | Induced container-pause recovery |
|---|---|---|---|---|
| In-memory | 10,000 jobs in ~10 s (≈ 950 jobs/sec) | 100 same-key jobs + 1,000 `project:*` jobs complete | ~46 runs | n/a |
| PostgreSQL 18 | 5,000 jobs in ~19 s (≈ 270 jobs/sec) | 100 same-key jobs + 100 `project:*` jobs complete | ~20 runs | pause mid-run → engine pauses → unpause → all jobs complete |
| Redis 7 (AOF) | 5,000 jobs in ~20 s (≈ 250 jobs/sec) | 100 same-key jobs + 100 `project:*` jobs complete | ~22 runs | pause mid-run → engine pauses → unpause → all jobs complete |

The store-outage soak tests exercise the dispatcher's circuit-breaker
recovery in production: the container is paused via the Docker API
mid-run, no progress is made while paused (the loop has paused itself),
and the engine resumes automatically once the store is reachable again.

## Testing

```bash
./gradlew check                  # compile + tests + spotless check
./gradlew build                  # the above + all jars
./gradlew spotlessApply          # auto-format Java / Kotlin / *.gradle.kts to project style
./gradlew :threadmill-soak:soakRegression  # fixed throughput / outage regression suite
./gradlew :threadmill-soak:soakPostgres    # tunable load soak harness
./gradlew productionCheck        # release-candidate validation gauntlet
```

The PostgreSQL and Redis tests use Testcontainers and require a running
container runtime (Docker / Podman / Colima / OrbStack).

## Examples

`threadmill-example` is a compiled example app under the package
`com.example.threadmill`. It includes:

- a minimal in-memory getting-started program;
- manual multi-instance Postgres and Redis worker examples;
- submitter examples that enqueue work into a shared Postgres or Redis store.

Start with `threadmill-example/README.md`.

## Code formatting

Java is formatted with **Palantir Java Format**; Kotlin / `*.gradle.kts`
with **ktfmt**. The formatter is pinned in `gradle/libs.versions.toml` so
everyone on the team ends up with byte-identical output, and `./gradlew
check` fails on violations.

## License

Apache License 2.0. See [LICENSE](LICENSE).
