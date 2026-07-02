# Threadmill Docs

## Start here

- [Getting started](getting-started.md) — five-minute Spring Boot quickstart.
- [Spring quickstart](quickstart.md) — Spring Boot wiring, `@Job`,
  transaction-aware enqueue modes.

## Deep dives

- [Architecture](architecture.md) — the module map, the engine loops
  (`ProcessingNode`, `Dispatcher`, `MaintenanceCycle`, `NodeRegistry`), the
  `JobStore` SPI and its three backends, the state machine, wake signals.
- [Handlers](handlers.md) — the `JobPayload` / `JobHandler` contract, handler
  resolution, `JobExecutionContext`, the scoped-value caveat, timeouts, retry
  precedence.
- [**Transactions**](transactions.md) — atomic boundaries per backend, what
  happens when the handler throws inside `@Transactional`, the at-least-once
  contract, the outbox pattern. **Read this before writing a handler.**
- [Concurrency](concurrency.md) — per-key locks (`EXCLUSIVE` / `SHARED`),
  workflow inheritance, worked examples.
- [Backend execution model](backend-execution-model.md) — Postgres and Redis
  claim/save paths, lock scopes, parallelism, and starvation boundaries.
- [Queue topology](queue-topology.md) — lanes, families, weights, pause /
  resume.
- [Long-running jobs](long-running-jobs.md) — `ctx.checkIn()`,
  `noProgressTimeout`, progress reporting.
- [Deduplication](deduplication.md) — `enqueueIfAbsent`, dedup TTL semantics.

## Reference

- [Configuration](configuration.md) — every `threadmill.*` property.
- [PostgreSQL schema](postgres-schema.md) — schema modes, manual DDL, and
  reset guidance.
- [Redis topologies](redis-topologies.md) — standalone, Sentinel, Cluster.
- [Operations](operations.md) — production runbook, pause / resume, monitoring.
- [Troubleshooting](troubleshooting.md) — symptom → cause → fix.
- [Migration](migration.md) — replacing an existing job or scheduler system.
- [Release checklist](release-checklist.md).

## Operations dashboard

Threadmill ships a mountable operations console for Spring Boot
applications. Add `threadmill-dashboard-spring` and `threadmill-dashboard-ui`
to the classpath: the JSON API is exposed under `/threadmill/api/**` and the
static UI is mounted under `/threadmill`. Security is required by default —
Threadmill registers a scoped `SecurityFilterChain` that demands
authentication (and CSRF tokens for mutating requests), and startup fails if
no chain exists unless the unsafe read-only local mode is explicitly enabled.
Sensitive job details (payloads, metadata, logs, results, failure messages)
are redacted unless explicitly exposed and permitted. See
[`threadmill-dashboard-spring/README.md`](../threadmill-dashboard-spring/README.md)
for permissions, configuration properties, audit hooks, and operator actions.

## Per-module deep dive

The per-module READMEs cover schema, key layout, and module-specific
operational notes:

- [`threadmill-core`](../threadmill-core/README.md) — the model, the engine,
  the SPI, the invariants.
- [`threadmill-store-postgres`](../threadmill-store-postgres/README.md) — the
  full schema, PG18-only enforcement, claim semantics, deadlock retry,
  connection-pool sizing.
- [`threadmill-store-redis`](../threadmill-store-redis/README.md) — the full
  key layout, the Lua script inventory, the reliable-fetch claim, AOF
  durability.
- [`threadmill-store-memory`](../threadmill-store-memory/README.md) — when to
  use it, what it does and doesn't provide.
- [`threadmill-spring-boot`](../threadmill-spring-boot/README.md) — auto-config,
  store auto-detection, `SmartLifecycle` phase choice, transactional enqueue.
- [`threadmill-metrics`](../threadmill-metrics/README.md) — every meter name,
  recommended alerts.
- [`threadmill-tracing`](../threadmill-tracing/README.md) — optional
  OpenTelemetry spans.
- [`threadmill-dashboard-api`](../threadmill-dashboard-api/README.md) —
  portable dashboard contract and `EngineSnapshot` shape.
- [`threadmill-dashboard-spring`](../threadmill-dashboard-spring/README.md) —
  Spring MVC/Security dashboard adapter.
- [`threadmill-soak`](../threadmill-soak/README.md) — sustained throughput,
  container-pause recovery numbers.
- [`threadmill-test-support`](../threadmill-test-support/README.md) — how to
  add a new backend.
- [`threadmill-example`](../threadmill-example/README.md) — runnable demos.
