# Threadmill Docs

## Start here

- [Getting started](getting-started.md) — five-minute Spring Boot quickstart.
- [Spring quickstart](quickstart.md) — Spring Boot wiring, `@ThreadmillJob`,
  after-commit enqueue.

## Deep dives

- [**Transactions**](transactions.md) — atomic boundaries per backend, what
  happens when the handler throws inside `@Transactional`, the at-least-once
  contract, the outbox pattern. **Read this before writing a handler.**
- [Concurrency](concurrency.md) — per-key locks (`EXCLUSIVE` / `SHARED`),
  workflow inheritance, worked examples.
- [Queue topology](queue-topology.md) — lanes, families, weights, pause /
  resume.
- [Long-running jobs](long-running-jobs.md) — `ctx.checkIn()`,
  `noProgressTimeout`, progress reporting.
- [Deduplication](deduplication.md) — `enqueueIfAbsent`, dedup TTL semantics.

## Reference

- [Configuration](configuration.md) — every `threadmill.*` property.
- [Redis topologies](redis-topologies.md) — standalone, Sentinel, Cluster.
- [Operations](operations.md) — production runbook, pause / resume, monitoring.
- [Troubleshooting](troubleshooting.md) — symptom → cause → fix.
- [Migration](migration.md) — from Quartz / Solid Queue / custom systems.
- [Release checklist](release-checklist.md).

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
- [`threadmill-dashboard`](../threadmill-dashboard/README.md) — `EngineSnapshot`
  shape.
- [`threadmill-soak`](../threadmill-soak/README.md) — sustained throughput,
  container-pause recovery numbers.
- [`threadmill-test-support`](../threadmill-test-support/README.md) — how to
  add a new backend.
- [`threadmill-example`](../threadmill-example/README.md) — runnable demos.
