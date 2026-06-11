# threadmill-store-memory

In-memory implementation of the `JobStore` SPI. Used for tests and local
development. **Never for production** — data is lost on restart.

## When to use

- **Tests.** Every contract test in `AbstractJobStoreContractTest` runs against
  this store via `InMemoryJobStoreContractTest`. The same 76 tests run
  against Postgres and Redis, so passing on memory means the behaviour
  matches the real backends.
- **Local dev.** The Spring Boot auto-config falls back to this when no
  Redis is configured and no `DataSource` is present, with a loud warning.
- **Quickstart.** `threadmill-example/GettingStartedMain.java` uses this store
  to demonstrate the lifecycle end-to-end in one file.

## What it provides

The same operational guarantees as the real backends:

- Atomic claim across virtual-thread workers (single mutex).
- Optimistic-lock version semantics identical to Postgres / Redis.
- All concurrency-group semantics (`EXCLUSIVE` / `SHARED`, workflow inheritance).
- Producer-side deduplication (`enqueueIfAbsent`).
- Maintenance lease, mutex SPI, atomic replacement, retention.

What it doesn't provide:

- **Durability across JVM restart.** Everything is in process memory.
- **Cross-JVM coordination.** A single-process store; if you run multiple
  `ProcessingNode`s in the same JVM they share state through this store,
  but separate JVMs each have their own.

## Design — not a simplified fake

The in-memory store keeps the **serialized wire form**, not the `Job` object.
Every `insert` and `saveAtomic` round-trips through `JsonJobSerializer`, so
the serializer is exercised on every operation. This catches serializer
regressions in tests that would otherwise only surface against the real
backends.

This is deliberate. A "simpler" in-memory store that holds `Job` references
would diverge from the real backends in subtle ways (4-byte Unicode handling,
oversize behaviour, snapshot-on-serialize correctness) and let bugs through
the contract suite.

## Build

```
./gradlew :threadmill-store-memory:test
```
