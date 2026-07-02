# Architecture

Threadmill is a background job-processing library for Java 25 built from
well-understood parts: a durable job store, optimistic concurrency control,
polling worker loops, and an explicit state machine. This page is the
ten-minute structural tour; each section links to the deeper per-module
README.

## Delivery guarantee

Threadmill provides **at-least-once delivery**. A job may run more than once —
after a node crash, after a long GC pause that makes a heartbeat look expired,
after a transient store outage. **Handlers must be idempotent.** Every other
design decision below assumes this contract.

## Modules

Modules are physically separate: no storage implementation and no UI is ever
bundled into `threadmill-core`.

| Module | Purpose |
|---|---|
| [`threadmill-core`](../threadmill-core/README.md) | Job model, state machine, `JobStore` SPI, serialization, engine, interceptors, scheduling API. No storage, no framework code. |
| [`threadmill-store-postgres`](../threadmill-store-postgres/README.md) | PostgreSQL 18+ backend: schema, in-process migrations, `SELECT … FOR UPDATE SKIP LOCKED` claim. |
| [`threadmill-store-redis`](../threadmill-store-redis/README.md) | Redis backend: atomic Lua scripts, standalone / Sentinel / Cluster topologies. |
| [`threadmill-store-memory`](../threadmill-store-memory/README.md) | In-memory store for tests and local dev — held to the same contract as the real backends. |
| [`threadmill-spring-boot`](../threadmill-spring-boot/README.md) | Spring Boot 4.x auto-configuration, `@Job` handler discovery, transactional enqueue modes. |
| [`threadmill-metrics`](../threadmill-metrics/README.md) | Micrometer integration. |
| [`threadmill-tracing`](../threadmill-tracing/README.md) | Optional OpenTelemetry API integration. |
| [`threadmill-dashboard-api`](../threadmill-dashboard-api/README.md) / [`-spring`](../threadmill-dashboard-spring/README.md) / `-ui` | Data-first dashboard contract, Spring MVC/Security adapter, static React console. |
| [`threadmill-test-support`](../threadmill-test-support/README.md) | The abstract `JobStore` contract test every backend passes. |

## The engine

A `ProcessingNode` is the per-application-instance execution engine — one per
JVM. It composes four cooperating parts:

- **`Dispatcher`** — the loop that claims and dispatches work. One dispatcher
  runs per queue lane; it claims ready jobs from the store and submits each to
  a virtual-thread worker. Capacity is enforced by a semaphore per
  `QueueLane`, so a flood on one queue cannot occupy capacity reserved for
  another. `QueueFamily` lanes (`project:*` patterns) discover matching queues
  dynamically and spread claims across them by weight. A circuit breaker
  pauses the loop on repeated store failure and probes for recovery — a store
  outage pauses the cluster, it does not kill it. The breaker decays on
  success, and a single poison job is quarantined rather than tripping it.
- **`MaintenanceCycle`** — master-only housekeeping: promotes due `SCHEDULED`
  jobs to `ENQUEUED`, materializes recurring (`CronTask`) instances, reclaims
  orphaned `PROCESSING` jobs whose owner heartbeat expired, and runs retention
  sweeps of terminal jobs, expired dedup keys, and stale node records.
- **`NodeRegistry`** — cluster membership: records this node's heartbeat and
  acquires or renews the store-backed maintenance lease. The
  `MaintenanceCycle` runs only on the lease holder, so exactly one node does
  housekeeping at a time and a dead master's lease simply expires.
- **`JobRunner`** — executes the user handler on a virtual thread with the
  per-execution context bound as a `ScopedValue`, enforces the per-job
  timeout, and persists the terminal transition.

`Scheduler` is the user-facing enqueue / schedule / recurring API. It needs
only a `JobStore` and a serializer — no running engine — so submission-only
processes work.

## The `JobStore` SPI and the three backends

The persistence boundary is the `JobStore` SPI, expressed in operations and
guarantees, not SQL — so a relational store and a key-value store both satisfy
one contract honestly. A capabilities descriptor documents real differences
(for example, Redis advertises `supportsRichSearch=false`). Every backend
extends the same abstract contract test suite in `threadmill-test-support`
and must pass it in full before adding anything backend-specific.

The load-bearing guarantee is that **`claimReady` is atomic across nodes** —
two nodes can never claim the same job:

- **In-memory** serializes claims through a single mutex, and deliberately
  round-trips every job through the serializer so tests exercise the wire
  format continuously.
- **PostgreSQL** pages candidates with `SELECT … FOR UPDATE SKIP LOCKED` and
  applies a version-matched `UPDATE` in the same transaction. Per-state counts
  come from a trigger-maintained counter table, so observability never scans
  the jobs table. Migrations are applied in-process at startup. PostgreSQL 18+
  only, enforced at startup.
- **Redis** commits every multi-key transition in a single atomic Lua script.
  The claim is a crash-safe reliable fetch, never a destructive pop: a crash
  before the script leaves the job `ENQUEUED`, a crash after it leaves a
  complete `PROCESSING` record for orphan recovery. Every engine key lives in
  the `{threadmill}` hash slot, so Cluster mode works with multi-key scripts.
  Run Redis with AOF persistence; `noeviction` is required at startup.

The claim/save paths, lock scopes, and parallelism boundaries of the two
durable backends are described in detail in
[Backend execution model](backend-execution-model.md).

## The state machine

Job state is an append-only history with one centralised transition table.
Illegal transitions throw `IllegalJobTransitionException` — never coerced
silently.

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

- `AWAITING` — a workflow successor waiting for its predecessor.
- `SCHEDULED` — due at a future instant; maintenance promotes it.
- `ENQUEUED` — claimable by a dispatcher.
- `PROCESSING` — claimed and owned by a node, heartbeat-refreshed.
- `SUCCEEDED` / `FAILED` / `DELETED` — terminal outcomes (`FAILED` may be
  retried back to `SCHEDULED` by the retry interceptor).
- `QUARANTINED` — the payload or handler cannot be loaded; never retried.
- `PROCESSED` — reserved for a future external-trigger feature.

**There is one failure code path.** A thrown exception, a per-job timeout, a
no-progress timeout, and orphan reclaim all funnel through the same
`PROCESSING → FAILED` transition and fire the same
`JobInterceptor.onProcessingFailed` hook. Orphan recovery never moves a job
directly back to `ENQUEUED` — `PROCESSING → ENQUEUED` is explicitly illegal —
so every failure source gets identical retry and interceptor semantics. Retry
itself is an interceptor, not engine special-casing (see
[Handlers](handlers.md) for the precedence model).

## Wake signals

Polling (`threadmill.pollInterval`) is the correctness mechanism; wake signals
are latency hints layered on top:

- **`WakeSignal`** lets an idle worker wake its dispatcher early when new work
  arrives, instead of waiting out the poll interval.
- **`LocalWakeBus`** connects same-JVM producers, recurring materialization,
  and scheduled-job promotion to matching dispatchers. Spring wires the shared
  bus automatically; manual core wiring passes the same bus to `Scheduler`
  and `ProcessingNode`.
- **`RemoteWakeChannel`** publishes the same hints across nodes for the
  durable stores: Postgres uses `LISTEN`/`NOTIFY` on `threadmill_wake`, Redis
  uses Pub/Sub on `{threadmill}:wake`. Publish or listen failures are ignored
  — the poll interval remains the fallback, so a broken wake channel degrades
  latency, never correctness.

## Further reading

- [Backend execution model](backend-execution-model.md) — claim/save paths
  per backend.
- [Concurrency](concurrency.md) — claim-time per-key locks and workflow
  inheritance.
- [Queue topology](queue-topology.md) — lanes, families, weights, pause /
  resume.
- [Transactions](transactions.md) — atomic boundaries and the at-least-once
  contract.
- [`threadmill-core/README.md`](../threadmill-core/README.md) — the engine's
  key invariants.
