# threadmill-simulation

End-to-end correctness verification under realistic mixed workload and
worker-process churn. Not a performance benchmark (that's
`threadmill-soak`), not a teaching example (that's `threadmill-example`) — a
third thing: simulations that exercise load-bearing primitives and produce
JSON-lines traces for verification or review.

## Correctness Simulation

This is the short fixed invariant verifier. It runs in seconds, uses
Testcontainers for Postgres and Redis, and fails the Gradle task when a backend
does not drain or the trace verifier finds an invariant violation.

```bash
./gradlew :threadmill-simulation:simulate
```

Runs all three backends sequentially (in-memory, Postgres 18 via
Testcontainers, Redis via Testcontainers). One command, full validation. Or
pick one:

```bash
./gradlew :threadmill-simulation:simulateMemory     # fast, no Docker
./gradlew :threadmill-simulation:simulatePostgres   # Testcontainers
./gradlew :threadmill-simulation:simulateRedis      # Testcontainers
```

### What the Scenario Does

50 projects, each with its own queue (`project:1` through `project:50`). Two
job shapes per project:

- **Import** — `EXCLUSIVE` lock on `project:N`, slower (30 ms simulated).
- **Export** — `SHARED` lock on `project:N`, faster (8 ms simulated).

Mix: ~10% imports, ~90% exports. 400 jobs total — small enough to finish in
seconds, big enough to exercise contention. Half the jobs are enqueued
one-by-one via `Scheduler.enqueue(...)`; the other half via
`JobStore.insertAll(...)` so the bulk-insert path runs under contention
with the engine already draining work.

Failure injection per attempt:
- **5%** throw a `RuntimeException` (retry path).
- **0.5%** hang past `jobTimeout` (timeout path).

Mid-run, a randomly-selected project is paused via
`Scheduler.pauseQueue(...)` and resumed after a moment.

### What the Verifier Asserts

Every JSON-lines event in `build/simulation/*.jsonl` is read back into
`TraceVerifier`. Violations are reported with the exact event chain that
proves them. Invariants:

- **At-least-once delivery.** Every enqueued job has a terminal event:
  `succeeded`, `quarantined`, or `failed`/`timed_out` with `final=true`.
- **Concurrency exclusion.** For any concurrency key, an `EXCLUSIVE`
  `lock_acquired` never overlaps with another lock for the same key; a
  `SHARED` `lock_acquired` never overlaps with an `EXCLUSIVE` for the same
  key.
- **Lock pairing.** Every `lock_acquired` has a matching `lock_released`
  for the same job id and key.
- **Pause obeyed.** Between `queue_paused` and the matching `queue_resumed`
  for one queue, no `claimed` event names that queue.

### Trace Format

JSON-lines (`*.jsonl`). One event per line. Every line is a complete JSON
object with at least `timestamp` (ISO-8601 UTC) and `event`. Common fields:

```
{ "timestamp": "…", "event": "enqueued",  "jobId": "…", "queue": "…", "type": "import|export", "lockKey": "…", "lockMode": "EXCLUSIVE|SHARED" }
{ "timestamp": "…", "event": "claimed",   "jobId": "…", "queue": "…", "attempt": 1 }
{ "timestamp": "…", "event": "lock_acquired",  "jobId": "…", "lockKey": "…", "lockMode": "…" }
{ "timestamp": "…", "event": "succeeded", "jobId": "…", "queue": "…", "attempts": 2, "final": true }
{ "timestamp": "…", "event": "failed",    "jobId": "…", "queue": "…", "attempts": 1, "causeKind": "EXCEPTION", "causeMessage": "…", "final": false }
{ "timestamp": "…", "event": "timed_out", "jobId": "…", "queue": "…", "attempts": 2, "causeKind": "TIMEOUT",    "final": true  }
{ "timestamp": "…", "event": "lock_released",  "jobId": "…", "lockKey": "…", "lockMode": "…" }
{ "timestamp": "…", "event": "queue_paused",   "queue": "…" }
{ "timestamp": "…", "event": "queue_resumed",  "queue": "…" }
{ "timestamp": "…", "event": "node_started",   "nodeId": "…", "workerCount": 8, "backend": "memory|postgres|redis" }
{ "timestamp": "…", "event": "node_stopped",   "nodeId": "…" }
```

### Exit Code

The Gradle task fails (non-zero exit) when **any** backend either:

- didn't fully drain inside the run budget, or
- produced a trace with at least one verifier violation.

### Adding A New Scenario

Add a new payload type and handler in `SimulationPayloads`, register it in
`SimulationRunner.enqueueWorkload`. If your invariant isn't on the
verifier's existing list, add a new check method in `TraceVerifier` and
document it above.

## Worker-Churn Simulation

This is the long-running multi-process simulation. A supervisor starts worker
JVMs against one shared external datastore, enqueues jobs continuously, kills
and recreates workers, and writes a JSON-lines trace under
`build/simulation/worker-churn-<timestamp>-<backend>.jsonl` by default.

Start local shared datastores from the repository root:

```bash
docker compose up -d postgres redis
```

Defaults:

- Postgres: `jdbc:postgresql://localhost:55432/threadmill`
- Redis: `redis://localhost:56379/0`

Override with:

- `THREADMILL_JDBC_URL`
- `THREADMILL_DB_USER`
- `THREADMILL_DB_PASSWORD`
- `THREADMILL_REDIS_URI`

Postgres, 10 minutes:

```bash
./gradlew :threadmill-simulation:simulateWorkerChurnPostgres --args="--backend postgres --duration 10m --workers 4 --jobs-per-second 20 --kill-every 30s --drain-timeout 3m"
```

Redis, 10 minutes:

```bash
./gradlew :threadmill-simulation:simulateWorkerChurnRedis --args="--backend redis --duration 10m --workers 4 --jobs-per-second 20 --kill-every 30s --drain-timeout 3m"
```

For an overnight run, use `--duration 8h`. To disable forced process kills,
use `--kill-every 0`. Pass `--trace path/to/trace.jsonl` to choose a custom
trace location, or `--queue name` when multiple supervisors should share one
queue deliberately.

The trace contains supervisor start/stop, worker process starts, exits, kills,
and restarts, every job enqueue, every attempt start and finish, interrupted
attempts, and periodic store snapshots. It is a production-behaviour trace
generator, not a replacement for the contract tests.

## Why a separate module

`threadmill-soak` is about sustained load and operational performance
artifacts. `threadmill-example` is about teaching users how to wire
Threadmill. This module is doing a third thing: correctness verification and
worker-process churn under realistic conditions.
