# Operations Guide

## Delivery Model

Threadmill provides at-least-once delivery. Duplicate execution is a normal
failure-recovery outcome, so handlers must be idempotent and should use an
application-level idempotency key for external side effects.

## Leadership

Each `ProcessingNode` records a heartbeat and tries to renew a store-backed
maintenance lease. Only the lease holder promotes scheduled jobs, materializes
recurring jobs, reclaims orphans, and runs retention. If the store is
unreachable, nodes stop acting as maintenance leader.

## Metrics

Use `ThreadmillMetrics` with a Micrometer registry. Key meters include job
counts by state, queue depths, oldest enqueued age by queue, oldest processing
heartbeat age, processed/failed counters, processing time, claim latency, and
metric refresh errors.

## Datastores

PostgreSQL uses indexed scalar columns plus a JSON body. The claim path uses
`FOR UPDATE SKIP LOCKED`; per-state counts come from `threadmill_job_counts`.

Redis uses a per-job hash, active-state sorted sets, per-state counts, and Lua
scripts for multi-key transitions. All keys use one `{threadmill}` Cluster slot:
Cluster gives topology/failover support in this version, not sharded job-key
distribution. Run Redis with AOF enabled, for example `appendonly yes` and a
durability policy appropriate for the application.
Redis must use `maxmemory-policy noeviction`; alert on rejected writes,
oldest processing heartbeat age, orphan reclaim count, claim failures, queue
depth, and Redis persistence/replication health.

## Deduplication

Producer-side deduplication coalesces duplicate submissions by `(queue,
dedupKey)`. It prevents duplicate enqueues, not duplicate executions. Expired
dedup records are retained while the referenced job is still active, then
removed by maintenance after the job is terminal or gone.

## Long-Running Jobs

Handlers can call `checkIn`, `updateProgress`, and `log` through
`JobExecutionContext`. Once a handler checks in, `noProgressTimeout` replaces
the wall-clock `jobTimeout`: a job can run for a long time as long as it keeps
making progress.

## Pausing a Queue

`Scheduler.pauseQueue(queue, reason)` (or the Spring `JobScheduler` equivalent)
stops claim cycles from picking up jobs on `queue`. Pending jobs stay
`ENQUEUED`; in-flight jobs run to completion. The reason string is recorded
for operator audit trails. Resume with `Scheduler.resumeQueue(queue)`. Both
operations are idempotent and reflected in `EngineSnapshot.pausedQueues()`.

Pausing is a per-queue operation, not per-node — the dispatcher reads the
shared pause set on every claim cycle, so a pause issued from any node takes
effect on every node within one poll interval.

Threadmill claims by explicit queue name, never with a wildcard at the store
layer. Per-queue pause therefore does not incur a `DISTINCT(queue)` scan; the
store performs a constant-time check against `threadmill_queue_pauses`
(PostgreSQL) or the `{threadmill}:queue_pauses` hash (Redis). Do **not** add a
wildcard claim selector at the store layer "for parity" — the current shape is
the performance win.

## Shutdown

`ProcessingNode.close()` stops dispatchers, stops maintenance and registry,
waits for in-flight jobs up to `shutdownGracePeriod`, then interrupts remaining
work. Unfinished jobs are recovered through orphan recovery.

## Diagnosing under load

The `threadmill-soak` module ships an operator-driven harness for reproducing
load-related symptoms against any backend. Each run writes a self-contained
artifact directory under `build/soak/` that an AI agent (or a human) can read
cold to answer "did it behave correctly?" and "how fast was it?".

Pick a scenario by symptom:

| Symptom | Scenario |
|---|---|
| Queue-depth spike with no obvious cause | `mixed-workload` |
| Lock-wait p99 looks high under contention | `rw-lock-stress` |
| One project queue dominating despite equal weighting | `weighted-queues` |
| Retries / failures blowing up | `retry-storm` |
| Long-running jobs being killed prematurely | `long-running` |
| Queue pause didn't take effect | `pause-resume` |
| Bulk-enqueue path slower than per-job | `bulk-enqueue` |
| Node crash didn't trigger orphan reclaim | `crash-recover` |

Invocation cheat-sheet:

```bash
./gradlew :threadmill-soak:soakMemory   -Pscenario=mixed-workload -Pduration=120s
./gradlew :threadmill-soak:soakPostgres -Pscenario=rw-lock-stress  -Pduration=300s
./gradlew :threadmill-soak:soakRedis    -Pscenario=mixed-workload -Pduration=120s
```

See `threadmill-soak/README.md` for the full `-P` property list, the output
directory layout, and the AI-drop-in workflow.
