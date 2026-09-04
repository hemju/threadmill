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

## Store Outages During Completion

If a handler finishes while its job store is unavailable, the worker remains
occupied and retries the terminal transition with capped backoff. Threadmill
does not return that worker to the pool or abandon the persisted job in
`PROCESSING`. Owner heartbeats continue while finalization is active so another
node cannot execute the same attempt concurrently. Once the store recovers,
the terminal save completes normally. If the node shuts down first, its retry
and heartbeats stop; the maintenance leader then reclaims the job after
`heartbeatTimeout` under the usual at-least-once semantics.

## Metrics

Use `ThreadmillMetrics` with a Micrometer registry. Key meters include job
counts by state, queue depths, oldest enqueued age by queue, oldest processing
heartbeat age, processed/failed counters, orphan reclaim, processing time,
claim latency/failures, rejected writes, and metric refresh health.

Wire `metrics.meteredStore()` into both processing nodes and producers, plus
`metrics.asInterceptor()` into each processing node. Claims and rejected
writes are observed at that store boundary; the interceptor records lifecycle
signals only after their state transition commits.

Store gauges refresh on pull at most once per second by default, even when no
job completes. A failed refresh retains the last successful counts/depths,
keeps age gauges advancing from the last known timestamps, sets
`threadmill.metrics.snapshot.stale` to `1`, and increments
`threadmill.metrics.refresh.errors`. Treat those values as last-known data
until stale clears; use `threadmill.metrics.snapshot.age` to judge their age.
Concurrent readers use the cached snapshot while a refresh is in flight. The
reader that starts the refresh performs the bounded store reads synchronously;
the refresh interval and queue cap therefore budget store load as well as
freshness and cardinality. An explicit `metrics.refresh()` waits for an
in-flight pull and then runs a new pass, preserving its immediate-refresh
contract for host-driven checks.

Queue tags are capped at 100 active queues per metrics instance by default.
`threadmill.metrics.queue.tags.omitted` reports how many active queues did not
receive a tag slot. Drained queues release slots for newly appearing queues.
The constructor overload accepts a different refresh interval and queue cap;
zero disables per-queue meters.

`threadmill.store.writes.rejected` counts failed write attempts, including
retries during one logical outage. It excludes the SPI's expected
stale-version, oversize, invalid-argument, and duplicate-id outcomes so normal
multi-node races and caller validation errors do not masquerade as store
health failures.

Queue-meter registration/removal failures increment
`threadmill.metrics.queue.meter.errors` and are logged separately. They do not
mark the store snapshot stale because counts and timestamps may already be
current even when the registry rejects a meter.

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

## Remote Wake

Each dispatcher still polls its store, but Spring auto-configured durable
stores also publish cross-node wake hints after local wake timing has already
decided that work is visible. Postgres uses `LISTEN`/`NOTIFY` on
`threadmill_wake` by default. Redis uses Pub/Sub on `{threadmill}:wake` by
default. Set `threadmill.remote-wake.channel` when multiple isolated
Threadmill deployments share one datastore.

The Postgres listener holds one JDBC connection for as long as remote wake is
enabled. If it uses the same application pool, size that pool with one
additional connection reserved for the listener. Applications with saturated
pools can provide a custom `PostgresRemoteWakeChannel` bean with a dedicated
one-connection listener `DataSource`.

Remote wake failures are logged at debug level and ignored. A missed
notification can only add latency up to `threadmill.pollInterval`; it cannot
skip work or change ownership. Disable the optimization with:

```yaml
threadmill:
  remote-wake:
    enabled: false
```

## Deduplication

Producer-side deduplication coalesces duplicate submissions by `(queue,
dedupKey)`. It prevents duplicate enqueues, not duplicate executions. Expired
dedup records are retained while the referenced job is still active, then
removed by maintenance after the job is terminal or gone.

## Long-Running Jobs

Handlers can call `checkIn`, `updateProgress`, and `log` through
`JobExecutionContext`. Once a handler checks in, `noProgressTimeout` replaces
the wall-clock `jobTimeout`: a job can run for a long time as long as it keeps
making progress. `ctx.deadline()` and `ctx.remaining()` expose the resulting
deadline so a handler can wind down before the engine interrupts it; see
[Handlers → Timeouts](handlers.md#timeouts).

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

`ProcessingNode.close()` stops the dispatchers, publishes a shutdown deadline
(`now + shutdownGracePeriod`) that every in-flight handler sees through
`ctx.deadline()` / `ctx.remaining()`, and waits for in-flight jobs up to that
deadline. Whatever is still running is then marked `SHUTDOWN` and interrupted;
the interrupted attempt is rescheduled immediately without consuming a retry
attempt, and a surviving node picks it up at the next promotion. Maintenance
and the owner-heartbeat loop stay alive through the drain so peers do not
orphan-reclaim jobs that are merely draining.

A handler that checks `ctx.remaining()` between steps winds down inside the
grace period and is never interrupted. One whose steps are longer than the
grace period is cut mid-step regardless — lengthen `shutdownGracePeriod` for
such workloads. See [Handlers → Timeouts](handlers.md#timeouts) for what the
interrupt does on a virtual thread.

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
