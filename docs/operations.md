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

Shutdown immediately revokes local leadership and waits up to one second for
the registry loop to finish its current write. It withdraws the heartbeat and
lease both during shutdown and after the loop's last in-flight write, so a
datastore call that ignores interruption cannot leave a renewed lease behind
after it returns. Withdrawal is best effort during an outage; lease expiry
remains the fallback. Cleanup temporarily clears and then restores interruption
so interrupt-sensitive clients can send the final withdrawal.

## Store Outages During Completion

If a handler finishes while its job store is unavailable, the worker remains
occupied and retries the terminal transition with capped backoff. Threadmill
does not return that worker to the pool or abandon the persisted job in
`PROCESSING`. Owner heartbeats continue while finalization is active so another
node cannot execute the same attempt concurrently. Once the store recovers,
the terminal save completes normally. If the node shuts down first, its retry
and heartbeats stop; the maintenance leader then reclaims the job after
`heartbeatTimeout` under the usual at-least-once semantics.

## Fatal JVM Errors and Process Supervision

Threadmill contains ordinary handler exceptions and `AssertionError` as
per-job failures. Handler linkage and initialization failures can be safely
quarantined. It does not contain `VirtualMachineError` (including
`OutOfMemoryError` and `StackOverflowError`) or `ThreadDeath`, even when one is
wrapped in a simple cause chain. Those errors escape handler, interceptor,
dispatcher, maintenance, registry, and remote-wake boundaries before logging,
failure serialization, interceptor callbacks, or retry logic can treat the JVM
as healthy.

Escaping an engine boundary terminates that engine thread, not necessarily the
JVM. Threadmill deliberately does not call `System.exit` or `Runtime.halt`; the
host owns termination policy. Without a host policy, a fatal error on a worker
can leave its job `PROCESSING` while the node's owner heartbeat continues to
refresh it. Orphan recovery cannot reclaim that job, and its claim-time
concurrency slot remains held, until the node stops and its heartbeat expires.
A fatal error on a long-lived engine loop can similarly leave a live but
impaired process.

Production deployments must therefore convert uncaught process-fatal errors
into process termination and run the service under a supervisor that restarts
it. For `OutOfMemoryError`, use `-XX:+ExitOnOutOfMemoryError`, or
`-XX:+CrashOnOutOfMemoryError` when a fatal-error log or core dump is required.
For other process-fatal errors, install a process-wide
`Thread.setDefaultUncaughtExceptionHandler` before starting Threadmill that
invokes the host's termination policy, or use an equivalent liveness watchdog
that terminates an impaired process. The default handler is JVM-global, so the
application—not this library—must own it. Java 25 no longer provides
`Thread.stop()` as a source of `ThreadDeath`, but application or instrumentation
code can still throw it explicitly.

After process termination stops the node heartbeat, normal heartbeat expiry
and orphan recovery provide at-least-once redelivery on a surviving node. The
handler must therefore remain idempotent.

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

### Maintenance capacity and backlog ages

Unfinished correctness-recovery passes resume on each `maintenancePollInterval`,
independently of retention. Retry recovery and workflow reconciliation each
inspect at most 500 jobs per tick, retaining an exclusive job-id cursor across
ticks. After a complete pass each activity pauses for 30 seconds, so retained
final failures and legitimate waiting children are not continuously re-read.
Workflow reconciliation reads each distinct parent once per page. Recovery of a
crashed completion hook can therefore wait that interval plus one pass; stranded
retry recovery also requires a five-minute-old failure. Recurring
materialization visits at most 64 definitions per tick with a stable name cursor.
Promotion handles at most 500 candidates. These activities yield between records
at a cooperative 200 ms budget; a single datastore call or interceptor can exceed
that budget, so configure datastore timeouts as well. A full sweep takes at least
`ceil(population / page size)` ticks, and can take longer under load. Monitor lag
when sizing the maintenance interval and recurring-definition population.

Retention still uses batches of 100 with at most 50 batches per terminal state
and a cooperative 200 ms budget. When a sweep exhausts either budget, it resumes
on the next maintenance tick. `retentionInterval` is the delay after a completed
sweep, rather than a throttle limiting cleanup to 5,000 records per hour. Expired
deduplication cleanup follows the same rule. Correctness activities run first;
a nonfatal failure in one activity does not suppress the others.

Retention scans at most 100 cutoff-eligible candidates per store call, ordered
by transition time and ID in the bundled stores. Recent jobs do not consume its
page budget or require body reads. Its opaque `RetentionCursor` advances past
protected records and survives deletion of the previous cursor's job, including
timestamp ties. Keep the state and cutoff fixed for an entire pass. Deletion
atomically checks state/version, live dedup protection, and waiting
children. A successful workflow predecessor remains until its waiting children
have been promoted; `FAILED` jobs remain while a retry is pending or their legacy
retry decision is unknown. Unreadable failure decisions also remain for operator
review. Use `JobStore.deleteFinishedPage` and its returned cursor for manual
bounded maintenance; `deleteFinishedOlderThan` inspects only the first page.

With `ThreadmillMetrics.meteredStore()`, `threadmill.retention.deleted` counts
actual deletions by terminal state or `dedup`; its rate measures cleanup capacity.
`threadmill.maintenance.oldest.age` reports milliseconds since the oldest due time
for `SCHEDULED`, and since the oldest state entry for other states. The scheduled
gauge is promotion lag. `AWAITING` and `FAILED` ages are investigation signals:
they include legitimate waiting children and final failures, respectively. For
terminal states, subtract the configured retention age to estimate overdue
storage. This is an upper bound: active deduplication windows can protect old
records. The gauges share the normal cached snapshot and staleness indicators.

A release soak must shorten retention enough to reach steady state, then compare
input and deletion rates, record count and storage size, and promotion/recovery
latency while cleanup is active. An eight-hour run with seven-day retention cannot
validate retention capacity.

Maintenance also inspects at most 100 persisted concurrency groups per poll,
using resumable cursors. It removes counters only when no active hold or
nonterminal work needs the key, with a one-minute idle grace to avoid deleting
hot keys between consecutive jobs. PostgreSQL measures from `last_modified`;
Redis starts the grace when cleanup first observes the idle key and resets it
on a new claim. PostgreSQL locks and rechecks the candidate;
Redis checks and removes it in one Lua call. New work can safely recreate the
bookkeeping. This bounds accumulation from one-use business-operation keys once
their workflows finish. The metered store exports these deletions with
`threadmill.retention.deleted{kind="concurrency"}`. The in-memory store derives
admission from its stored jobs and has no separate counter rows to reclaim.

PostgreSQL also inspects at most 100 queue-counter groups per poll through
`deleteIdleQueueMetadata`. It deletes only locked, zero-sum shard rows of empty
queues; negative individual shards and concurrent producers remain valid.
`threadmill.retention.deleted{kind="queue_metadata"}` counts removed rows.
The in-memory and Redis stores need no corresponding empty-queue counter cleanup.

Execution resources are released through `JobInterceptor.onProcessingFinished`,
which runs in an engine `finally` block in reverse interceptor order. It also
runs for stale or rejected completion writes, fatal errors, shutdown release and
orphan recovery. Cleanup does not certify that the job reached a persisted
terminal state. Use the context instance to identify an execution: job ID and
attempt number alone can collide with concurrent recovery or a refunded retry.

Metrics expose `threadmill.executions.active` and
`threadmill.executions.unconfirmed`. Unconfirmed exits include attempts whose
completion lost an optimistic-lock race; they do not imply that durable work was
lost. OpenTelemetry spans include `threadmill.execution.completion_confirmed`;
when false, the local attempt ended without a confirmed outcome notification.
An orphan-recovery span never borrows or closes the original execution's scope.

For scrape endpoints with strict latency budgets, configure the optional
[asynchronous metrics refresh executor](../threadmill-metrics/README.md#wiring).
A store outage then leaves cached values and increasing snapshot age visible
without blocking the scrape. Explicit refresh calls remain synchronous.
