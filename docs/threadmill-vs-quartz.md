# Threadmill vs Quartz

A side-by-side for teams evaluating Threadmill against the Quartz Scheduler
they already run, or migrating from one to the other. The goal is to make the
design differences concrete so you can decide which one fits your problem,
not to argue one is universally better.

## At a glance

| Topic | Quartz | Threadmill |
|---|---|---|
| Cluster model | Single-node by default; opt-in clustering via shared `QRTZ_LOCKS` table | Multi-node from day one; every node polls the store directly |
| Concurrency control | Pessimistic SQL locks on the locks table | Optimistic version field on every job + `SELECT … FOR UPDATE SKIP LOCKED` (Postgres) / Lua scripts (Redis) |
| Delivery guarantee | Effectively at-most-once for misfire-discarded triggers, at-least-once otherwise | At-least-once, always. Handlers must be idempotent |
| API style | Imperative — build `JobDetail` + `Trigger`, call `scheduler.scheduleJob(...)` | Declarative — `@Job` / `@Recurring` Spring beans discovered at context refresh |
| Job payload | Untyped `JobDataMap` (`Map<String, Object>` serialized to JDBC blob) | Typed `JobPayload` record + `JobHandler<P>`; serialized as JSON with a class-name tag |
| Missed firings | Per-trigger misfire instructions, resolved on scheduler start | Per-task `MissedRunPolicy` (`DROP` or `CATCH_UP`) set at registration time |
| Runtime pause | `scheduler.standby()` pauses dispatch on the local node | `pauseQueue(queue)` pauses claims on every node, cluster-wide |
| Startup gate | `scheduler.standby()` then `scheduler.start()` after registration | Maximum/default `SmartLifecycle` phase starts the node as late as possible; remote wake is ordered inside the same lifecycle |
| Storage backends | JDBC (`QRTZ_*` tables across many vendors) or RAM | PostgreSQL 18+, Redis (standalone / Sentinel / Cluster), in-memory |
| Threading | Configurable thread pool of platform threads | Virtual threads (Java 25) per worker; lanes give per-queue capacity |
| Result of a run | Write-back through `JobDataMap` | Typed `JobResult` slot persisted on `SUCCEEDED` |
| Transactional enqueue | `JobStoreTX` owns a transaction; `JobStoreCMT` joins a managed transaction | `after_commit` by default; Spring + Postgres can use `join_transaction` to commit jobs with the caller transaction |
| Per-key concurrency | Manual via `@DisallowConcurrentExecution` on the `JobDetail` class | First-class `EXCLUSIVE` / `SHARED` modes per `concurrencyKey`, inherited through workflows |
| Workflows / chaining | Application code | Built-in `WorkflowInterceptor` — `AWAITING` children promote when parent succeeds; concurrency key inherited |
| Long-running jobs | Hard `JobRunShell` timeout, no progress channel | `ctx.checkIn()` + `noProgressTimeout` — wall-clock budget switches to "no progress in N" once the handler reports liveness |
| Bulk enqueue | Loop over `scheduler.scheduleJob` | `insertAll(List<Job>)` is atomic — whole batch rejected on any oversize / duplicate |
| Producer dedup | Application code | `enqueueIfAbsent(job, dedupKey, ttl)` coalesces concurrent producers at the store level |
| Listeners / interceptors | `JobListener`, `TriggerListener`, `SchedulerListener` | `JobInterceptor` SPI with `RetryInterceptor` and `WorkflowInterceptor` shipped |
| Observability | JMX, log lines | `EngineSnapshot` data API + Micrometer integration + reusable static dashboard UI |

## Architecture

### Multi-node from the ground up

Quartz starts as a single-node scheduler. Clustering is an opt-in mode where
multiple JVMs share a JDBC datastore and serialise dispatch through a
pessimistically-locked `QRTZ_LOCKS` table. Bringing a node up involves
acquiring those locks, scanning trigger state, and coordinating with the
existing cluster — standby is the safe window for that coordination.

Threadmill assumes from the start that more than one `ProcessingNode` is
running against the same store. `NodeRegistry.start()` just spawns a
heartbeat loop; the store is the single source of truth. The maintenance
lease (`MaintenanceCycle` runs only on the lease holder) is a continuous
runtime competition any node can win at any tick (see
`NodeRegistry.electedMaster()`). There is no join handshake to wait through,
so there is no need for a separate "standby" mode.

### Optimistic vs pessimistic concurrency

Quartz serialises mutations through SQL locks on `QRTZ_LOCKS`. Two nodes
trying to claim the same trigger queue at the same time block.

Threadmill claims atomically without blocking other nodes — Postgres uses
`SELECT … FOR UPDATE SKIP LOCKED` to skip rows another worker already
holds; Redis uses a single Lua script that verifies version + queue
membership before committing. Every job has a `version` field; updates are
conditional on the expected version. The hot bug case ("two workers run
the same job") is impossible by construction, not by table-wide locking.

## API and payload model

### Declarative bean discovery vs imperative registration

Quartz expects you to construct `JobDetail` and `Trigger` objects and call
`scheduler.scheduleJob(detail, trigger)`. Standby exists so this can happen
*after* the scheduler is alive but *before* dispatch starts.

Threadmill discovers handlers as Spring beans:

```java
@Component
@Job(queue = "email", timeout = "PT2M", maxRetries = 5)
final class SendEmailHandler implements JobHandler<SendEmail> {
    @Override public void run(SendEmail payload, JobExecutionContext ctx) { … }
}
```

Recurring tasks are similarly annotation-driven (`@Recurring`). The
`SmartLifecycle` phase fires `node.start()` only after every bean is wired,
so the construct-register-dispatch sequence Quartz needs standby for is
already a hard guarantee of the lifecycle phase.

### Typed payload vs untyped `JobDataMap`

Quartz hands the handler a `JobExecutionContext` whose `JobDataMap` is a
`Map<String, Object>`; the handler casts each key to its expected type.
Threadmill takes a typed `JobPayload` record and the handler is
`JobHandler<P>` — the engine deserialises into `P` before invoking the
handler, so type errors surface at enqueue time (via
`JobScheduler`'s pre-validation), not at runtime.

## Scheduling

### Misfires vs `MissedRunPolicy`

Quartz tracks "misfired" triggers — wall-clock firings the scheduler
missed because it was down, paused, or saturated. Each trigger declares a
misfire instruction (`MISFIRE_INSTRUCTION_DO_NOTHING`,
`MISFIRE_INSTRUCTION_FIRE_NOW`, `MISFIRE_INSTRUCTION_RESCHEDULE_NEXT_*`,
etc.) consulted when the scheduler comes back. Standby is the safe window
to configure these before the catch-up wave runs.

Threadmill has no misfires by design. Every `CronTask` carries a
`MissedRunPolicy` set when the task is registered:

- `DROP` (default) — materialise only the latest fire on the next tick,
  drop everything that should have fired while the engine was down.
- `CATCH_UP` — materialise every missed fire.

The policy is per-task, not scheduler-wide, and the choice is made at
registration time. Tests `SchedulingTest.dropPolicyDoesNotCauseACatchUpStorm`
and `catchUpPolicyMaterializesEveryMissedFire` pin both behaviours.

### No standby mode

Quartz needs `scheduler.standby()` for two reasons covered above
(imperative registration and misfire control) plus one runtime use case:
pausing dispatch on a live node without shutting it down. Threadmill's
per-queue pause is the right primitive for that — it lives in the store,
so it is cluster-wide, and pending jobs stay `ENQUEUED` for resume. To
drain the whole cluster, list and pause every active queue:

```java
String reason = "manual maintenance window";
for (String queue : store.listEnqueuedQueues()) {
    scheduler.pauseQueue(queue, reason);
}
// … later …
for (String queue : store.listPausedQueues()) {
    scheduler.resumeQueue(queue);
}
```

To drain a single node before shutdown, `SmartLifecycle.stop()` already
waits up to `threadmill.shutdownGracePeriod` for in-flight jobs.

The one ergonomic gap is a single-call "pause everything everywhere"
helper. If that becomes a frequent operator request, a
`Scheduler.pauseAll()` / `resumeAll()` pair is a small additive
follow-up — a convenience over the existing primitive, not a missing
feature.

## Things Threadmill has that Quartz doesn't

- **Per-key concurrency groups.** A `concurrencyKey` plus `EXCLUSIVE` /
  `SHARED` mode is enforced at claim time. Workflow successors inherit
  the root job's key and hold it until the last descendant terminates.
  See [`docs/concurrency.md`](concurrency.md).
- **Workflows.** `JobRelationship.parentId` plus `WorkflowInterceptor`
  promotes `AWAITING` children when the parent succeeds. Failures
  abandon still-waiting descendants instead of leaking concurrency
  holds.
- **Producer-side deduplication.** `enqueueIfAbsent(job, key, ttl)`
  coalesces concurrent producers at the store level.
- **Bulk enqueue.** `insertAll(List<Job>)` is atomic — whole batch
  rejected on any oversize or duplicate, no version-desync risk.
- **Long-running check-ins.** `ctx.checkIn()` switches the wall-clock
  `jobTimeout` to `noProgressTimeout` for the rest of the run, so a
  long but live job is not killed for being slow.
- **Transaction-aware enqueue.** The default Spring `JobScheduler`
  fires `store.insert` on `afterCommit`; a rollback discards the
  enqueue. No outbox table required for the common case.
- **Queue families with weights.** A single lane can serve a pattern
  like `tenant:*` with per-queue weights resolved on a discovery
  cadence.

## Things Quartz has that Threadmill doesn't (yet)

- **Calendar exclusions** (skip weekends, holidays, business-hour
  windows). Threadmill expects the cron expression to encode this.
- **Trigger listeners with veto.** Quartz's `TriggerListener` can veto
  an about-to-fire trigger. Threadmill's `JobInterceptor` runs around
  execution, not around dispatch decisions.
- **A long-tail of misfire instructions.** Threadmill collapses them
  into two policies. If you need `MISFIRE_INSTRUCTION_RESCHEDULE_NEXT_WITH_REMAINING_COUNT`
  semantics, you'll re-encode it in the cron expression itself.

## Migrating

The general shape is: map each `JobDetail` / `Job` class to a
`JobHandler<P>`, lift the `JobDataMap` keys into a typed `JobPayload`
record, and re-register triggers as `@Recurring` annotations or
`Scheduler.defineCronTask` / `defineIntervalTask` calls.

See [`docs/migration.md`](migration.md) for the step-by-step.
