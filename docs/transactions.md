# Transactions

The questions every job-library user eventually asks. Spelt out for each
backend (Postgres, Redis, in-memory) and for the Spring Boot adapter.

> **The headline contract: at-least-once delivery.** A handler may run more
> than once for the same logical job — after a node crash, after orphan
> reclaim, after a retry. **Idempotency is the user's responsibility.**

## What atomic boundary wraps each `JobStore` operation?

| Backend | Boundary |
|---|---|
| Postgres | A JDBC transaction. Every state-changing op begins with `setAutoCommit(false)` and commits at the end. `claimReady` opens one transaction that runs the `SELECT … FOR UPDATE SKIP LOCKED` and the per-row `UPDATE` and commits before returning. Crash mid-transaction → nothing visible. |
| Redis | A single Lua script. Each script runs to completion atomically on the Redis server (single-threaded execution model). Crash mid-script is impossible from the client's perspective — Redis either commits the script or doesn't run it at all. |
| In-memory | A single monitor (`synchronized (claimMutex)`). Conceptually the same as a transaction: nothing observes a half-applied state change. |

Nothing in any backend can be observed half-applied. Either the entire op
committed, or none of it did.

## Does `JobHandler.run(...)` execute inside a Threadmill-managed transaction?

**No.** Loudly. The handler runs **between** the claim transaction and the
save transaction:

```
[claim txn: SELECT FOR UPDATE → UPDATE state=PROCESSING → COMMIT]
   ↓
[handler.run(...)  ← no Threadmill transaction here]
   ↓
[save txn: UPDATE state=SUCCEEDED (or FAILED) version=v+1 → COMMIT]
```

Threadmill does not enrol the handler in any transaction it owns. The handler
is on its own for the transactional behaviour of its side effects.

This is deliberate. A handler that takes seconds or minutes (long-running
import / export job) must not hold a row lock that long; that would make the
queue itself unscalable.

## What about the claim?

One atomic operation per backend:

- **Postgres:** `SELECT … FOR UPDATE SKIP LOCKED` + version-matched `UPDATE`
  in one JDBC transaction.
- **Redis:** `claim_commit.lua` — verifies version / state / queue membership
  and commits the new body + every index update + counts together.
- **In-memory:** under `claimMutex`.

Once the claim transaction commits, Threadmill releases it and only re-opens
a transaction to save the terminal state. The handler runs on its own.

For the detailed Postgres and Redis execution paths, including how the
claim-time concurrency checks interact with row locks and Lua scripts, see
[Backend execution model](backend-execution-model.md).

## What if my handler does its own `@Transactional` (Spring) work?

That's the **handler's** transaction, managed by Spring against the
application's `DataSource`. It is **independent of any Threadmill
transaction**. Threadmill borrows connections from the same `DataSource`
pool but does not share a transaction.

Two consequences worth calling out:

1. **A handler-thrown exception rolls back the handler's `@Transactional`
   work, not Threadmill's state transition.** The handler's database writes
   are gone; Threadmill still records the failure cleanly via the single
   failure code path (`JobRunner.recordFailure`).
2. **Threadmill's state transition cannot be rolled back by the handler.**
   Once the save transaction commits the `SUCCEEDED` (or `FAILED`) state,
   it stays. There's no two-phase commit between the handler's database
   and Threadmill's database.

## How does enqueueing inside a `@Transactional` method behave?

Spring exposes three enqueue modes through `threadmill.spring.enqueue-mode`.

### `after_commit` (default)

- **`JobId` is reserved synchronously** at the call (UUIDv7, generated
  client-side).
- **The actual `store.insert(...)` is deferred** until the enclosing
  transaction commits — registered via `TransactionSynchronizationManager`.
- If the transaction rolls back, the job is **not enqueued**.

```java
@Transactional
public void sendWelcome(User u) {
    repo.save(u);                                  // pending
    JobId id = enqueuer.enqueue(new SendEmail(...));  // pending
    // store.findById(id) returns Optional.empty() here.
}
// On commit: row saved, then job inserted, then the txn returns.
// On rollback: neither happens.
```

`findById(id)` returns empty until the transaction commits — the id is
reserved, but the row doesn't exist yet. This mode avoids jobs that point to
rolled-back application rows, but it has one remaining failure window: the
business transaction can commit and the after-commit job insert can still fail.

Each deferred enqueue is isolated: a store failure is logged and the remaining
callbacks still run. The auto-configured scheduler also publishes
`AfterCommitEnqueueFailure` with the reserved ids and cause. Persistence is
**unconfirmed**, because a lost acknowledgement can follow a successful write.
Inspect those ids before recovering; Threadmill does not automatically repeat
the insert. Listener failures are contained. The scheduler's
`deferredEnqueueFailureCount()` counts affected ids and can be exported through
a Micrometer `FunctionCounter`.

```java
@EventListener
void onDeferredEnqueueFailure(AfterCommitEnqueueFailure failure) {
    enqueueAlerts.recordUnconfirmed(failure.jobIds(), failure.cause());
}
```

The event is an in-process observation, not durable recovery: a process crash
can prevent its delivery. Use `join_transaction` with the same PostgreSQL
`DataSource` for atomic business and job writes, or an application-owned durable
outbox when crossing datastores. Handlers still require idempotency under
Threadmill's at-least-once delivery guarantee.

Deferred job bodies are validated synchronously. Per scheduler and transaction,
submissions are limited to 1,000 jobs and 8 MiB of combined encoded bodies,
including separate enqueue calls. A rejected submission leaves earlier accepted
callbacks intact; the caller can roll back or use smaller transactions. Callbacks
retain enqueue order and each bulk call remains one atomic store operation.

**`enqueueIfAbsent(...)` is the exception: it is always immediate in this
mode.** Its synchronous `EnqueueResult` (Created vs Coalesced) cannot be
deferred to `afterCommit` without changing the API, so the dedup record and
the job row are written when the method is called — and they **survive a
rollback** of the surrounding business transaction. A DEBUG log line is
emitted when `enqueueIfAbsent` runs inside an active transaction. If the
deduplicated enqueue must roll back with the caller, use
`join_transaction` (Postgres) — there the dedup write shares the caller's
JDBC transaction — or restructure to a plain `enqueue()` after commit.

### `join_transaction`

Postgres + Spring can make scheduling part of the caller's SQL transaction:

```yaml
threadmill:
  spring:
    enqueue-mode: join_transaction
```

In this mode normal enqueue, scheduled enqueue, bulk enqueue, and
`enqueueIfAbsent(...)` write through the same Spring-bound JDBC connection as
the application transaction. Commit makes both the application rows and the
Threadmill job visible; rollback removes both. Local worker wakeups still run
only after commit so workers never race an uncommitted row.

This mode is intentionally limited to the Spring auto-configured
`PostgresJobStore` using the same `DataSource` as the caller transaction.
Redis cannot join a SQL transaction; unsupported combinations fail fast at
startup.

One deduplication edge is intentionally different in this mode. If two
transactions race on the same `(queue, dedupKey)`, the loser receives the
Postgres unique-constraint failure instead of Threadmill coalescing it into an
existing job id. Once Postgres aborts a statement inside the caller's
transaction, Threadmill cannot safely run the fallback lookup without also
owning the transaction boundary.

### `immediate`

```yaml
threadmill:
  spring:
    enqueue-mode: immediate
```

Immediate mode writes jobs as soon as `enqueue()` is called, regardless of
transaction state. Use it only when jobs may safely run against application
state that is not committed yet. If the application transaction rolls back,
the job can reference rows that never existed.

## What about jobs enqueued from a non-transactional context?

Immediate insert. `after_commit` and `join_transaction` both fall through to a
normal store-owned transaction when no Spring transaction is active.

## Recurring definitions stay immediate by design

`enqueueRecurring(...)` writes cron-task definitions immediately. Cron-task
definitions are configuration, not work. Registering them on rollback would be
surprising.

## Nudging a recurring task (wake-driven pollers)

`nudgeRecurring(taskName)` requests that a registered recurring task
materialize an instance as soon as possible. It exists for the outbox-pump
shape: instead of running a poller task every few seconds just to bound the
latency between "work row written" and "work row processed", producers nudge
the task when there is work and the recurring schedule becomes a slow
self-healing backstop (every few minutes). Job-row churn becomes proportional
to actual work instead of wall-clock time.

> This section covers the **transactional** contract. For the pattern itself —
> handler shape, how to choose the backstop interval, and what coalescing
> means for your code — see [Wake-driven pollers](wake-driven-pollers.md).

The guarantees, in producer terms:

- **Run-after-wake.** After every accepted nudge, at least one instance
  *starts* after it. A nudge that arrives while an instance is already running
  produces one follow-up run after it completes — the in-flight run may have
  read the work table before your write committed, so it never counts as
  satisfying your nudge.
- **Coalescing.** A burst of nudges collapses to at most the current run plus
  one follow-up. Nudge freely; there is no 1:1 nudge-to-run mapping to worry
  about. (This bound is failure-free: consistent with at-least-once, a crash
  in the narrow window between the follow-up's insert and the request's
  clear can produce an extra run — failures only ever add runs, never lose
  one.)
- **Durability over signaling.** The nudge is a durable store write consumed
  by the maintenance master's recurring tick — there is no transient signal
  that can be dropped. Worst-case materialization latency is one
  `maintenancePollInterval` (default 1 s).
- **Schedule non-interference.** A nudged run never moves the schedule: a cron
  task's next fire stays the regular wall-clock match, and an interval
  trigger's phase is preserved (an every-6h task that last fired at 06:00 and
  is nudged at 07:00 still fires next at 12:00).

Nudging an unknown task throws `IllegalArgumentException`; nudging a disabled
task throws `IllegalStateException` — an explicit pause wins. Disabling or
re-enabling a task clears any pending nudge (consistent with
re-enable-does-not-catch-up).

Under Spring, address the task by its handler class rather than its name:

```java
@Job(queue = "system")
@Recurring(interval = "PT10M")                 // the self-healing backstop
class OutboxPump implements JobAction {
    public void run(JobExecutionContext ctx) { /* drain the work table */ }
}

@Service
class OrderService {
    private final JobScheduler jobs;

    @Transactional
    public void placeOrder(Order order) {
        outboxRepo.save(row(order));
        jobs.nudgeRecurring(OutboxPump.class);  // refactor-safe
    }
}
```

A `@Recurring` task's durable identity defaults to the handler's
fully-qualified class name, so the string overload would make callers
hard-code `"com.acme.jobs.OutboxPump"` and break on a rename or package move.
The class overload resolves the registered name through the handler registry.
Use the string form for tasks registered imperatively through the core
`Scheduler`, where the name is chosen by the caller and is the identity.

**Nudges are after-commit in every enqueue mode**, including
`join_transaction`. Validation (unknown or disabled task) fails fast on the
calling thread, the write itself fires in `afterCommit`, and a rollback
discards it — so you can nudge in the same `@Transactional` method that
writes the work row, in any mode, with the same semantics.

This is the one write that deliberately does *not* join the caller's
transaction, and the reason is scaling. Coalescing is by design one store
cell per task, so a joined nudge would hold that row's write lock for the
whole business transaction: every concurrent producer of that task would
serialize behind it, capping throughput at one transaction at a time per task
and creating lock-ordering deadlocks that did not exist before. It would fail
silently — correct at low rate, collapsing under load. What joining buys is
closing the crash window between the caller's commit and the nudge write, and
that window is an explicit non-goal: the backstop schedule bounds the
worst-case latency, and a lost nudge costs one schedule period, never a run.

On the hot path the write is a short Threadmill-owned transaction that commits
independently of the pool's `autoCommit` default. An in-JVM per-task coalescer
additionally bounds the store write rate to about one round trip regardless of
producer rate.

## Connection-pool sharing

Spring users typically have one shared `DataSource` / HikariCP. Threadmill
borrows from this pool for its own operations; the handler borrows from the
same pool for `@Transactional` work. Pool sizing must account for both.
Recommended floor:

```
workerCount + claimBatchSize + headroom-for-handler-work
```

If the pool is too small, the dispatcher waits on a connection for the claim
transaction while a handler is holding one for its own work, and throughput
collapses.

## What guarantee does Threadmill provide for side-effect idempotency?

**None directly.** At-least-once delivery is the contract — the handler may
run more than once for the same logical job, and Threadmill does not roll
back the handler's side effects when the job is retried or orphan-reclaimed.

> Idempotency is the user's responsibility.

State this loudly to yourself before writing a handler. Common patterns:

- An external HTTP call that's idempotent on the receiver (PUT with a
  client-generated key, GraphQL mutation with `idempotencyKey`, …).
- A database write that uses `INSERT … ON CONFLICT DO NOTHING` or `MERGE` to
  short-circuit on the second attempt.
- An "outbox" check — see the next question.

## When exactly can two instances of the same job overlap?

Three windows, and it is worth knowing which of them you can close.

**Shutdown is not one of them.** When a node stops, it drains: the worker pool
is closed to new work and in-flight handlers are given
`threadmill.shutdown-grace-period` to finish before anything is interrupted.
An interrupted job's requeue is written by that job's *own* worker thread,
after `handler.run` has already returned or thrown — so a surviving node
cannot claim the job while your code is still executing on the old one. The
node also keeps its owner heartbeats fresh through the whole drain, so peers
do not mass-reclaim jobs that are merely draining.

**Retry handoff is closed for recurring tasks.** A failure and its reschedule
are two separate store writes, and a recurring task's pile-up guard used to
treat the intermediate `FAILED` as "finished", so a materializer tick landing
between the two writes could create a fresh instance beside a retrying one.
The guard now holds while a failed instance is still plausibly awaiting its
retry, and an [exclusive recurring task](concurrency.md#exclusive-recurring-tasks)
closes it outright, because the fresh instance cannot be admitted while the
retrying one holds the key.

**Lease-expiry reclaim cannot be closed — by Threadmill or by anything else.**
A node that stops heartbeating is indistinguishable from one that is paused
and about to resume: a long GC pause, a suspended VM, a partitioned network,
a `SIGSTOP`. After `threadmill.heartbeat-timeout` the cluster must decide
whether to give up the job or strand it forever, and it chooses to reclaim.
If the original node was alive after all, its handler is still running when
the replacement starts. Reclaim also releases the claim-time concurrency slot
as part of the terminal failure save, so an `EXCLUSIVE` key does **not** cover
this window.

> Threadmill narrows the overlap surface. It does not remove the need for
> idempotent handlers.

**Fence at your own data.** For an effect that must not happen twice, the last
line of defence belongs in the datastore you control, not in the scheduler:

- A compare-and-set transition — `UPDATE … SET state = 'sent' WHERE id = ?
  AND state = 'pending'` — and do the work only if one row changed.
- A unique constraint on the natural key of the effect (one payment per
  invoice per period), so the second writer fails loudly rather than
  duplicating.
- A claim column stamped with the job id plus a timestamp, so a second runner
  can see it is not the current owner.

These hold regardless of what the scheduler believes, which is exactly the
property you want when the scheduler's belief is the thing that was wrong.

## Can I get exactly-once-successful side effects?

Threadmill provides **at-least-once** execution after a successful durable enqueue,
subject to the datastore's persistence and replication configuration. Exactly-once
external effects require cooperation from the destination. These two patterns
make the boundaries explicit:

### 1. Transactional outbox

Write business changes and an outgoing intent in one application database
transaction. A unique constraint on the intent's idempotency key makes repeated
and concurrent handler attempts produce one intent. `insertIfAbsent` below is an
application repository operation implemented with an atomic insert, such as
`INSERT ... ON CONFLICT DO NOTHING`; it is not an exists-then-insert race.

```java
@Transactional
public void run(SendEmail payload, JobExecutionContext ctx) {
    outboxRepo.insertIfAbsent(ctx.jobId().toString(), payload.to(), payload.body());
}
```

A separate publisher claims pending intents, sends them, and marks delivery.
The publisher must itself tolerate retries. A crash after a remote send but before
the delivery marker can send twice: the SQL transaction cannot roll back a remote
email or HTTP request. Forward the intent's stable key to a destination that
actually supports idempotency, and honor that destination's key scope and retention
window. Without that support, duplicate external delivery remains possible.

Do not put a remote send before an outbox marker inside `@Transactional` and
describe it as atomic. The outbox guarantees durable intent and local deduplication;
downstream cooperation determines the external-effect guarantee.

### 2. Idempotency-key handshake with the downstream

The handler forwards `ctx.jobId()` to the receiver; the receiver dedups on
its side. Use this only when the destination documents idempotency semantics for
the specific operation; a header alone does not provide deduplication.

```java
public void run(ChargeCustomer payload, JobExecutionContext ctx) {
    stripe.charges().create(
        ChargeCreateParams.builder()
            .setAmount(payload.cents())
            .setCustomer(payload.customerId())
            .build(),
        RequestOptions.builder()
            .setIdempotencyKey(ctx.jobId().toString())
            .build());
}
```

For Postgres-collocated handlers, a future Threadmill option could hand the
handler a `Connection` from a Threadmill-managed transaction so the
handler's writes commit with the `PROCESSING → SUCCEEDED` transition. That's
not in v1; today the user implements the outbox pattern by hand.

## Retry transactions

Each retry is a **fresh transaction**: claim → run handler → save. There is
no "retried-in-the-same-transaction" — every attempt is independent. The
handler must be idempotent across attempts.

## Workflow chains

Each child job in a workflow is an **independent claim/save pair**. Parent
and child do not share a transaction. The concurrency lock is held across
the whole chain (Phase 12: `workflow_root_id` inheritance) but transactions
are not.

## What if Threadmill's save transaction itself fails?

| Failure | What happens |
|---|---|
| `StaleJobException` (someone else moved the job) | The in-memory job is exactly reusable — version unchanged. The engine logs and skips. |
| `OversizedJobException` (new body too big) | Caught by the Phase 3 snapshot-time truncation policy: `JobLog` is trimmed from the head, FAILED state-history messages are capped with a sentinel, and the save retries. If even the truncated body is over the cap (metadata explosion), the exception bubbles. |
| Postgres deadlock (SQLSTATE 40P01 / 40001) | `DeadlockRetry.run(...)` retries with exponential backoff + jitter. |
| Any other `SQLException` / Redis error | Bubbles up. The dispatcher's circuit breaker counts it; on the threshold the loop pauses and probes the store for recovery. |

None of these mutate in-memory state in a way that requires the user to
clean up.

## Redis durability vs. Postgres durability

- **Postgres:** claim/save transactions commit to disk per the host's
  `synchronous_commit` / `fsync` settings.
- **Redis:** durability is whatever Redis is configured for — `appendonly yes`
  with `appendfsync everysec` is the documented baseline. A crash within 1 s
  of a state change may lose the state change.

Crash-mid-claim semantics are *correct* on both backends (the orphan-recovery
path runs from `findOrphaned`); the durability question is about whether the
engine remembers what it did.

## Spring Boot integration specifics

### How does Threadmill hook into Spring's transaction synchronization?

Via `TransactionSynchronizationManager`. In `after_commit` mode,
`TransactionAwareJobScheduler` registers a synchronization whose
`afterCommit()` runs the actual `store.insert(...)`. In `join_transaction`
mode, `TransactionJoinedJobScheduler` writes immediately through a
Spring-bound PostgreSQL connection and registers only the local wake signal for
`afterCommit()`.

### `SmartLifecycle` phase

`ThreadmillLifecycle` runs at Spring's maximum/default phase
`Integer.MAX_VALUE`. Spring starts lower phases first and stops higher phases
first, so Threadmill starts as late and stops as early as the lifecycle protocol
allows. Store connections are constructed before lifecycle startup. Remote-wake
subscriptions share the lifecycle: they subscribe after the node starts and
close before the node drains.

### What happens if a `@Transactional` method throws after enqueue?

The transaction rolls back; the deferred enqueue is discarded. The `JobId`
returned at the call site is now garbage — the row was never inserted.
Don't persist the `JobId` for later lookup until the transaction commits.

### Immediate-mode opt-out

```yaml
threadmill:
  spring:
    enqueue-mode: immediate
```

Restores the pre-after-commit behaviour: jobs visible to workers as soon as
`enqueue()` returns, regardless of transaction state. Use only when you
understand the trade-off (jobs may run against application state the
transaction hasn't yet committed; if the transaction rolls back, workers
see a job that references a non-existent parent row).

### Spring `@Async`

Orthogonal. Threadmill jobs don't need `@Async` to run off-thread — they're
already off-thread on virtual workers. Mixing them is fine but rarely
useful: an `@Async` wrapper around an `enqueuer.enqueue(...)` call just adds
hops without value.

## Worked example

This PostgreSQL example uses `threadmill.spring.enqueue-mode=join_transaction`
and the same application `DataSource` for business writes and Threadmill. The
user row and job insert commit together. The default `after_commit` mode has a
separate post-commit enqueue failure window and does not provide this atomicity.

```java
@Component
@Job(queue = "email", timeout = "PT30S", maxAttempts = 5)
class SendEmailHandler implements JobHandler<SendEmail> {
    private final OutboxRepository outbox;

    SendEmailHandler(OutboxRepository outbox) { this.outbox = outbox; }

    @Override
    @Transactional
    public void run(SendEmail payload, JobExecutionContext ctx) {
        // Atomic insert with a unique key; no remote send in this transaction.
        outbox.insertIfAbsent(ctx.jobId().toString(), payload.to(), payload.body());
    }
}

@Service
class WelcomeService {
    private final UserRepo users;
    private final JobScheduler jobs;

    WelcomeService(UserRepo users, JobScheduler jobs) { this.users = users; this.jobs = jobs; }

    @Transactional
    public JobId welcome(NewUser command) {
        var user = users.save(command.toUser());
        return jobs.enqueue(SendEmailHandler.class, new SendEmail(user.email(), template()));
    }
}
```

`OutboxRepository`, its unique-key schema, and the publisher belong to the
application. The publisher reads pending intents, sends them with the same
stable idempotency key where supported, and records delivery. A Threadmill
recurring [outbox pump](wake-driven-pollers.md) can drive that publisher.

| Failure point | Durable result and recovery |
|---|---|
| Business transaction succeeds | User and job commit together; the handler later commits one outgoing intent. |
| Business transaction rolls back | Neither user nor job is committed with `join_transaction`. |
| Handler dies before its outbox transaction commits | No outgoing intent commits; retry can insert it. No remote effect has happened in this handler. |
| Handler commits the intent, then dies before SUCCEEDED | Retry's atomic insert encounters the same unique key and does not create another intent. |
| Publisher sends, then dies before recording delivery | The intent is retried. A destination with the agreed idempotency contract deduplicates; otherwise duplicate delivery is possible. |
| Two attempts or publishers overlap | The database unique key prevents duplicate local intents. External duplicate prevention still depends on publisher claiming and the destination's idempotency contract. |

The application must retain the idempotency record for its required replay
window. Threadmill job retention and producer deduplication do not replace it.

## See also

- `threadmill-spring-boot/README.md` — writing Spring handlers with `@Job`,
  idempotency, and the auto-config story.
- `docs/concurrency.md` — when to use `EXCLUSIVE` / `SHARED` per-key
  concurrency.
- `threadmill-spring-boot/README.md` — the auto-config story, the
  `SmartLifecycle` phase choice.
- `threadmill-store-postgres/README.md` — Postgres-specific transaction
  semantics, deadlock retry.
- `threadmill-store-redis/README.md` — Lua script atomicity, AOF durability.
