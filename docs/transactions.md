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

With the Spring adapter's default (`threadmill.spring.enqueue-after-commit=true`):

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
reserved, but the row doesn't exist yet. Code that depends on
`findById(id)` succeeding immediately after `enqueue()` returns should set
`threadmill.spring.enqueue-after-commit=false` and accept the trade-off
(jobs visible to workers before the transaction commits; if the
transaction rolls back, the workers see a job that references a non-existent
parent row).

## What about jobs enqueued from a non-transactional context?

Immediate insert. The `TransactionAwareJobEnqueuer` checks
`TransactionSynchronizationManager.isSynchronizationActive()` and, when no
synchronisation is active, falls through to the underlying `Scheduler.enqueue(...)`
path — identical to the pre-after-commit behaviour.

## Two paths that stay immediate by design

- **`enqueueIfAbsent(...)`** — dedup must return a meaningful
  `EnqueueResult.Created` / `EnqueueResult.Coalesced` synchronously. We can't
  defer the actual write without changing the API.
- **`enqueueRecurring(...)`** — cron-task definitions are configuration, not
  work. Registering them on rollback would be surprising.

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

## Can I get exactly-once-successful side effects?

Not from Threadmill alone. No library can without two-phase commit. The two
patterns that work in practice:

### 1. Transactional outbox

The handler writes to its own database with an idempotency record keyed by
`JobId`. A re-run sees the record and short-circuits.

```java
@Transactional
public void run(SendEmail payload, JobExecutionContext ctx) {
    String key = ctx.jobId().toString();
    if (outboxRepo.existsById(key)) return;       // already done
    emailService.send(payload.to(), payload.body());
    outboxRepo.save(new OutboxEntry(key, Instant.now()));
}
```

The `existsById` check + the `save` happen in the same transaction. If
either the `existsById` or the `save` fails, the `send` doesn't matter — on
retry the existsById sees the row (if it was committed) or doesn't (if not),
and the handler does the right thing either way.

### 2. Idempotency-key handshake with the downstream

The handler forwards `ctx.jobId()` to the receiver; the receiver dedups on
its side. Most modern HTTP APIs accept an `Idempotency-Key` header for this.

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

Via `TransactionSynchronizationManager`. `TransactionAwareJobEnqueuer` checks
`isSynchronizationActive()`; when true, it registers a synchronization whose
`afterCommit()` runs the actual `store.insert(...)`.

### `SmartLifecycle` phase

`ThreadmillLifecycle` runs at phase `Integer.MAX_VALUE / 2`. Spring stops
higher phases first, so the engine starts after infrastructure beans
(DataSource, RedisConnectionFactory at the default `Integer.MAX_VALUE`)
and stops before them on graceful shutdown.

### What happens if a `@Transactional` method throws after enqueue?

The transaction rolls back; the deferred enqueue is discarded. The `JobId`
returned at the call site is now garbage — the row was never inserted.
Don't persist the `JobId` for later lookup until the transaction commits.

### Immediate-mode opt-out

```yaml
threadmill:
  spring:
    enqueue-after-commit: false
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

```java
@SpringBootApplication
class WelcomeApp { … }

@Component
@ThreadmillJob(queue = "email", timeout = "PT30S", maxRetries = 5)
class SendEmailHandler implements JobHandler<SendEmail> {
    private final EmailGateway gateway;
    private final OutboxRepository outbox;

    SendEmailHandler(EmailGateway g, OutboxRepository o) { this.gateway = g; this.outbox = o; }

    @Override
    @Transactional
    public void run(SendEmail payload, JobExecutionContext ctx) {
        String key = ctx.jobId().toString();
        if (outbox.existsById(key)) return;                       // (a) idempotency
        gateway.send(payload.to(), payload.body());               // (b) side effect
        outbox.save(new OutboxEntry(key, Instant.now()));         // (c) outbox commit
    }
}

@Service
class WelcomeService {
    private final UserRepo users;
    private final JobEnqueuer jobs;

    WelcomeService(UserRepo u, JobEnqueuer j) { this.users = u; this.jobs = j; }

    @Transactional
    public JobId welcome(NewUser cmd) {
        var user = users.save(cmd.toUser());                       // (d) pending write
        return jobs.enqueue(new SendEmail(user.email(), template())); // (e) pending enqueue
        // (f) on commit: row saved, then job inserted.
        // (g) on rollback: neither happens — workers never see this job.
    }
}
```

Six diff-readable scenarios:

1. **Happy path.** `welcome(...)` commits → user row saved, job inserted →
   worker claims, runs `send`, commits outbox row → done.
2. **Rollback after enqueue.** `welcome(...)` throws before commit → user
   row + enqueue both discarded.
3. **Worker crash mid-handler.** Worker dies after `gateway.send(...)` but
   before `outbox.save(...)` → orphan recovery reclaims → second attempt
   sees `outbox.existsById(...)` is false → re-sends. **At-least-once.** The
   handler must accept that real emails can go twice on this exact failure
   shape. If the gateway is `Idempotency-Key`-aware (Stripe, SendGrid, …),
   forward `ctx.jobId()` as the key to make it exactly-once on the gateway
   side.
4. **Handler throws.** `gateway.send` throws → `run`'s `@Transactional` rolls
   back; outbox row never written. Threadmill's failure path records the
   failure cleanly; retry runs the whole handler again on the same job.
5. **Worker crash after outbox save but before Threadmill saves SUCCEEDED.**
   Outbox row committed; Threadmill still thinks the job is PROCESSING.
   Orphan recovery reclaims; retry runs the handler again; the
   `outbox.existsById(...)` check short-circuits at line (a). Threadmill
   transitions to SUCCEEDED on this attempt.
6. **`outbox.save` throws.** Whole handler transaction rolls back; outbox
   row not written; Threadmill records failure; retry runs the whole
   handler again.

The outbox makes scenarios 3 and 5 safe. Without the outbox, scenario 3
results in a duplicate email.

## See also

- `docs/handlers.md` — writing handlers, `@ThreadmillJob`, idempotency, the
  at-least-once contract.
- `docs/concurrency.md` — when to use `EXCLUSIVE` / `SHARED` per-key
  concurrency.
- `threadmill-spring-boot/README.md` — the auto-config story, the
  `SmartLifecycle` phase choice.
- `threadmill-store-postgres/README.md` — Postgres-specific transaction
  semantics, deadlock retry.
- `threadmill-store-redis/README.md` — Lua script atomicity, AOF durability.
