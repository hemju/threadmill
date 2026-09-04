# Handlers

A unit of background work in Threadmill is a **payload** (what to do) plus a
**handler** (how to do it).

## The contract

A payload is a simple Jackson-serializable command object implementing the
`JobPayload` marker interface. A handler implements `JobHandler<P>` for
exactly one payload type:

```java
public record SendEmail(String to, String subject) implements JobPayload {}

public final class SendEmailHandler implements JobHandler<SendEmail> {
    @Override
    public void run(SendEmail payload, JobExecutionContext ctx) throws Exception {
        ctx.log("sending " + payload.subject() + " to " + payload.to());
        // ... the external side effect must be idempotent ...
    }
}
```

Any exception thrown from `run` funnels through the engine's single failure
path: a state transition to `FAILED` plus interceptor notification (which is
where retry happens). For periodic work that needs no per-invocation payload,
implement `JobAction` — a specialization of `JobHandler<NoPayload>`.

**Handlers must be idempotent.** Threadmill delivers at-least-once: after a
node crash, an expired heartbeat, or a retry, the same logical job runs again.
Design every handler as if it will run twice — occasionally it will. See
[Transactions](transactions.md) before writing a handler that touches a
database.

## Handler resolution

A job's spec names the handler's fully-qualified type; the engine resolves an
instance through the `JobHandlerResolver` SPI:

- **`ReflectiveJobHandlerResolver`** (core) constructs handlers from a no-arg
  constructor via reflection and caches the instance. Adequate for tests and
  small applications. Persisted handler names must match the current class
  name exactly.
- **`SpringJobHandlerResolver`** (`threadmill-spring-boot`) first tries a bean
  lookup, then falls back to autowire-by-type — handlers can be `@Component`
  beans with constructor injection. With the `@Job` annotation, Spring
  discovers handlers at startup and `JobScheduler` verifies the
  handler/payload pair at enqueue time.

A job whose handler cannot be resolved, or whose payload cannot be
deserialized, is moved to `QUARANTINED`. It never crashes the worker loop and
is never retried.

## JobExecutionContext

The second argument to `run` is the per-execution view of the job — not the
structure the engine serializes. It exposes:

- **Identity and timing:** `jobId()`, `nodeId()`, `attempt()` (starting at 1),
  `claimedAt()`.
- **`deadline()` / `remaining()`** — the instant the engine will interrupt
  this attempt if it is still running, and the time left until then. Computed
  by the same rule as the engine's watchdog (see [Timeouts](#timeouts)), so a
  handler that checks `remaining()` against its next step's cost stops before
  the interrupt instead of being cut off mid-step.
- **`cancellation()` / `isCancelled()`** — why the engine has abandoned this
  attempt (`TIMEOUT` or `SHUTDOWN`), set immediately before the worker thread
  is interrupted and never cleared. Cleanup code reads this instead of the
  thread's interrupt status or the exception it caught.
- **`log(message)`** — appends an INFO entry to the bounded per-job log.
- **`updateProgress(fraction)`** — reports fraction complete, `0.0`–`1.0`.
- **`checkIn()` / `checkIn(message)`** — records that a long-running job is
  alive and making progress. Check-in, progress, and log writes are coalesced
  to at most one store write per `checkInMinInterval` (default 5s), plus a
  final flush; write failures are logged, never thrown into handler code.
- **`setResult(value)`** — records a typed result, persisted together with
  the `SUCCEEDED` transition and bounded by the same job size cap as the rest
  of the job body. `readResult()` reads it back.
- **`cronFireTime()`** — for recurring instances, the nominal schedule tick
  this instance represents. Under the `CATCH_UP` missed-run policy each missed
  interval's instance carries its own fire time, so an idempotent handler can
  derive a per-interval idempotency key from it. Under `DROP`, the single
  recovery instance for a missed backlog carries the most recent nominal
  fire time — never the recovery wall-clock — so the key stays meaningful
  there too. Nudged instances (`nudgeRecurring`) carry no fire time: they
  represent no schedule tick.
- **`cronOrigin()`** — for recurring instances, what triggered this one:
  `schedule` (a regular fire), `nudge` (`nudgeRecurring`), or `manual` (the
  dashboard's force-trigger).
- **`metadata()`** — mutable per-job metadata.

See [Long-running jobs](long-running-jobs.md) for check-in patterns and the
per-job log bounds.

Code below the handler that has no `ctx` parameter reaches the running
context through `JobExecutionContext.current()` (empty outside a job). It
resolves the same scoped-value binding described next, so it works from
anything the handler calls on its own thread and from structured-concurrency
forks, but not on plain executor threads.

## Scoped values, not ThreadLocal

Per-execution context (job id, attempt, MDC) is propagated with a
`ScopedValue`, bound around `handler.run(...)`. The binding is inherited by
structured-concurrency forks — a `StructuredTaskScope` opened inside the
handler — but **not** by threads the handler spawns directly via an executor
or `Thread.ofVirtual().start(...)`. To carry the context across such a
boundary, wrap the work with `EngineScopedValues.capturing(...)`:

```java
executor.submit(EngineScopedValues.capturing(() -> {
    // context-aware work on a handler-spawned thread
}));
```

## Timeouts

Every attempt runs under a deadline, and **when it passes the engine
interrupts the worker thread**. The failure routes through the same single
failure path as a thrown exception, with cause `TIMEOUT`.

The deadline is computed by one rule, shared by the watchdog and
`ctx.deadline()`:

1. **Before the first `checkIn()`:** `claimedAt()` plus the job's effective
   timeout — the per-job override in metadata under
   `threadmill.job.timeoutSeconds` (`JobRunner.META_TIMEOUT_SECONDS`) when
   present, else `threadmill.jobTimeout` (default 5m). Spring's
   `@Job(timeout = "PT2M")` writes the override for you. Recurring tasks carry
   it on their definition — `@Job(timeout = ...)` on a `@Recurring` handler,
   or the `timeout` parameter of `Scheduler.defineCronTask` /
   `defineIntervalTask` / `defineRecurring` — and every materialized instance
   (including a manual dashboard trigger) inherits it.
2. **After a check-in:** the most recent check-in plus `noProgressTimeout`
   (default 15m). A handler that checks in between steps is bounded by how
   long it may go silent, not by total runtime: it can run for hours, and one
   that goes quiet is killed.
3. **While the node is shutting down:** no later than the end of
   `shutdownGracePeriod`, after which the draining worker pool interrupts
   whatever is still running (see [Shutdown](operations.md#shutdown)).

The watchdog checks about once a second, so the interrupt lands up to a
second after `deadline()`.

### What an interrupt does to your handler

Workers are virtual threads, and on a virtual thread an interrupt is not a
polite flag:

- Blocking I/O in progress can be aborted. The JDK guarantees this for
  `java.net.Socket` with the default implementation and for interruptible
  channels: the socket is closed and `SocketException: Closed by interrupt`
  (or `ClosedByInterruptException`) is thrown. That covers the JDBC, HTTP,
  and Redis clients built on those primitives; a client that brings its own
  transport may translate the interrupt differently or only notice it at its
  next interruptible call.
- The interrupt flag **stays set**. Only methods that throw
  `InterruptedException` clear it; a `SocketException` does not, and the
  for a `TIMEOUT` cancellation the watchdog re-asserts the interrupt every
  tick until the handler returns — a `checkIn()` from cleanup code does not
  lift it. A `SHUTDOWN` cancellation is delivered once, when the grace period
  expires: the node is closing and stops its watchdog, so a handler that
  swallows that interrupt runs on unobserved until the process exits. Every
  later
  blocking call on the thread can fail the same way, and each pooled
  connection the handler borrows for cleanup may be destroyed on first use.

So treat an interrupt as cancellation: stop issuing blocking calls, do not
classify the failure as a fault of the external system you were talking to,
and return or rethrow promptly. `ctx.cancellation()` tells cleanup code why
the attempt was abandoned (`TIMEOUT` or `SHUTDOWN`) without inspecting
exceptions or the interrupt flag; it is set immediately before the interrupt
is sent and is never cleared.

### Stopping before the deadline

The engine never has to interrupt a cooperative handler. Before each costly
step, compare `ctx.remaining()` with what the step needs:

```java
for (Step step : plan) {
    if (ctx.remaining().compareTo(step.budget()) < 0) {
        checkpoint(step);                                    // persist where we stopped
        jobs.enqueue(new ResumePlan(payload.id(), step.index())); // continuation job
        return;                                              // SUCCEEDED
    }
    step.run();
    ctx.checkIn();
}
```

There are two ways out and they mean different things. **Checkpoint and
return** leaves the job `SUCCEEDED`, so the handler must make the remaining
work reachable itself — a continuation job, or a persisted cursor its next run
reads. **Throw** leaves the job `FAILED` and retried under the normal retry
policy, which costs an attempt — including when the deadline collapsed
because the node is closing. The free, immediate requeue applies only to an
attempt the engine itself cancelled, that is once `ctx.cancellation()` reports
`SHUTDOWN` because the interrupt landed; the engine cannot tell a deliberate
early stop from a genuine failure, so a handler winding down early during a
drain should checkpoint and return. Sizing a single long call is the same
move — give the client a timeout of `remaining()` minus a margin, and the
call ends cleanly before the socket is torn down under it.

## Retry

Retry is implemented by `RetryInterceptor`, which reschedules a failed job
(`FAILED → SCHEDULED`) with exponential backoff. Precedence, most specific
first:

1. **Per-job metadata override** — `threadmill.retry.maxAttempts` (integer)
   and/or `threadmill.retry.initialBackoffSeconds` (long); either key alone
   activates the override. An explicit Spring `@Job(maxAttempts = 5)` maps to
   it, on the enqueue path and (like the timeout) on every materialized
   instance of a `@Recurring` handler; a handler without an explicit value
   stamps nothing, so the per-exception-type and global tiers below stay
   reachable. Core users set it per recurring definition via the
   `maxAttempts` parameter of `Scheduler.defineCronTask` / `defineIntervalTask`
   / `defineRecurring`. The value counts total attempts including the first —
   `maxAttempts = 1` is one attempt, no retries.
2. **Per-exception-type policy** — registered via
   `RetryInterceptor.policyFor(Class, RetryPolicy)`; the most specific class
   match in the exception's hierarchy wins.
3. **Global default** — `threadmill.defaultMaxAttempts` (default 5) and
   `threadmill.retryInitialBackoff` (default 5s).

Quarantined jobs never retry — a permanently broken job cannot cause a retry
storm.

A recurring task that must never run two instances at once declares
`@Recurring(exclusive = true)` (or the `exclusive` flag on
`Scheduler.defineRecurring`), which serializes its instances at claim time
under a derived key instead of leaving you to hand-roll an advisory lock. See
[Exclusive recurring tasks](concurrency.md#exclusive-recurring-tasks).
