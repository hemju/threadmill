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
implement `JobAction` — a typed alias for `JobHandler<NoPayload>`.

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
  small applications; it also accepts `TypeNameAliases` so a renamed handler
  class can still resolve jobs persisted under the old name.
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
  derive a per-interval idempotency key from it.
- **`metadata()`** — mutable per-job metadata.

See [Long-running jobs](long-running-jobs.md) for check-in patterns and the
per-job log bounds.

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

Every job runs under a wall-clock timeout (`threadmill.jobTimeout`, default
5m). A per-job override lives in job metadata under
`threadmill.job.timeoutSeconds` (`JobRunner.META_TIMEOUT_SECONDS`); Spring's
`@Job(timeout = "PT2M")` writes it for you. Recurring tasks carry the same
override on their definition — `@Job(timeout = ...)` on a `@Recurring`
handler, or the `timeout` parameter of `Scheduler.defineCronTask` /
`defineIntervalTask` / `defineRecurring` — and every materialized instance
(including a manual dashboard trigger) inherits it. On timeout the watchdog
interrupts the handler thread, and the failure routes through the same single
failure path as a thrown exception.

Once a job has checked in at least once, the wall-clock timeout no longer
applies; instead `noProgressTimeout` (default 15m) runs from the most recent
check-in. A handler that checks in regularly can run for hours; one that goes
silent is killed.

## Retry

Retry is implemented by `RetryInterceptor`, which reschedules a failed job
(`FAILED → SCHEDULED`) with exponential backoff. Precedence, most specific
first:

1. **Per-job metadata override** — `threadmill.retry.maxAttempts` (integer)
   and/or `threadmill.retry.initialBackoffSeconds` (long); either key alone
   activates the override. Spring's `@Job(maxRetries = 5)` maps to it, on the
   enqueue path and (like the timeout) on every materialized instance of a
   `@Recurring` handler; core users set it per recurring definition via the
   `maxAttempts` parameter of `Scheduler.defineCronTask` / `defineIntervalTask`
   / `defineRecurring`.
2. **Per-exception-type policy** — registered via
   `RetryInterceptor.policyFor(Class, RetryPolicy)`; the most specific class
   match in the exception's hierarchy wins.
3. **Global default** — `threadmill.defaultMaxAttempts` (default 5) and
   `threadmill.retryInitialBackoff` (default 5s).

Quarantined jobs never retry — a permanently broken job cannot cause a retry
storm.
