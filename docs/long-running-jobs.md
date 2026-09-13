# Long-Running Jobs

Long-running handlers should check in while they make progress. Check-ins are
liveness and progress signals; they are not a way to disable failure detection.

```java
@Job(queue = "imports", timeout = "PT1M")
final class ImportHandler implements JobHandler<ImportFile> {
    @Override
    public void run(ImportFile payload, JobExecutionContext ctx) throws Exception {
        for (int i = 0; i < payload.parts(); i++) {
            importPart(payload, i);
            ctx.updateProgress((i + 1) / (double) payload.parts());
            ctx.checkIn("imported part " + (i + 1));
        }
    }
}
```

## Timeout Rules

- If a job never checks in, `jobTimeout` (or the per-job override) applies
  from `claimedAt()`.
- Once a job checks in, wall-clock `jobTimeout` no longer kills it.
- If progress stops, `noProgressTimeout` applies from the most recent check-in.
- While the node is shutting down, the deadline is never later than the end
  of `shutdownGracePeriod`.

`ctx.deadline()` is that instant, computed by the same rule the engine's
watchdog uses, and `ctx.remaining()` is the time left. When the deadline
passes the engine **interrupts the worker thread**; on a virtual thread that
can abort in-flight socket I/O and leaves the interrupt flag set — see
[Handlers → Timeouts](handlers.md#timeouts) for the full contract. A
long-running handler therefore checks its budget before each step rather than
relying on the interrupt:

```java
for (int i = savedCursor(payload); i < payload.parts(); i++) {
    if (ctx.remaining().compareTo(PART_BUDGET) < 0) {
        saveCursor(payload, i);            // the next attempt resumes here
        throw new OutOfTimeException();    // FAILED → retried under the retry policy
    }
    importPart(payload, i);
    ctx.updateProgress((i + 1) / (double) payload.parts());
    ctx.checkIn("imported part " + (i + 1));
}
```

Cleanup that runs after an interrupt reads `ctx.cancellation()` (`TIMEOUT` or
`SHUTDOWN`) rather than the interrupt flag or the exception it caught.

The engine coalesces check-in, progress, and log persistence to at most one
write per `checkInMinInterval`, plus a final flush before success or failure.
Store write failures during a check-in are best-effort: they are logged and
counted, but they are not thrown into user handler code.

## Bounded Logs

`ctx.log(message)` appends an INFO entry to the per-job log. Logs are bounded by:

- `logMaxRatePerSecond` (default `100` accepted entries per second)
- `logMaxEntries` (default `1000`)
- `logMaxBytes` (default `256KB` of message text)

Older entries are discarded first when size limits are exceeded.

## Ordering execution updates

Progress, log, and check-in flushes from one execution context are serialized.
Each confirmed flush advances an attempt-local execution revision, separately
from the job state version. An older snapshot is rejected, including when both
snapshots have the same check-in timestamp. Claim resets the revision for the
next attempt. Custom `JobStore` implementations must preserve this contract.

Owner heartbeats and check-in times never move backward. Store reads merge the
current heartbeat scalar into the job view; PostgreSQL and Redis can therefore
refresh node liveness without rewriting every job body on each heartbeat tick.
