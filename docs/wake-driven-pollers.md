# Wake-driven pollers

The pattern: a recurring task that processes whatever work has piled up in
your own tables — an outbox, a queue of pending imports, rows flagged
`needs_sync`. Producers **nudge** the task when they create work, so it runs
within a second; the task's own schedule stays slow and exists only to
self-heal.

## The problem this solves

Poller tasks force a trade-off. Run every 2 seconds and latency is fine, but
you materialize ~43,000 job rows per day per task whether or not there was
anything to do — insert, claim, terminal save, retention delete, every time.
Run every 5 minutes and the churn disappears, but so does responsiveness.

Nudging removes the trade-off: work is processed within a second of being
written, and job-row churn becomes proportional to actual work instead of
wall-clock time.

## The shape

```java
@Job(queue = "system")                    // its own lane: never starved by the flood
@Recurring(interval = "PT10M")            // the backstop, not the driver
class OutboxPump implements JobAction {

    private final OutboxRepository outbox;
    private final PaymentGateway gateway;

    @Override
    public void run(JobExecutionContext ctx) {
        for (OutboxRow row : outbox.findUnprocessed(500)) {
            gateway.send(row);                     // idempotent — see below
            outbox.markProcessed(row.id());
        }
    }
}
```

```java
@Service
class OrderService {

    private final JobScheduler jobs;
    private final OutboxRepository outbox;

    @Transactional
    public void placeOrder(Order order) {
        outbox.save(OutboxRow.of(order));
        jobs.nudgeRecurring(OutboxPump.class);   // takes effect on commit
    }
}
```

That is the whole pattern. Outside Spring it is the same two calls on the
core API: `scheduler.defineIntervalTask("outbox-pump", Duration.ofMinutes(10), …)`
once at startup, then `scheduler.nudgeRecurring("outbox-pump")` from producers.

## Choosing the backstop interval

This is the part people get wrong, because the number looks like "how often
should this run" and it isn't. **Nudges do the work; the interval is only
insurance.** Ask instead:

> If a nudge were lost, how stale am I willing to let this table get?

A nudge can be lost in one narrow case: the process dies between your
transaction committing and the nudge write landing (nudges are recorded
after commit — see [Transactions](transactions.md#nudging-a-recurring-task-wake-driven-pollers)
for why). The schedule bounds that worst case. Nothing else about the
interval affects normal operation.

- **Minutes to an hour** is the normal range. Ten minutes says "in the rare
  crash case, this work can wait ten minutes".
- **Don't set it to seconds.** That recreates exactly the churn you adopted
  nudging to avoid, and buys nothing: the nudge already got you sub-second.
- **Don't set it to days** unless the work genuinely tolerates that. The
  backstop is the only thing standing between a lost nudge and unbounded
  staleness.

If the schedule is firing often enough that you see it in the origin metric,
the interval is too fast for the pattern.

## What the guarantees mean for your handler

- **Run-after-wake.** Every accepted nudge is followed by a run that *starts*
  after it. A nudge landing while the pump is mid-run produces one follow-up
  afterwards — the running pass may have read your table before your row
  committed, so it is never treated as having covered you.
- **Coalescing.** A burst of nudges collapses into at most the current run
  plus one follow-up. You cannot count runs, and you should not try: nudge
  once per work item and let the engine collapse them. A thousand nudges a
  second is fine.
- **Drain everything visible.** Because runs are coalesced, each run must
  process *all* pending rows, not one. Write the handler as a drain loop with
  a sensible batch cap, not as a single-item processor.
- **Idempotency still applies.** Delivery is at-least-once, so a run can
  repeat after a crash. That is unchanged by nudging — see
  [Handlers](handlers.md).
- **The schedule is untouched.** A nudged run never moves the next regular
  fire: a cron task keeps its wall-clock grid, and an interval keeps its
  phase.

## Edges

- Nudging an unknown task throws `IllegalArgumentException`; nudging a
  disabled one throws `IllegalStateException` — an explicit pause beats a
  nudge. Disabling or re-enabling a task clears any pending nudge, so
  re-enabling never fires demand recorded before the pause.
- Nudging is not "run this with these arguments". It carries no payload; it
  asks an already-registered task to run. For work that carries data, use
  `enqueue`.
- Latency is bounded by `maintenancePollInterval` (default 1 s), because the
  nudge is a durable store write consumed by the maintenance leader rather
  than a signal that could be dropped. Nothing to configure, nothing to lose.

## Watching it work

Every recurring instance records what triggered it:

- **Dashboard** — a `schedule` / `nudge` / `manual` badge next to the handler
  in the jobs table.
- **Metrics** — `threadmill.jobs.recurring.runs{origin="nudge"}` versus
  `origin="schedule"`. In a healthy wake-driven pump the schedule count stays
  near zero; if it climbs, either nudges are not reaching the store or your
  interval is too fast to be a backstop.
- **Handler code** — `ctx.cronOrigin()`.

A useful sanity check is the nudge-to-run ratio. Under sustained load it is
normal to see hundreds of nudges collapse into a handful of runs; that ratio
is the feature working, not work going missing.

## See also

- [Transactions](transactions.md#nudging-a-recurring-task-wake-driven-pollers) —
  the transactional contract for nudging, and why nudges never join the
  caller's transaction.
- [Handlers](handlers.md) — the at-least-once contract and `ctx.cronOrigin()`.
- [Queue topology](queue-topology.md) — why the pump gets its own lane.
