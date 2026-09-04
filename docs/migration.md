# Migration Guide

Map existing background work to `JobPayload` plus `JobHandler`. Keep handler
code idempotent, move external side effects behind an application idempotency
key, and use Threadmill queues to preserve operational separation between
latency-sensitive and bulk work.

For scheduled work, use `Scheduler.scheduleAt` for one-off jobs and
`defineCronTask` or `defineIntervalTask` for recurring jobs. Choose
`DROP` for normal recurring tasks and `CATCH_UP` only when every missed firing
must run.

Before production, run the getting-started example, port one real job, then run
the job twice manually to prove idempotency before enabling recurring or retry
behavior.

## Metrics Wiring

Use `ThreadmillMetrics.meteredStore()` as the store passed to processing nodes
and producers. It now records claim latency/failures and rejected-write
attempts at the actual `JobStore` boundary. Gauge reads refresh one shared
snapshot after the configured interval, independently of job completion.

## Renaming Handlers And Payloads

Threadmill persists exact handler class names in `JobSpec.handlerType()` and
exact payload type names in `JobArgument.typeTag()`. Runtime aliases and
payload-shape migrations are intentionally unsupported. Drain or delete every
job and recurring definition that uses the old names before deploying a class
or payload rename, or rewrite the durable records with an application-owned
offline migration. An unresolved handler or payload follows the normal
quarantine path.

For annotation-driven Spring recurring tasks, set
`threadmill.spring.recurring-namespace` or `spring.application.name`. Threadmill
then reconciles the namespace at startup: discovered recurring tasks are
upserted, and previously-owned tasks missing from the current application are
deleted. Set `@Recurring(recurringName = "...")` when you want the durable
recurring identity to survive a handler class rename.
