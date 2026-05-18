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

## Renaming Handlers And Payloads

Threadmill persists handler class names in `JobSpec.handlerType()` and payload
type names in `JobArgument.typeTag()`. Renames are safe only when the deployed
application tells Threadmill how to map the old names:

- Use `TypeNameAliases` for handler and payload class/package moves.
- Use `PayloadMigrations` when the old payload JSON shape cannot deserialize
  into the new class directly.
- Use `JobDefinitionMigrator` to rewrite already-persisted non-running jobs
  (`ENQUEUED`, `SCHEDULED`, `AWAITING`) from an old handler signature to the
  new `JobSpec`.

Jobs already running are not rewritten. If a payload or handler cannot be
resolved and no alias/migration exists, the normal quarantine path applies.

For annotation-driven Spring recurring tasks, set
`threadmill.spring.recurring-namespace` or `spring.application.name`. Threadmill
then reconciles the namespace at startup: discovered recurring tasks are
upserted, and previously-owned tasks missing from the current application are
deleted. Set `@Recurring(recurringName = "...")` when you want the durable
recurring identity to survive a handler class rename.
