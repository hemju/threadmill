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
