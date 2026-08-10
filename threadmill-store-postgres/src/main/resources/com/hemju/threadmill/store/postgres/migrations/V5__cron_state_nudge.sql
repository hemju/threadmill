-- On-demand materialization requests ("nudges") for recurring tasks
-- (issue #108). A producer's nudge stamps this cell; the maintenance
-- master's recurring tick consumes it with a compare-and-clear. One cell
-- per task makes nudge bursts coalesce structurally. Deliberately written
-- only by requestCronNudge / clearCronNudge — the blanket state upsert
-- leaves it untouched so re-registrations and materializer bookkeeping
-- cannot clobber a concurrently accepted nudge — and deliberately
-- unindexed so nudge writes stay HOT-eligible.
ALTER TABLE threadmill_cron_task_state ADD COLUMN nudge_requested_at TIMESTAMPTZ;
