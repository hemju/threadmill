-- On-demand materialization requests ("nudges") for recurring tasks
-- (issue #108). A producer's nudge stamps nudge_requested_at and advances
-- nudge_revision; the maintenance master's recurring tick consumes the
-- request with a compare-and-clear on the REVISION — the store-generated,
-- strictly monotonic, never-reset identity — because wall-clock timestamps
-- can collide within store precision and a collision would let a clear
-- erase a newer nudge. One cell per task makes nudge bursts coalesce
-- structurally. Both columns are written only by requestCronNudge /
-- clearCronNudge — the blanket state upsert leaves them untouched so
-- re-registrations and materializer bookkeeping cannot clobber a
-- concurrently accepted nudge — and both are deliberately unindexed so
-- nudge writes stay HOT-eligible.
ALTER TABLE threadmill_cron_task_state ADD COLUMN nudge_requested_at TIMESTAMPTZ;
ALTER TABLE threadmill_cron_task_state ADD COLUMN nudge_revision BIGINT;
