-- Timing fingerprint for the recurring schedule state (issue #105 hardening).
-- Written atomically with next_run_at, the fingerprint records which trigger
-- timing that next run was computed from. Scheduler.upsertCron preserves
-- overdue schedule state only when the re-registered task's fingerprint
-- matches, so a crash between the separate task-definition and state writes
-- can never pair a new trigger with old timing undetectably. NULL (legacy
-- rows) simply forces one recompute on the next re-registration.
ALTER TABLE threadmill_cron_task_state ADD COLUMN timing_fingerprint TEXT;
