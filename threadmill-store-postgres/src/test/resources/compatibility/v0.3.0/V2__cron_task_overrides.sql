-- Per-instance overrides carried on the recurring definition. NULL means
-- "use the engine defaults" — the behaviour of every pre-existing row.
ALTER TABLE threadmill_cron_tasks ADD COLUMN timeout_seconds BIGINT;
ALTER TABLE threadmill_cron_tasks ADD COLUMN max_attempts INT;
