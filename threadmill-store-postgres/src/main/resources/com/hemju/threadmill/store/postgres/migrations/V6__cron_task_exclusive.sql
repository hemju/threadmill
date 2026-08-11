-- Claim-time exclusive execution for recurring tasks. FALSE means "no
-- claim-time concurrency key" — the behaviour of every pre-existing row.
ALTER TABLE threadmill_cron_tasks ADD COLUMN exclusive BOOLEAN NOT NULL DEFAULT FALSE;
