-- Threadmill v1 baseline schema (PostgreSQL 18+).
--
-- This file is the single consolidated migration for v1. The migration runner
-- bootstraps threadmill_schema_history itself before recording that this one
-- ran. Future schema changes ship as additive V2__*.sql, V3__*.sql, … files.
--
-- Body column is the source-of-truth wire form. The other columns are
-- denormalised, indexed scalars that exist purely so the engine's hot queries
-- hit indexes without parsing the body. Keep them in sync with the body on
-- every write.

CREATE TABLE threadmill_jobs (
    id UUID PRIMARY KEY,
    state TEXT NOT NULL,
    queue TEXT NOT NULL,
    priority INT NOT NULL DEFAULT 0,
    handler_signature TEXT NOT NULL,
    scheduled_at TIMESTAMPTZ,
    owner_node_id UUID,
    owner_heartbeat_at TIMESTAMPTZ,
    last_checkin_at TIMESTAMPTZ,
    current_state_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL,
    body TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    concurrency_key TEXT,
    concurrency_mode TEXT,
    workflow_root_id UUID NOT NULL,
    parent_job_id UUID
);

-- Claim path: WHERE state='ENQUEUED' AND queue=? ORDER BY priority DESC, id LIMIT n FOR UPDATE SKIP LOCKED.
CREATE INDEX threadmill_jobs_enqueued_idx
    ON threadmill_jobs (queue, priority DESC, id)
    WHERE state = 'ENQUEUED';

-- Due-for-promotion: WHERE state='SCHEDULED' AND scheduled_at <= now() ORDER BY scheduled_at LIMIT n.
CREATE INDEX threadmill_jobs_scheduled_idx
    ON threadmill_jobs (scheduled_at)
    WHERE state = 'SCHEDULED';

-- Orphan-recovery: WHERE state='PROCESSING' AND owner_heartbeat_at <= ? ORDER BY owner_heartbeat_at LIMIT n.
CREATE INDEX threadmill_jobs_processing_idx
    ON threadmill_jobs (owner_heartbeat_at)
    WHERE state = 'PROCESSING';

-- Orphan recovery uses the latest processing liveness marker: owner heartbeat
-- or long-running job check-in, whichever is newer.
CREATE INDEX threadmill_jobs_processing_liveness_idx
    ON threadmill_jobs ((GREATEST(owner_heartbeat_at, COALESCE(last_checkin_at, owner_heartbeat_at))))
    WHERE state = 'PROCESSING';

-- Find-by-handler-signature.
CREATE INDEX threadmill_jobs_handler_idx ON threadmill_jobs (handler_signature);

-- Retention: WHERE state=? AND current_state_at <= ?.
CREATE INDEX threadmill_jobs_state_time_idx ON threadmill_jobs (state, current_state_at);

-- Claim-time concurrency pending check. The engine asks "is there an earlier
-- pending job for this concurrency key?" before committing a claim; without
-- this partial index the check degrades to a table scan on busy stores.
CREATE INDEX threadmill_jobs_concurrency_pending_idx
    ON threadmill_jobs (concurrency_key, current_state_at, id)
    WHERE state IN ('ENQUEUED', 'SCHEDULED', 'AWAITING');

-- Workflow successor promotion: find AWAITING jobs whose parent just completed.
CREATE INDEX threadmill_jobs_awaiting_parent_idx
    ON threadmill_jobs (parent_job_id, current_state_at, id)
    WHERE state = 'AWAITING' AND parent_job_id IS NOT NULL;

CREATE TABLE threadmill_nodes (
    id UUID PRIMARY KEY,
    last_heartbeat_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE threadmill_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Recurring tasks: identity (definition) and schedule-state (last/next run
-- bookkeeping) are kept in separate tables so re-registering a task never
-- resurrects stale timing.
CREATE TABLE threadmill_cron_tasks (
    name TEXT PRIMARY KEY,
    trigger_kind TEXT NOT NULL,           -- CRON | INTERVAL
    trigger_value TEXT NOT NULL,          -- cron expression or ISO-8601 duration
    handler_signature TEXT NOT NULL,
    payload_type_tag TEXT NOT NULL,
    payload_serialized TEXT NOT NULL,
    queue TEXT NOT NULL DEFAULT 'default',
    priority INT NOT NULL DEFAULT 0,
    missed_run_policy TEXT NOT NULL DEFAULT 'DROP',
    time_zone TEXT NOT NULL DEFAULT 'UTC',
    enabled BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE threadmill_cron_task_state (
    task_name TEXT PRIMARY KEY REFERENCES threadmill_cron_tasks(name) ON DELETE CASCADE,
    last_run_at TIMESTAMPTZ,
    last_run_job_id UUID,
    next_run_at TIMESTAMPTZ,
    in_flight_job_id UUID
);

CREATE INDEX threadmill_cron_task_state_due_idx ON threadmill_cron_task_state (next_run_at);

-- Cross-cluster named mutex with a lease. expires_at drives the "dead holder
-- cannot block forever" rule.
CREATE TABLE threadmill_mutexes (
    name TEXT PRIMARY KEY,
    holder TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX threadmill_mutexes_expiry_idx ON threadmill_mutexes (expires_at);

-- Store-backed leadership leases for the maintenance cycle.
CREATE TABLE threadmill_leases (
    name TEXT PRIMARY KEY,
    holder UUID NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX threadmill_leases_expires_idx ON threadmill_leases (expires_at);

-- Producer-side deduplication. Cleanup is gated on the referenced job being
-- terminal so a long-running active job retains its dedup protection.
CREATE TABLE threadmill_dedup_keys (
    queue TEXT NOT NULL,
    dedup_key TEXT NOT NULL,
    job_id UUID NOT NULL REFERENCES threadmill_jobs(id) ON DELETE CASCADE,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (queue, dedup_key)
);

CREATE INDEX threadmill_dedup_keys_expires_idx
    ON threadmill_dedup_keys (expires_at);

-- Claim-time per-key concurrency bookkeeping. Updated in the same transaction
-- as the job state transition so counts and in-flight job state can never
-- diverge.
CREATE TABLE threadmill_concurrency_groups (
    concurrency_key TEXT PRIMARY KEY,
    exclusive_in_flight INT NOT NULL DEFAULT 0,
    shared_in_flight INT NOT NULL DEFAULT 0,
    last_modified TIMESTAMPTZ NOT NULL
);

-- Workflow-root outstanding counts. The concurrency lock is held continuously
-- from the workflow root's claim to the last descendant's terminal save.
CREATE TABLE threadmill_concurrency_workflow_holds (
    concurrency_key TEXT NOT NULL,
    workflow_root_id UUID NOT NULL,
    outstanding INT NOT NULL,
    PRIMARY KEY (concurrency_key, workflow_root_id)
);

-- Per-queue pause primitive. Operators (or the admin API) can pause one
-- queue without restarting the cluster; pending jobs stay in ENQUEUED and
-- resume claiming the moment the queue is resumed.
CREATE TABLE threadmill_queue_pauses (
    queue TEXT PRIMARY KEY,
    paused_at TIMESTAMPTZ NOT NULL,
    paused_by TEXT
);

-- Incrementally-maintained per-state counts. Reading per-state counts must
-- not contend with the claim path on a large jobs table — a naive COUNT(*)
-- is a known production bottleneck. The trigger below keeps this table in
-- sync row-by-row.
CREATE TABLE threadmill_job_counts (
    state TEXT PRIMARY KEY,
    count BIGINT NOT NULL DEFAULT 0
);

INSERT INTO threadmill_job_counts (state, count) VALUES
    ('AWAITING', 0),
    ('SCHEDULED', 0),
    ('ENQUEUED', 0),
    ('PROCESSING', 0),
    ('PROCESSED', 0),
    ('SUCCEEDED', 0),
    ('FAILED', 0),
    ('DELETED', 0),
    ('QUARANTINED', 0);

CREATE OR REPLACE FUNCTION threadmill_maintain_counts() RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        UPDATE threadmill_job_counts SET count = count + 1 WHERE state = NEW.state;
    ELSIF (TG_OP = 'DELETE') THEN
        UPDATE threadmill_job_counts SET count = count - 1 WHERE state = OLD.state;
    ELSIF (TG_OP = 'UPDATE' AND OLD.state IS DISTINCT FROM NEW.state) THEN
        UPDATE threadmill_job_counts SET count = count - 1 WHERE state = OLD.state;
        UPDATE threadmill_job_counts SET count = count + 1 WHERE state = NEW.state;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER threadmill_jobs_counts_trigger
AFTER INSERT OR UPDATE OR DELETE ON threadmill_jobs
FOR EACH ROW EXECUTE FUNCTION threadmill_maintain_counts();
