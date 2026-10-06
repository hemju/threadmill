-- Threadmill v1 baseline schema (Oracle Database 19c+).
--
-- The whole schema installs in one migration. Post-release changes ship as
-- additive V2__*.sql, V3__*.sql, ... files; this baseline is never edited
-- again once released. OracleMigrationRunner bootstraps
-- threadmill_schema_history itself.
--
-- Statement format: every statement ends with a line containing only "/"
-- (the SQL*Plus / SQLcl convention), so the file runs unchanged in those
-- tools. SQL statements carry no trailing semicolon; PL/SQL blocks do.
--
-- Oracle commits DDL implicitly, so a migration is not atomic. The runner
-- records progress after every statement and resumes a crashed migration at
-- the first unrecorded statement.
--
-- Type mapping and conventions:
--   * Job and node ids are RAW(16) in big-endian UUID byte order. RAW compares
--     unsigned byte by byte, which equals JobId's canonical natural order.
--   * Timestamps are TIMESTAMP(9) holding UTC wall time: nanosecond-exact for
--     java.time.Instant, time-zone independent, and directly indexable.
--   * Booleans are NUMBER(1) with a 0/1 check (BOOLEAN is 23ai-only).
--   * Text uses CHAR length semantics where values are user names, and the
--     database character set must be AL32UTF8 (checked at startup).
--   * Oracle has no partial indexes. Each one is emulated with VIRTUAL columns of
--     the form CASE WHEN <predicate> THEN <column> END and an index over them:
--     Oracle B-tree indexes skip rows whose key columns are all NULL, so the
--     index holds only rows matching <predicate>. Virtual columns (rather than
--     expression indexes) give the queries plain column names and let the 19c
--     optimizer read index order directly, so per-key top-n probes stop after
--     n index entries instead of sorting the key's whole range.
--
-- The body column is the source-of-truth wire form. The other job columns are
-- denormalised, indexed scalars kept in sync with the body on every write.

CREATE TABLE threadmill_jobs (
    id RAW(16) NOT NULL,
    state VARCHAR2(16) NOT NULL,
    queue VARCHAR2(128 CHAR) NOT NULL,
    priority NUMBER(10) DEFAULT 0 NOT NULL,
    handler_signature VARCHAR2(1000 CHAR) NOT NULL,
    scheduled_at TIMESTAMP(9),
    owner_node_id RAW(16),
    owner_heartbeat_at TIMESTAMP(9),
    last_checkin_at TIMESTAMP(9),
    current_state_at TIMESTAMP(9) NOT NULL,
    version NUMBER(19) NOT NULL,
    execution_revision NUMBER(19) DEFAULT 0 NOT NULL,
    body CLOB NOT NULL,
    created_at TIMESTAMP(9) NOT NULL,
    concurrency_key VARCHAR2(256 BYTE),
    concurrency_mode VARCHAR2(16),
    workflow_root_id RAW(16) NOT NULL,
    parent_job_id RAW(16),
    -- Claim path, unkeyed lane: ENQUEUED jobs without a concurrency key, per
    -- queue, in (priority DESC, id) order. The rank is -priority so the index
    -- is ascending throughout.
    unkeyed_queue VARCHAR2(128 CHAR) GENERATED ALWAYS AS (
        CASE WHEN state = 'ENQUEUED' AND concurrency_key IS NULL THEN queue END) VIRTUAL,
    unkeyed_rank NUMBER(11) GENERATED ALWAYS AS (
        CASE WHEN state = 'ENQUEUED' AND concurrency_key IS NULL THEN -priority END) VIRTUAL,
    unkeyed_id RAW(16) GENERATED ALWAYS AS (
        CASE WHEN state = 'ENQUEUED' AND concurrency_key IS NULL THEN id END) VIRTUAL,
    -- Claim path, keyed lane: per-queue distinct keys and each key's ENQUEUED
    -- heads in the engine's (current_state_at, id) in-key order.
    keyed_queue VARCHAR2(128 CHAR) GENERATED ALWAYS AS (
        CASE WHEN state = 'ENQUEUED' AND concurrency_key IS NOT NULL THEN queue END) VIRTUAL,
    keyed_key VARCHAR2(256 BYTE) GENERATED ALWAYS AS (
        CASE WHEN state = 'ENQUEUED' AND concurrency_key IS NOT NULL THEN concurrency_key END)
        VIRTUAL,
    keyed_at TIMESTAMP(9) GENERATED ALWAYS AS (
        CASE WHEN state = 'ENQUEUED' AND concurrency_key IS NOT NULL THEN current_state_at END)
        VIRTUAL,
    keyed_id RAW(16) GENERATED ALWAYS AS (
        CASE WHEN state = 'ENQUEUED' AND concurrency_key IS NOT NULL THEN id END) VIRTUAL,
    -- Claim-time admission: pending (ENQUEUED, SCHEDULED, AWAITING) keyed jobs.
    pending_key VARCHAR2(256 BYTE) GENERATED ALWAYS AS (
        CASE WHEN state IN ('ENQUEUED', 'SCHEDULED', 'AWAITING')
            THEN concurrency_key END) VIRTUAL,
    pending_at TIMESTAMP(9) GENERATED ALWAYS AS (
        CASE WHEN state IN ('ENQUEUED', 'SCHEDULED', 'AWAITING')
            AND concurrency_key IS NOT NULL THEN current_state_at END) VIRTUAL,
    pending_id RAW(16) GENERATED ALWAYS AS (
        CASE WHEN state IN ('ENQUEUED', 'SCHEDULED', 'AWAITING')
            AND concurrency_key IS NOT NULL THEN id END) VIRTUAL,
    -- Claim-time admission: pending EXCLUSIVE jobs (the leapfrog rule).
    exclusive_key VARCHAR2(256 BYTE) GENERATED ALWAYS AS (
        CASE WHEN state IN ('ENQUEUED', 'SCHEDULED', 'AWAITING')
            AND concurrency_mode = 'EXCLUSIVE' THEN concurrency_key END) VIRTUAL,
    exclusive_at TIMESTAMP(9) GENERATED ALWAYS AS (
        CASE WHEN state IN ('ENQUEUED', 'SCHEDULED', 'AWAITING')
            AND concurrency_mode = 'EXCLUSIVE' THEN current_state_at END) VIRTUAL,
    exclusive_id RAW(16) GENERATED ALWAYS AS (
        CASE WHEN state IN ('ENQUEUED', 'SCHEDULED', 'AWAITING')
            AND concurrency_mode = 'EXCLUSIVE' THEN id END) VIRTUAL,
    -- Workflow holds: non-terminal keyed jobs by (key, workflow root).
    outstanding_key VARCHAR2(256 BYTE) GENERATED ALWAYS AS (
        CASE WHEN state NOT IN ('SUCCEEDED', 'FAILED', 'DELETED', 'QUARANTINED')
            THEN concurrency_key END) VIRTUAL,
    outstanding_root RAW(16) GENERATED ALWAYS AS (
        CASE WHEN state NOT IN ('SUCCEEDED', 'FAILED', 'DELETED', 'QUARANTINED')
            AND concurrency_key IS NOT NULL THEN workflow_root_id END) VIRTUAL,
    -- Due-for-promotion scan.
    scheduled_due TIMESTAMP(9) GENERATED ALWAYS AS (
        CASE WHEN state = 'SCHEDULED' THEN scheduled_at END) VIRTUAL,
    -- Orphan recovery by the newer of owner heartbeat and check-in. COALESCE
    -- keeps GREATEST from returning NULL when only one marker exists.
    processing_liveness TIMESTAMP(9) GENERATED ALWAYS AS (
        CASE WHEN state = 'PROCESSING' THEN GREATEST(
            COALESCE(owner_heartbeat_at, last_checkin_at),
            COALESCE(last_checkin_at, owner_heartbeat_at)) END) VIRTUAL,
    -- Oldest processing heartbeat (monitoring).
    processing_heartbeat TIMESTAMP(9) GENERATED ALWAYS AS (
        CASE WHEN state = 'PROCESSING' THEN owner_heartbeat_at END) VIRTUAL,
    -- Workflow successor promotion: AWAITING jobs of one parent.
    awaiting_parent RAW(16) GENERATED ALWAYS AS (
        CASE WHEN state = 'AWAITING' THEN parent_job_id END) VIRTUAL,
    awaiting_at TIMESTAMP(9) GENERATED ALWAYS AS (
        CASE WHEN state = 'AWAITING' AND parent_job_id IS NOT NULL
            THEN current_state_at END) VIRTUAL,
    awaiting_id RAW(16) GENERATED ALWAYS AS (
        CASE WHEN state = 'AWAITING' AND parent_job_id IS NOT NULL THEN id END) VIRTUAL,
    -- Oldest ENQUEUED job per queue and idle-queue checks.
    enqueued_queue VARCHAR2(128 CHAR) GENERATED ALWAYS AS (
        CASE WHEN state = 'ENQUEUED' THEN queue END) VIRTUAL,
    enqueued_at TIMESTAMP(9) GENERATED ALWAYS AS (
        CASE WHEN state = 'ENQUEUED' THEN current_state_at END) VIRTUAL,
    CONSTRAINT threadmill_jobs_pk PRIMARY KEY (id),
    CONSTRAINT threadmill_jobs_state_ck CHECK (state IN ('AWAITING', 'SCHEDULED', 'ENQUEUED',
        'PROCESSING', 'SUCCEEDED', 'FAILED', 'DELETED', 'QUARANTINED', 'PROCESSED')),
    CONSTRAINT threadmill_jobs_mode_ck
        CHECK (concurrency_mode IS NULL OR concurrency_mode IN ('SHARED', 'EXCLUSIVE')),
    CONSTRAINT threadmill_jobs_shape_ck CHECK (
        (concurrency_key IS NULL AND concurrency_mode IS NULL)
        OR (concurrency_key IS NOT NULL AND concurrency_mode IS NOT NULL)),
    CONSTRAINT threadmill_jobs_version_ck CHECK (version >= 0),
    CONSTRAINT threadmill_jobs_exec_rev_ck CHECK (execution_revision >= 0)
)
/

CREATE INDEX threadmill_jobs_unkeyed_idx
    ON threadmill_jobs (unkeyed_queue, unkeyed_rank, unkeyed_id)
/

CREATE INDEX threadmill_jobs_keyed_idx
    ON threadmill_jobs (keyed_queue, keyed_key, keyed_at, keyed_id)
/

CREATE INDEX threadmill_jobs_pending_idx
    ON threadmill_jobs (pending_key, pending_at, pending_id)
/

CREATE INDEX threadmill_jobs_exclusive_idx
    ON threadmill_jobs (exclusive_key, exclusive_at, exclusive_id)
/

CREATE INDEX threadmill_jobs_workflow_idx
    ON threadmill_jobs (outstanding_key, outstanding_root)
/

CREATE INDEX threadmill_jobs_scheduled_idx ON threadmill_jobs (scheduled_due)
/

CREATE INDEX threadmill_jobs_liveness_idx ON threadmill_jobs (processing_liveness)
/

CREATE INDEX threadmill_jobs_heartbeat_idx ON threadmill_jobs (processing_heartbeat)
/

CREATE INDEX threadmill_jobs_awaiting_idx
    ON threadmill_jobs (awaiting_parent, awaiting_at, awaiting_id)
/

CREATE INDEX threadmill_jobs_queue_age_idx ON threadmill_jobs (enqueued_queue, enqueued_at)
/

-- Retention keyset pages by (current_state_at, id) within one finished state.
CREATE INDEX threadmill_jobs_retention_idx
    ON threadmill_jobs (state, current_state_at, id)
/

-- Stable maintenance keyset pages by id within one state.
CREATE INDEX threadmill_jobs_state_id_idx ON threadmill_jobs (state, id)
/

-- Dashboard history pages, newest transition first.
CREATE INDEX threadmill_jobs_state_page_idx
    ON threadmill_jobs (state, current_state_at DESC, id)
/

-- Dashboard search filtered by state, queue, and handler.
CREATE INDEX threadmill_jobs_search_idx
    ON threadmill_jobs (state, queue, handler_signature, current_state_at DESC, id)
/

CREATE INDEX threadmill_jobs_handler_idx ON threadmill_jobs (handler_signature)
/

CREATE TABLE threadmill_nodes (
    id RAW(16) NOT NULL,
    last_heartbeat_at TIMESTAMP(9) NOT NULL,
    CONSTRAINT threadmill_nodes_pk PRIMARY KEY (id)
)
/

-- Recurring task identity. payload_serialized is nullable only because Oracle
-- stores an empty string as NULL; the store reads NULL back as "".
CREATE TABLE threadmill_cron_tasks (
    name VARCHAR2(128 CHAR) NOT NULL,
    trigger_kind VARCHAR2(16) NOT NULL,
    trigger_value VARCHAR2(1000 CHAR) NOT NULL,
    handler_signature VARCHAR2(1000 CHAR) NOT NULL,
    payload_type_tag VARCHAR2(1000 CHAR) NOT NULL,
    payload_serialized CLOB,
    queue VARCHAR2(128 CHAR) DEFAULT 'default' NOT NULL,
    priority NUMBER(10) DEFAULT 0 NOT NULL,
    timeout_seconds NUMBER(19),
    max_attempts NUMBER(10),
    is_exclusive NUMBER(1) DEFAULT 0 NOT NULL,
    missed_run_policy VARCHAR2(16) DEFAULT 'DROP' NOT NULL,
    time_zone VARCHAR2(64 CHAR) DEFAULT 'UTC' NOT NULL,
    enabled NUMBER(1) DEFAULT 1 NOT NULL,
    CONSTRAINT threadmill_cron_tasks_pk PRIMARY KEY (name),
    CONSTRAINT threadmill_cron_kind_ck CHECK (trigger_kind IN ('CRON', 'INTERVAL')),
    CONSTRAINT threadmill_cron_policy_ck CHECK (missed_run_policy IN ('DROP', 'CATCH_UP')),
    CONSTRAINT threadmill_cron_timeout_ck CHECK (timeout_seconds IS NULL OR timeout_seconds > 0),
    CONSTRAINT threadmill_cron_attempts_ck CHECK (max_attempts IS NULL OR max_attempts > 0),
    CONSTRAINT threadmill_cron_exclusive_ck CHECK (is_exclusive IN (0, 1)),
    CONSTRAINT threadmill_cron_enabled_ck CHECK (enabled IN (0, 1))
)
/

-- Recurring schedule state. The nudge columns are written only by
-- requestCronNudge / clearCronNudge; the blanket state upsert leaves them
-- untouched so re-registration cannot clobber a concurrently accepted nudge.
CREATE TABLE threadmill_cron_task_state (
    task_name VARCHAR2(128 CHAR) NOT NULL,
    last_run_at TIMESTAMP(9),
    last_run_job_id RAW(16),
    next_run_at TIMESTAMP(9),
    in_flight_job_id RAW(16),
    timing_fingerprint VARCHAR2(4000 BYTE),
    nudge_requested_at TIMESTAMP(9),
    nudge_revision NUMBER(19),
    CONSTRAINT threadmill_cron_state_pk PRIMARY KEY (task_name),
    CONSTRAINT threadmill_cron_state_task_fk FOREIGN KEY (task_name)
        REFERENCES threadmill_cron_tasks (name) ON DELETE CASCADE
)
/

CREATE INDEX threadmill_cron_state_due_idx ON threadmill_cron_task_state (next_run_at)
/

CREATE TABLE threadmill_cron_task_ownership (
    namespace VARCHAR2(128 CHAR) NOT NULL,
    task_name VARCHAR2(128 CHAR) NOT NULL,
    CONSTRAINT threadmill_cron_owner_pk PRIMARY KEY (namespace, task_name),
    CONSTRAINT threadmill_cron_owner_task_fk FOREIGN KEY (task_name)
        REFERENCES threadmill_cron_tasks (name) ON DELETE CASCADE
)
/

CREATE INDEX threadmill_cron_owner_task_idx ON threadmill_cron_task_ownership (task_name)
/

-- Cross-cluster named mutex with a lease. holder is nullable only because
-- Oracle stores an empty string as NULL; comparisons use DECODE, which treats
-- two NULLs as equal.
CREATE TABLE threadmill_mutexes (
    name VARCHAR2(128 CHAR) NOT NULL,
    holder VARCHAR2(4000 BYTE),
    expires_at TIMESTAMP(9) NOT NULL,
    CONSTRAINT threadmill_mutexes_pk PRIMARY KEY (name)
)
/

-- Store-backed leadership lease for the maintenance cycle.
CREATE TABLE threadmill_leases (
    name VARCHAR2(64) NOT NULL,
    holder RAW(16) NOT NULL,
    expires_at TIMESTAMP(9) NOT NULL,
    CONSTRAINT threadmill_leases_pk PRIMARY KEY (name)
)
/

-- Producer-side deduplication. Cleanup is gated on the referenced job being
-- terminal so a long-running active job keeps its dedup protection.
CREATE TABLE threadmill_dedup_keys (
    queue VARCHAR2(128 CHAR) NOT NULL,
    dedup_key VARCHAR2(256 BYTE) NOT NULL,
    job_id RAW(16) NOT NULL,
    expires_at TIMESTAMP(9) NOT NULL,
    CONSTRAINT threadmill_dedup_keys_pk PRIMARY KEY (queue, dedup_key),
    CONSTRAINT threadmill_dedup_keys_job_fk FOREIGN KEY (job_id)
        REFERENCES threadmill_jobs (id) ON DELETE CASCADE
)
/

CREATE INDEX threadmill_dedup_keys_exp_idx ON threadmill_dedup_keys (expires_at)
/

-- Without this index every retention delete scans the dedup table for the
-- cascade check.
CREATE INDEX threadmill_dedup_keys_job_idx ON threadmill_dedup_keys (job_id)
/

-- Claim-time per-key concurrency bookkeeping, updated in the same transaction
-- as the job state transition.
CREATE TABLE threadmill_concurrency_groups (
    concurrency_key VARCHAR2(256 BYTE) NOT NULL,
    exclusive_in_flight NUMBER(10) DEFAULT 0 NOT NULL,
    shared_in_flight NUMBER(10) DEFAULT 0 NOT NULL,
    last_modified TIMESTAMP(9) NOT NULL,
    -- Reclaimable groups (no job in flight) in binary key order. A RAW sort key
    -- orders identically in every session, whatever its NLS_SORT; ordering the
    -- text column would follow a linguistic NLS_SORT (derived from the client
    -- JVM locale) and could neither use the index nor match keyset predicates.
    idle_sort RAW(2000) GENERATED ALWAYS AS (
        CASE WHEN exclusive_in_flight = 0 AND shared_in_flight = 0
            THEN NLSSORT(concurrency_key, 'NLS_SORT = BINARY') END) VIRTUAL,
    CONSTRAINT threadmill_concurrency_pk PRIMARY KEY (concurrency_key),
    CONSTRAINT threadmill_concurrency_ex_ck
        CHECK (exclusive_in_flight >= 0 AND exclusive_in_flight <= 1),
    CONSTRAINT threadmill_concurrency_sh_ck CHECK (shared_in_flight >= 0),
    CONSTRAINT threadmill_concurrency_mode_ck
        CHECK (exclusive_in_flight = 0 OR shared_in_flight = 0)
)
/

-- Bounded keyset pages over reclaimable groups.
CREATE INDEX threadmill_concurrency_idle_idx ON threadmill_concurrency_groups (idle_sort)
/

-- Workflow-root outstanding counts: the key stays held from the root's claim
-- to the last descendant's terminal save.
CREATE TABLE threadmill_concurrency_workflow_holds (
    concurrency_key VARCHAR2(256 BYTE) NOT NULL,
    workflow_root_id RAW(16) NOT NULL,
    outstanding NUMBER(10) NOT NULL,
    CONSTRAINT threadmill_workflow_holds_pk PRIMARY KEY (concurrency_key, workflow_root_id),
    CONSTRAINT threadmill_workflow_holds_ck CHECK (outstanding >= 0)
)
/

-- Per-queue pause primitive.
CREATE TABLE threadmill_queue_pauses (
    queue VARCHAR2(128 CHAR) NOT NULL,
    paused_at TIMESTAMP(9) NOT NULL,
    paused_by CLOB,
    CONSTRAINT threadmill_queue_pauses_pk PRIMARY KEY (queue)
)
/

-- Incrementally maintained per-state counts, sharded 16 ways by session so
-- concurrent writers update disjoint rows. Only the SUM per state is
-- meaningful; individual shards may go negative. Never COUNT(*) the jobs table.
CREATE TABLE threadmill_job_counts (
    state VARCHAR2(16) NOT NULL,
    shard_no NUMBER(2) NOT NULL,
    job_count NUMBER(19) DEFAULT 0 NOT NULL,
    CONSTRAINT threadmill_job_counts_pk PRIMARY KEY (state, shard_no)
)
/

INSERT INTO threadmill_job_counts (state, shard_no, job_count)
SELECT s.state, sh.shard_no, 0
FROM (SELECT 'AWAITING' AS state FROM dual
      UNION ALL SELECT 'SCHEDULED' FROM dual
      UNION ALL SELECT 'ENQUEUED' FROM dual
      UNION ALL SELECT 'PROCESSING' FROM dual
      UNION ALL SELECT 'PROCESSED' FROM dual
      UNION ALL SELECT 'SUCCEEDED' FROM dual
      UNION ALL SELECT 'FAILED' FROM dual
      UNION ALL SELECT 'DELETED' FROM dual
      UNION ALL SELECT 'QUARANTINED' FROM dual) s
CROSS JOIN (SELECT LEVEL - 1 AS shard_no FROM dual CONNECT BY LEVEL <= 16) sh
/

-- Incrementally maintained ENQUEUED depth per queue, sharded the same way.
-- Monitoring work scales with queue cardinality, not queued job count.
CREATE TABLE threadmill_queue_counts (
    queue VARCHAR2(128 CHAR) NOT NULL,
    shard_no NUMBER(2) NOT NULL,
    job_count NUMBER(19) NOT NULL,
    CONSTRAINT threadmill_queue_counts_pk PRIMARY KEY (queue, shard_no)
)
/

-- Maintains both counter tables row by row. The shard is the session id
-- modulo 16, the Oracle analogue of PostgreSQL's backend pid.
CREATE OR REPLACE TRIGGER threadmill_jobs_counts_trg
AFTER INSERT OR DELETE OR UPDATE OF state, queue ON threadmill_jobs
FOR EACH ROW
DECLARE
    sh PLS_INTEGER := MOD(TO_NUMBER(SYS_CONTEXT('USERENV', 'SID')), 16);
    old_queue threadmill_jobs.queue%TYPE;
    new_queue threadmill_jobs.queue%TYPE;

    PROCEDURE adjust_state(st VARCHAR2, delta PLS_INTEGER) IS
    BEGIN
        UPDATE threadmill_job_counts SET job_count = job_count + delta
        WHERE state = st AND shard_no = sh;
    END;

    PROCEDURE adjust_queue(q VARCHAR2, delta PLS_INTEGER) IS
    BEGIN
        UPDATE threadmill_queue_counts SET job_count = job_count + delta
        WHERE queue = q AND shard_no = sh;
        IF SQL%ROWCOUNT = 0 THEN
            BEGIN
                INSERT INTO threadmill_queue_counts (queue, shard_no, job_count)
                VALUES (q, sh, delta);
            EXCEPTION
                WHEN DUP_VAL_ON_INDEX THEN
                    UPDATE threadmill_queue_counts SET job_count = job_count + delta
                    WHERE queue = q AND shard_no = sh;
            END;
        END IF;
    END;
BEGIN
    IF INSERTING THEN
        adjust_state(:NEW.state, 1);
    ELSIF DELETING THEN
        adjust_state(:OLD.state, -1);
    ELSIF :OLD.state <> :NEW.state THEN
        adjust_state(:OLD.state, -1);
        adjust_state(:NEW.state, 1);
    END IF;

    IF NOT INSERTING AND :OLD.state = 'ENQUEUED' THEN
        old_queue := :OLD.queue;
    END IF;
    IF NOT DELETING AND :NEW.state = 'ENQUEUED' THEN
        new_queue := :NEW.queue;
    END IF;
    IF old_queue IS NOT NULL AND new_queue IS NOT NULL AND old_queue = new_queue THEN
        RETURN;
    END IF;
    -- Stable order for queue replacements reduces cross-queue lock inversions.
    IF old_queue IS NOT NULL AND new_queue IS NOT NULL AND new_queue < old_queue THEN
        adjust_queue(new_queue, 1);
        adjust_queue(old_queue, -1);
    ELSE
        IF old_queue IS NOT NULL THEN
            adjust_queue(old_queue, -1);
        END IF;
        IF new_queue IS NOT NULL THEN
            adjust_queue(new_queue, 1);
        END IF;
    END IF;
END;
/
