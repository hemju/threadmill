-- Reject scalar corruption at the database boundary. The serialized body remains
-- authoritative, but invalid indexed state must not enter scheduling queries or
-- concurrency bookkeeping through manual SQL or a faulty integration.

ALTER TABLE threadmill_jobs
    ADD CONSTRAINT threadmill_jobs_state_check
        CHECK (state IN ('AWAITING', 'SCHEDULED', 'ENQUEUED', 'PROCESSING', 'SUCCEEDED',
                         'FAILED', 'DELETED', 'QUARANTINED', 'PROCESSED')),
    ADD CONSTRAINT threadmill_jobs_concurrency_mode_check
        CHECK (concurrency_mode IS NULL OR concurrency_mode IN ('SHARED', 'EXCLUSIVE')),
    ADD CONSTRAINT threadmill_jobs_concurrency_shape_check
        CHECK ((concurrency_key IS NULL) = (concurrency_mode IS NULL)),
    ADD CONSTRAINT threadmill_jobs_version_check
        CHECK (version >= 0);

ALTER TABLE threadmill_cron_tasks
    ADD CONSTRAINT threadmill_cron_tasks_trigger_kind_check
        CHECK (trigger_kind IN ('CRON', 'INTERVAL')),
    ADD CONSTRAINT threadmill_cron_tasks_missed_run_policy_check
        CHECK (missed_run_policy IN ('DROP', 'CATCH_UP')),
    ADD CONSTRAINT threadmill_cron_tasks_timeout_check
        CHECK (timeout_seconds IS NULL OR timeout_seconds > 0),
    ADD CONSTRAINT threadmill_cron_tasks_max_attempts_check
        CHECK (max_attempts IS NULL OR max_attempts > 0);

ALTER TABLE threadmill_concurrency_groups
    ADD CONSTRAINT threadmill_concurrency_groups_exclusive_check
        CHECK (exclusive_in_flight >= 0 AND exclusive_in_flight <= 1),
    ADD CONSTRAINT threadmill_concurrency_groups_shared_check
        CHECK (shared_in_flight >= 0),
    ADD CONSTRAINT threadmill_concurrency_groups_mode_check
        CHECK (exclusive_in_flight = 0 OR shared_in_flight = 0);

ALTER TABLE threadmill_concurrency_workflow_holds
    ADD CONSTRAINT threadmill_concurrency_workflow_holds_outstanding_check
        CHECK (outstanding >= 0);
