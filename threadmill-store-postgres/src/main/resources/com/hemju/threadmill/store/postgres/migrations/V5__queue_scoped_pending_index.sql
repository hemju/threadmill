-- Queue-scoped candidate index for the claim path's keyed lane.
--
-- Claims are per-queue, but the concurrency pending index leads with
-- concurrency_key alone: a per-key head probe filtered by queue walks the
-- key's ENTIRE pending range when the key's jobs live in other queues —
-- measured 1.4s per claim call at a 415k backlog with 20 keys, which kept
-- claim cost linear in backlog depth even after candidate gathering became
-- key-driven. Leading with (queue, concurrency_key) makes both the per-queue
-- key enumeration (recursive loose scan, 3ms) and each key's head probe
-- (0.6ms) independent of backlog depth on the same data.

CREATE INDEX threadmill_jobs_queue_pending_idx
    ON threadmill_jobs (queue, concurrency_key, current_state_at, id)
    WHERE state = 'ENQUEUED';
