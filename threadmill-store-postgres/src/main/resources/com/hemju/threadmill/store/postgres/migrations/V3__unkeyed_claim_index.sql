-- Dedicated partial index for the claim path's unkeyed lane.
--
-- The claim candidate source is split by concurrency shape: keyed candidates
-- are driven per key from threadmill_jobs_concurrency_pending_idx, and
-- unkeyed candidates page over this index. Without it, finding unkeyed work
-- under a keyed-heavy backlog walks the shared enqueued index past every
-- keyed row — O(backlog) — which is exactly the claim-cost shape that made
-- consumption decay from ~39/s to ~3/s as a stress-run backlog grew to 371k
-- rows.

CREATE INDEX threadmill_jobs_unkeyed_enqueued_idx
    ON threadmill_jobs (queue, priority DESC, id)
    WHERE state = 'ENQUEUED' AND concurrency_key IS NULL;
