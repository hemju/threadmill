-- Head-probe index for the claim path's earliest-pending-EXCLUSIVE lookup.
--
-- Admission needs, per candidate key, the earliest pending job and the
-- earliest pending EXCLUSIVE (the leapfrog rule). The historical
-- DISTINCT ON lookup had no per-group early termination, so every claim
-- pass scanned the whole pending population — 130ms at a 415k-row backlog,
-- twice per pass, which is what kept claim cost linear in backlog depth
-- even after the candidate gathering was made key-driven. The lookups are
-- now per-key LATERAL head probes: the plain probe rides
-- threadmill_jobs_concurrency_pending_idx; this index gives the
-- EXCLUSIVE-only probe the same O(1) head access even on keys with few or
-- no EXCLUSIVE members (0.7ms for 20 keys at the same backlog, index-only).

CREATE INDEX threadmill_jobs_exclusive_pending_idx
    ON threadmill_jobs (concurrency_key, current_state_at, id)
    WHERE state IN ('ENQUEUED', 'SCHEDULED', 'AWAITING') AND concurrency_mode = 'EXCLUSIVE';
