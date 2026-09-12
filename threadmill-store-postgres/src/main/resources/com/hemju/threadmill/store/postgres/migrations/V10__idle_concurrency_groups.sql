-- Bounded keyset pages over reclaimable-count candidates, independent of active groups.
CREATE INDEX threadmill_concurrency_idle_idx
    ON threadmill_concurrency_groups(concurrency_key)
    WHERE exclusive_in_flight=0 AND shared_in_flight=0;
