-- Stable keyset maintenance pages cannot be displaced by earlier state transitions.
CREATE INDEX threadmill_jobs_state_id_idx ON threadmill_jobs (state, id);
