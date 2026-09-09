-- Cutoff-eligible keyset retention requires ascending time AND id, including
-- large equal-time groups. The dashboard's mixed-direction V9 index serves a
-- different order; the old two-column state/time index is now redundant.
CREATE INDEX threadmill_jobs_retention_idx
    ON threadmill_jobs(state, current_state_at, id);
DROP INDEX threadmill_jobs_state_time_idx;
