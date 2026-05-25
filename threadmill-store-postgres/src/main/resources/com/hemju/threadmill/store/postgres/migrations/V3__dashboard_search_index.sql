CREATE INDEX IF NOT EXISTS threadmill_jobs_dashboard_search_idx
    ON threadmill_jobs (state, queue, handler_signature, current_state_at DESC, id);
