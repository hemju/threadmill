-- Execution diagnostics use their own optimistic revision; state versions remain unchanged.
ALTER TABLE threadmill_jobs ADD COLUMN execution_revision bigint NOT NULL DEFAULT 0
    CHECK (execution_revision >= 0);
