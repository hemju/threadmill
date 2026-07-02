-- Shard the per-state job counters.
--
-- V1 kept ONE row per state, maintained by the AFTER trigger on
-- threadmill_jobs. That row is touched inside every insert, every claim, and
-- every terminal save — so under concurrent producers all writers serialize
-- on the same row lock (and claims, whose transactions are long, hold it for
-- their full duration). A 16-producer stress run collapsed total throughput
-- to ~13 jobs/s with pg_stat_activity showing hundreds of
-- Lock:transactionid waits on the counter updates.
--
-- Each state now has 16 shard rows; a session updates the shard picked by
-- its backend pid, so concurrent connections touch disjoint rows. Reads SUM
-- over the shards (144 tiny rows — still never a scan of threadmill_jobs).
-- Individual shard rows may go negative (a job inserted on one connection
-- and completed on another decrements a different shard); only the SUM is
-- meaningful.

ALTER TABLE threadmill_job_counts ADD COLUMN shard INT NOT NULL DEFAULT 0;
ALTER TABLE threadmill_job_counts DROP CONSTRAINT threadmill_job_counts_pkey;
ALTER TABLE threadmill_job_counts ADD PRIMARY KEY (state, shard);

-- Existing totals stay in shard 0; add the remaining 15 shards per state.
INSERT INTO threadmill_job_counts (state, shard, count)
SELECT s.state, sh, 0
FROM (SELECT DISTINCT state FROM threadmill_job_counts) AS s(state),
     generate_series(1, 15) AS sh;

CREATE OR REPLACE FUNCTION threadmill_maintain_counts() RETURNS TRIGGER AS $$
DECLARE
    sh INT := pg_backend_pid() % 16;
BEGIN
    IF (TG_OP = 'INSERT') THEN
        UPDATE threadmill_job_counts SET count = count + 1 WHERE state = NEW.state AND shard = sh;
    ELSIF (TG_OP = 'DELETE') THEN
        UPDATE threadmill_job_counts SET count = count - 1 WHERE state = OLD.state AND shard = sh;
    ELSIF (TG_OP = 'UPDATE' AND OLD.state IS DISTINCT FROM NEW.state) THEN
        UPDATE threadmill_job_counts SET count = count - 1 WHERE state = OLD.state AND shard = sh;
        UPDATE threadmill_job_counts SET count = count + 1 WHERE state = NEW.state AND shard = sh;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
