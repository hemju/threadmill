-- Monitoring work scales with queue cardinality, not queued job count.
CREATE TABLE threadmill_queue_counts (
    queue TEXT NOT NULL,
    shard INT NOT NULL,
    count BIGINT NOT NULL,
    PRIMARY KEY (queue, shard)
);

-- Migration runs with writers stopped; the migration transaction locks the jobs
-- table while installing the trigger and bootstrapping its exact counters.
LOCK TABLE threadmill_jobs IN SHARE ROW EXCLUSIVE MODE;
INSERT INTO threadmill_queue_counts (queue, shard, count)
SELECT queue, 0, count(*) FROM threadmill_jobs WHERE state = 'ENQUEUED' GROUP BY queue;

CREATE FUNCTION threadmill_adjust_queue_count(q TEXT, delta BIGINT) RETURNS VOID AS $$
BEGIN
    INSERT INTO threadmill_queue_counts(queue, shard, count)
    VALUES (q, pg_backend_pid() % 16, delta)
    ON CONFLICT (queue, shard) DO UPDATE SET count = threadmill_queue_counts.count + delta;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION threadmill_maintain_queue_counts() RETURNS TRIGGER AS $$
DECLARE
    old_queue TEXT;
    new_queue TEXT;
BEGIN
    IF TG_OP <> 'INSERT' AND OLD.state = 'ENQUEUED' THEN old_queue := OLD.queue; END IF;
    IF TG_OP <> 'DELETE' AND NEW.state = 'ENQUEUED' THEN new_queue := NEW.queue; END IF;
    IF old_queue IS NOT DISTINCT FROM new_queue THEN RETURN NULL; END IF;
    -- Stable order for queue replacements reduces cross-queue lock inversions.
    IF old_queue IS NOT NULL AND new_queue IS NOT NULL AND new_queue < old_queue THEN
        PERFORM threadmill_adjust_queue_count(new_queue, 1);
        PERFORM threadmill_adjust_queue_count(old_queue, -1);
    ELSE
        IF old_queue IS NOT NULL THEN PERFORM threadmill_adjust_queue_count(old_queue, -1); END IF;
        IF new_queue IS NOT NULL THEN PERFORM threadmill_adjust_queue_count(new_queue, 1); END IF;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER threadmill_jobs_queue_counts_trigger
AFTER INSERT OR UPDATE OF state, queue OR DELETE ON threadmill_jobs
FOR EACH ROW EXECUTE FUNCTION threadmill_maintain_queue_counts();

CREATE INDEX threadmill_jobs_queue_age_idx
    ON threadmill_jobs(queue, current_state_at)
    WHERE state = 'ENQUEUED';

-- The dashboard's state-only history page must also avoid sorting the whole
-- state population, including many jobs with identical transition timestamps.
CREATE INDEX threadmill_jobs_state_page_idx
    ON threadmill_jobs(state, current_state_at DESC, id);
