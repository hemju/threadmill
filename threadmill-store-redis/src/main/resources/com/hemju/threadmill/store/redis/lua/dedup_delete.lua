-- Compare-and-delete a dedup record (mirrors the mutex release pattern):
-- delete only if the record still references the job id observed during the
-- maintenance scan. A concurrent enqueue_if_absent that atomically replaced
-- the record with a fresh entry must not lose it to the sweeper.
--
-- KEYS:
--   [1] dedup record HASH
--   [2] dedup expiry ZSET
--
-- ARGV:
--   [1] expected job id ('' if the record hash was already gone)
--
-- Returns 1 if the record (or its orphaned expiry entry) was removed,
-- 0 if the record changed since the scan.

local current = redis.call('HGET', KEYS[1], 'job_id')
if current == false then
    if ARGV[1] == '' then
        redis.call('ZREM', KEYS[2], KEYS[1])
        return 1
    end
    -- The record vanished after the scan; leave the expiry entry to the
    -- next sweep, which will observe the absence consistently.
    return 0
end
if current == ARGV[1] then
    redis.call('DEL', KEYS[1])
    redis.call('ZREM', KEYS[2], KEYS[1])
    return 1
end
return 0
