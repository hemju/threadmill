-- Atomic per-job retention hard-delete. Re-checks the state inside the
-- script so a job that legally left the terminal state between the scan and
-- the delete (e.g. dashboard SUCCEEDED -> DELETED) is skipped, and the DEL,
-- index removals, and count decrement land together — a crash can no longer
-- permanently overstate the per-state count.
--
-- KEYS:
--   [1] job hash
--   [2] by_state_time ZSET for the expected state
--   [3] counts hash
--   [4] by_handler SET observed during the scan
--
-- ARGV:
--   [1] job id
--   [2] expected state
--   [3] now (epoch millis)
--
-- Returns 1 if the job was deleted, 0 if it was skipped.

local now_ms = tonumber(ARGV[3])

local state = redis.call('HGET', KEYS[1], 'state')
if state == false then
    -- Hash already gone: clean the dangling index entry only.
    redis.call('ZREM', KEYS[2], ARGV[1])
    return 0
end
if state ~= ARGV[2] then
    return 0
end
-- Keep a job whose dedup key is still unexpired: deleting it would end the
-- producer-dedup window early. The dedup_key is discovered from the job hash
-- (a deliberate non-KEYS access; safe under the single {threadmill} slot).
local dedup_key = redis.call('HGET', KEYS[1], 'dedup')
if dedup_key and dedup_key ~= false then
    local exp = tonumber(redis.call('HGET', dedup_key, 'expires_at'))
    if exp ~= nil and now_ms ~= nil and exp > now_ms then
        return 0
    end
end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
if KEYS[4] ~= '' then
    redis.call('SREM', KEYS[4], ARGV[1])
end
redis.call('HINCRBY', KEYS[3], ARGV[2], -1)
return 1
