-- Update the heartbeat for every PROCESSING job owned by a given node.
-- Atomically rescores the per-node and global processing ZSETs and writes
-- the new heartbeat to each job hash's scalar.
--
-- KEYS:
--   [1] processing_for_node ZSET (threadmill:processing:{nodeId})
--   [2] processing_all ZSET      (threadmill:processing)
--
-- ARGV:
--   [1] heartbeat_at_millis
--   [2] job_key_prefix
--
-- Returns the number of jobs whose heartbeat was advanced.

local node_key = KEYS[1]
local all_key  = KEYS[2]
local hb_ms    = tonumber(ARGV[1])
local prefix   = ARGV[2]

-- Deliberate non-KEYS access: the per-job keys are discovered from the
-- per-node ZSET inside the script, so they cannot be passed in KEYS. Safe
-- because every engine key carries the {threadmill} hash tag and therefore
-- lives in one cluster slot (pinned by RedisKeysTest.allEngineKeysUseOneClusterSlot).
local ids = redis.call('ZRANGE', node_key, 0, -1)
local count = 0
for _, id in ipairs(ids) do
    local job_key = prefix .. id
    if redis.call('EXISTS', job_key) == 1 then
        redis.call('ZADD', node_key, hb_ms, id)
        redis.call('ZADD', all_key, hb_ms, id)
        redis.call('HSET', job_key, 'owner_heartbeat_at', tostring(hb_ms))
        count = count + 1
    else
        -- A dangling id (partial deletion, manual intervention) must not be
        -- resurrected with a fresh heartbeat forever: once its node stops,
        -- it would permanently occupy the lowest-score window of the orphan
        -- scan. Drop it from both indexes so the structure self-heals.
        redis.call('ZREM', node_key, id)
        redis.call('ZREM', all_key, id)
    end
end
return count
