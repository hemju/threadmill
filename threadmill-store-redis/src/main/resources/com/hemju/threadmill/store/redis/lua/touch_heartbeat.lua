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

local ids = redis.call('ZRANGE', node_key, 0, -1)
local count = 0
for _, id in ipairs(ids) do
    redis.call('ZADD', node_key, hb_ms, id)
    redis.call('ZADD', all_key, hb_ms, id)
    local job_key = prefix .. id
    if redis.call('EXISTS', job_key) == 1 then
        redis.call('HSET', job_key, 'owner_heartbeat_at', tostring(hb_ms))
    end
    count = count + 1
end
return count
