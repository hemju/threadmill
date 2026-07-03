-- Atomically transition a job into DELETED, regardless of its prior state,
-- so long as it isn't already there. Updates all structures and counts.
--
-- KEYS:
--   [1] job hash
--   [2] old active key, or empty
--   [3] old active per-node key, or empty
--   [4] old by_state_time
--   [5] new by_state_time (DELETED)
--   [6] counts hash
--   [7] old concurrency pending ZSET, or empty
--   [8] old concurrency counters HASH, or empty
--   [9] old concurrency workflows HASH, or empty
--   [10] old concurrency workflow counts HASH, or empty
--   [11] awaiting_by_parent SET, or empty
--   [12] old queue_keys HASH (key -> ENQUEUED count in old queue)
--   [13] old queue_unkeyed ZSET
--   [14] old concurrency pending_root ZSET, or empty
--
-- ARGV:
--   [1] job id
--   [2] new_body
--   [3] now_millis
--   [4] old_state
--   [5] old_concurrency_key
--   [6] old_concurrency_mode
--   [7] old_workflow_root_id
--   [8] old_pending_member
--   [9] expected_version (from the caller's read)
--
-- Returns 1 on success, 0 if already DELETED, -1 if vanished, -2 if the
-- live version no longer matches expected_version (caller must re-read).

local job_key            = KEYS[1]
local old_active_key     = KEYS[2]
local old_active_node    = KEYS[3]
local old_state_time_key = KEYS[4]
local new_state_time_key = KEYS[5]
local counts_key         = KEYS[6]
local old_pending_key    = KEYS[7]
local old_counters_key   = KEYS[8]
local old_workflows_key  = KEYS[9]
local old_workflow_counts_key = KEYS[10]
local awaiting_parent_key = KEYS[11]
local old_queue_keys_key = KEYS[12]
local old_unkeyed_key = KEYS[13]
local old_pending_root_key = KEYS[14]

local job_id   = ARGV[1]
local new_body = ARGV[2]
local now_ms   = tonumber(ARGV[3])
local old_state = ARGV[4]
local old_concurrency_key = ARGV[5]
local old_concurrency_mode = ARGV[6]
local old_workflow_root_id = ARGV[7]
local old_pending_member = ARGV[8]
local expected_version = tonumber(ARGV[9])

local function is_terminal(state)
    return state == 'SUCCEEDED' or state == 'FAILED' or state == 'DELETED' or state == 'QUARANTINED'
end

if redis.call('EXISTS', job_key) == 0 then
    return -1
end

local current_state = redis.call('HGET', job_key, 'state')
if current_state == 'DELETED' then
    return 0
end

local old_version = tonumber(redis.call('HGET', job_key, 'version'))
if old_version ~= expected_version then
    -- A state transition (e.g. a claim) landed between the caller's HGETALL
    -- and this script: committing against the stale read would decrement the
    -- wrong state count, strand index entries, and release live concurrency
    -- holds. The caller re-reads and retries.
    return -2
end
local new_version = old_version + 1

if old_active_key ~= '' then
    redis.call('ZREM', old_active_key, job_id)
end
if old_active_node ~= '' then
    redis.call('ZREM', old_active_node, job_id)
end
redis.call('ZREM', old_state_time_key, job_id)
if old_pending_key ~= '' and old_pending_member ~= '' then
    redis.call('ZREM', old_pending_key, old_pending_member)
    if old_pending_root_key ~= '' then
        redis.call('ZREM', old_pending_root_key, old_pending_member)
    end
end
if old_state == 'ENQUEUED' then
    if old_concurrency_key ~= '' then
        local remaining = redis.call('HINCRBY', old_queue_keys_key, old_concurrency_key, -1)
        if remaining <= 0 then
            redis.call('HDEL', old_queue_keys_key, old_concurrency_key)
        end
    else
        redis.call('ZREM', old_unkeyed_key, job_id)
    end
end
if awaiting_parent_key ~= '' and old_state == 'AWAITING' then
    redis.call('SREM', awaiting_parent_key, job_id)
end
redis.call('HINCRBY', counts_key, old_state, -1)
redis.call('HINCRBY', counts_key, 'DELETED', 1)

if old_concurrency_key ~= '' and old_workflows_key ~= '' and old_counters_key ~= '' and not is_terminal(old_state) then
    if old_workflow_counts_key ~= '' then
        local workflow_count = redis.call('HINCRBY', old_workflow_counts_key, old_workflow_root_id, -1)
        if workflow_count <= 0 then
            redis.call('HDEL', old_workflow_counts_key, old_workflow_root_id)
        end
    end
    -- Release the hold share only when a hold actually exists for this root.
    -- A never-claimed standalone keyed job has no hold entry; the insert path
    -- guards its increment on hold existence, so an unguarded decrement here
    -- would fabricate a -1 entry and then wrongly decrement the in-flight
    -- counter other roots on this key rely on for admission.
    if redis.call('HGET', old_workflows_key, old_workflow_root_id) ~= false then
        local outstanding = redis.call('HINCRBY', old_workflows_key, old_workflow_root_id, -1)
        if outstanding <= 0 then
            redis.call('HDEL', old_workflows_key, old_workflow_root_id)
            local field = 'shared_in_flight'
            if old_concurrency_mode == 'EXCLUSIVE' then
                field = 'exclusive_in_flight'
            end
            local next_count = redis.call('HINCRBY', old_counters_key, field, -1)
            if next_count < 0 then
                redis.call('HSET', old_counters_key, field, '0')
            end
        end
    end
end

redis.call('HSET', job_key,
    'state', 'DELETED',
    'current_state_at', tostring(now_ms),
    'version', tostring(new_version),
    'body', new_body
)
redis.call('ZADD', new_state_time_key, now_ms, job_id)
return 1
