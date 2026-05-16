-- Atomic, version-matched update of a job. Removes the job from its prior
-- active structure (if any) and adds it to the new one (if any), keeps the
-- by_state_time index and counts in sync, and rewrites the body. All in one
-- atomic execution.
--
-- KEYS:
--   [1] job hash
--   [2] new active-state key, or empty   (queue / scheduled / awaiting / processing_all)
--   [3] new active-state per-node key, or empty (processing per-node)
--   [4] new by_state_time
--   [5] old active-state key, or empty
--   [6] old active-state per-node key, or empty (processing per-node)
--   [7] old by_state_time
--   [8] counts hash
--   [9] old concurrency pending ZSET, or empty
--   [10] new concurrency pending ZSET, or empty
--   [11] old concurrency counters HASH, or empty
--   [12] old concurrency workflows HASH, or empty
--
-- ARGV:
--   [1] job id
--   [2] expected_version
--   [3] new_version
--   [4] new_body
--   [5] new_state
--   [6] new_queue
--   [7] new_priority
--   [8] new_scheduled_at (millis or '')
--   [9] new_owner_node_id ('' if none)
--   [10] new_owner_heartbeat_at (millis or '')
--   [11] new_last_checkin_at (millis or '')
--   [12] new_current_state_at (millis)
--   [13] new_active_score (numeric)
--   [14] old_state
--   [15] workflow_root_id
--   [16] concurrency_key
--   [17] concurrency_mode
--   [18] old_concurrency_key
--   [19] old_concurrency_mode
--   [20] old_workflow_root_id
--   [21] old_pending_member
--   [22] new_pending_member
--   [23] new_pending_score
--
-- Returns 'ok' on success, 'stale' on version mismatch, 'vanished' if the job
-- has been hard-deleted.

local job_key                = KEYS[1]
local new_active_key         = KEYS[2]
local new_active_node_key    = KEYS[3]
local new_state_time_key     = KEYS[4]
local old_active_key         = KEYS[5]
local old_active_node_key    = KEYS[6]
local old_state_time_key     = KEYS[7]
local counts_key             = KEYS[8]
local old_pending_key        = KEYS[9]
local new_pending_key        = KEYS[10]
local old_counters_key       = KEYS[11]
local old_workflows_key      = KEYS[12]

local job_id              = ARGV[1]
local expected_version    = tonumber(ARGV[2])
local new_version         = ARGV[3]
local new_body            = ARGV[4]
local new_state           = ARGV[5]
local new_queue           = ARGV[6]
local new_priority        = ARGV[7]
local new_scheduled_at    = ARGV[8]
local new_owner_node_id   = ARGV[9]
local new_owner_heartbeat = ARGV[10]
local new_last_checkin    = ARGV[11]
local new_state_time      = tonumber(ARGV[12])
local new_active_score    = tonumber(ARGV[13])
local old_state           = ARGV[14]
local workflow_root_id    = ARGV[15]
local concurrency_key     = ARGV[16]
local concurrency_mode    = ARGV[17]
local old_concurrency_key  = ARGV[18]
local old_concurrency_mode = ARGV[19]
local old_workflow_root_id = ARGV[20]
local old_pending_member   = ARGV[21]
local new_pending_member   = ARGV[22]
local new_pending_score    = tonumber(ARGV[23])

local function is_terminal(state)
    return state == 'SUCCEEDED' or state == 'FAILED' or state == 'DELETED' or state == 'QUARANTINED'
end

if redis.call('EXISTS', job_key) == 0 then
    return 'VANISHED'
end

local persisted_version = tonumber(redis.call('HGET', job_key, 'version'))
if persisted_version ~= expected_version then
    return 'STALE'
end

-- Remove from the prior active structure(s).
if old_active_key ~= '' then
    redis.call('ZREM', old_active_key, job_id)
end
if old_active_node_key ~= '' then
    redis.call('ZREM', old_active_node_key, job_id)
end
redis.call('ZREM', old_state_time_key, job_id)
if old_pending_key ~= '' and old_pending_member ~= '' then
    redis.call('ZREM', old_pending_key, old_pending_member)
end

-- Update counts only when the state actually changed.
if old_state ~= new_state then
    redis.call('HINCRBY', counts_key, old_state, -1)
    redis.call('HINCRBY', counts_key, new_state, 1)
end

if old_concurrency_key ~= '' and old_workflows_key ~= '' and old_counters_key ~= '' and
   (not is_terminal(old_state)) and is_terminal(new_state) then
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

-- Rewrite the hash.
redis.call('HSET', job_key,
    'body', new_body,
    'state', new_state,
    'queue', new_queue,
    'priority', new_priority,
    'scheduled_at', new_scheduled_at,
    'owner_node_id', new_owner_node_id,
    'owner_heartbeat_at', new_owner_heartbeat,
    'last_checkin_at', new_last_checkin,
    'current_state_at', tostring(new_state_time),
    'workflow_root_id', workflow_root_id,
    'concurrency_key', concurrency_key,
    'concurrency_mode', concurrency_mode,
    'version', new_version
)

-- Add to the new active structure(s).
if new_active_key ~= '' and new_active_score ~= nil then
    redis.call('ZADD', new_active_key, new_active_score, job_id)
end
if new_active_node_key ~= '' and new_active_score ~= nil then
    redis.call('ZADD', new_active_node_key, new_active_score, job_id)
end
if concurrency_key ~= '' and new_pending_key ~= '' and new_pending_member ~= '' and
   (new_state == 'ENQUEUED' or new_state == 'SCHEDULED' or new_state == 'AWAITING') then
    redis.call('ZADD', new_pending_key, new_pending_score, new_pending_member)
end
redis.call('ZADD', new_state_time_key, new_state_time, job_id)

return 'OK'
