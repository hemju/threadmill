-- Atomically replace a non-running job's spec/scalars + body, contingent on
-- the current version and a replaceable state (ENQUEUED / SCHEDULED / AWAITING).
--
-- KEYS:
--   [1] job hash
--   [2] new active key, or empty            (queue / scheduled / awaiting)
--   [3] old active key, or empty            (queue / scheduled / awaiting)
--   [4] new by_state_time                   (always the same state — only updated for the score)
--   [5] old concurrency pending ZSET, or empty
--   [6] new concurrency pending ZSET, or empty
--   [7] old by_handler SET
--   [8] new by_handler SET
--   [9] queue registry SET
--   [10] old queue_keys HASH (key -> ENQUEUED count in old queue)
--   [11] new queue_keys HASH
--   [12] old queue_unkeyed ZSET
--   [13] new queue_unkeyed ZSET
--   [14] concurrency pending_root ZSET, or empty (key/mode/root are preserved)
--
-- ARGV:
--   [1] job id (string member of zsets)
--   [2] expected version
--   [3] new version (string)
--   [4] new body
--   [5] new queue
--   [6] new priority (string int)
--   [7] new handler_signature
--   [8] new scheduled_at (millis or empty)
--   [9] new current_state_at (millis)
--   [10] new active_score (numeric)
--   [11] workflow_root_id
--   [12] concurrency_key
--   [13] concurrency_mode
--   [14] old_pending_member
--   [15] new_pending_member
--   [16] new_pending_score
--
-- Returns 'OK', 'STALE' (version mismatch), 'WRONG_STATE', or 'VANISHED'.

local job_key       = KEYS[1]
local new_active    = KEYS[2]
local old_active    = KEYS[3]
local state_time_k  = KEYS[4]
local old_pending_k = KEYS[5]
local new_pending_k = KEYS[6]
local old_handler_k = KEYS[7]
local new_handler_k = KEYS[8]
local queues_key    = KEYS[9]
local old_queue_keys_key = KEYS[10]
local new_queue_keys_key = KEYS[11]
local old_unkeyed_key    = KEYS[12]
local new_unkeyed_key    = KEYS[13]
local pending_root_key   = KEYS[14]

local job_id        = ARGV[1]
local expected_ver  = tonumber(ARGV[2])
local new_ver       = ARGV[3]
local new_body      = ARGV[4]
local new_queue     = ARGV[5]
local new_priority  = ARGV[6]
local new_handler   = ARGV[7]
local new_sched     = ARGV[8]
local new_state_at  = tonumber(ARGV[9])
local new_score     = tonumber(ARGV[10])
local workflow_root_id = ARGV[11]
local concurrency_key  = ARGV[12]
local concurrency_mode = ARGV[13]
local old_pending_member = ARGV[14]
local new_pending_member = ARGV[15]
local new_pending_score = tonumber(ARGV[16])

if redis.call('EXISTS', job_key) == 0 then
    return 'VANISHED'
end
local persisted = tonumber(redis.call('HGET', job_key, 'version'))
if persisted ~= expected_ver then
    return 'STALE'
end
local state = redis.call('HGET', job_key, 'state')
if state ~= 'ENQUEUED' and state ~= 'SCHEDULED' and state ~= 'AWAITING' then
    return 'WRONG_STATE'
end

-- Move within the active structure if it changed (e.g. queue rename).
if old_active ~= '' then
    redis.call('ZREM', old_active, job_id)
end
if new_active ~= '' and new_score ~= nil then
    redis.call('ZADD', new_active, new_score, job_id)
end
if old_pending_k ~= '' and old_pending_member ~= '' then
    redis.call('ZREM', old_pending_k, old_pending_member)
    if pending_root_key ~= '' then
        redis.call('ZREM', pending_root_key, old_pending_member)
    end
end
if concurrency_key ~= '' and new_pending_k ~= '' and new_pending_member ~= '' then
    redis.call('ZADD', new_pending_k, new_pending_score, new_pending_member)
    if pending_root_key ~= '' then
        redis.call('ZADD', pending_root_key, new_pending_score, new_pending_member)
    end
end
if state == 'ENQUEUED' then
    if concurrency_key ~= '' then
        local remaining = redis.call('HINCRBY', old_queue_keys_key, concurrency_key, -1)
        if remaining <= 0 then
            redis.call('HDEL', old_queue_keys_key, concurrency_key)
        end
        redis.call('HINCRBY', new_queue_keys_key, concurrency_key, 1)
    else
        redis.call('ZREM', old_unkeyed_key, job_id)
        redis.call('ZADD', new_unkeyed_key, new_score, job_id)
    end
end
if old_handler_k ~= new_handler_k then
    redis.call('SREM', old_handler_k, job_id)
    redis.call('SADD', new_handler_k, job_id)
end
if state == 'ENQUEUED' then
    redis.call('SADD', queues_key, new_queue)
end
-- Rescore in the by_state_time index too (state is unchanged).
redis.call('ZADD', state_time_k, new_state_at, job_id)

redis.call('HSET', job_key,
    'body', new_body,
    'queue', new_queue,
    'priority', new_priority,
    'handler_signature', new_handler,
    'scheduled_at', new_sched,
    'current_state_at', tostring(new_state_at),
    'workflow_root_id', workflow_root_id,
    'concurrency_key', concurrency_key,
    'concurrency_mode', concurrency_mode,
    'version', new_ver
)
return 'OK'
