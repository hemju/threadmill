-- Atomically insert a new job. Refuses duplicates.
--
-- KEYS:
--   [1] job hash                          (threadmill:job:{id})
--   [2] active-state ZSET, or empty       (queue / scheduled / awaiting)
--   [3] by_state_time ZSET                (threadmill:by_state_time:{STATE})
--   [4] by_handler set                    (threadmill:by_handler:{handlerType})
--   [5] counts hash                       (threadmill:counts)
--   [6] concurrency pending ZSET, or empty
--   [7] concurrency workflows HASH, or empty
--   [8] concurrency workflow counts HASH, or empty
--   [9] awaiting_by_parent SET, or empty
--   [10] queue registry SET
--
-- ARGV:
--   [1] job id (string)
--   [2] body
--   [3] state
--   [4] queue
--   [5] handler_signature
--   [6] priority
--   [7] scheduled_at      (millis since epoch, empty string if absent)
--   [8] owner_node_id     (empty string if absent)
--   [9] owner_heartbeat_at (millis, empty string if absent)
--   [10] last_checkin_at  (millis, empty string if absent)
--   [11] current_state_at (millis)
--   [12] created_at       (millis)
--   [13] active_score     (numeric)
--   [14] workflow_root_id
--   [15] concurrency_key
--   [16] concurrency_mode
--   [17] concurrency pending member, or empty
--   [18] concurrency pending score
--
-- Returns 1 on success, "exists" on duplicate id.

local job_key        = KEYS[1]
local active_key     = KEYS[2]
local state_time_key = KEYS[3]
local handler_key    = KEYS[4]
local counts_key     = KEYS[5]
local pending_key    = KEYS[6]
local workflows_key  = KEYS[7]
local workflow_counts_key = KEYS[8]
local awaiting_parent_key = KEYS[9]
local queues_key          = KEYS[10]

local job_id           = ARGV[1]
local body             = ARGV[2]
local state            = ARGV[3]
local queue            = ARGV[4]
local handler          = ARGV[5]
local priority         = ARGV[6]
local scheduled_at     = ARGV[7]
local owner_node       = ARGV[8]
local owner_heartbeat  = ARGV[9]
local last_checkin     = ARGV[10]
local state_time       = tonumber(ARGV[11])
local created_at       = ARGV[12]
local active_score     = tonumber(ARGV[13])
local workflow_root_id = ARGV[14]
local concurrency_key  = ARGV[15]
local concurrency_mode = ARGV[16]
local pending_member   = ARGV[17]
local pending_score    = tonumber(ARGV[18])

if redis.call('EXISTS', job_key) == 1 then
    return 'EXISTS'
end

redis.call('HSET', job_key,
    'body', body,
    'state', state,
    'queue', queue,
    'handler_signature', handler,
    'priority', priority,
    'scheduled_at', scheduled_at,
    'owner_node_id', owner_node,
    'owner_heartbeat_at', owner_heartbeat,
    'last_checkin_at', last_checkin,
    'current_state_at', tostring(state_time),
    'created_at', created_at,
    'workflow_root_id', workflow_root_id,
    'concurrency_key', concurrency_key,
    'concurrency_mode', concurrency_mode,
    'version', '1'
)

if active_key ~= '' and active_score ~= nil then
    redis.call('ZADD', active_key, active_score, job_id)
end
if concurrency_key ~= '' and pending_key ~= '' and pending_member ~= '' and
   (state == 'ENQUEUED' or state == 'SCHEDULED' or state == 'AWAITING') then
    redis.call('ZADD', pending_key, pending_score, pending_member)
end
if concurrency_key ~= '' and workflows_key ~= '' and
   redis.call('HGET', workflows_key, workflow_root_id) ~= false and
   (state == 'ENQUEUED' or state == 'SCHEDULED' or state == 'AWAITING' or state == 'PROCESSING') then
    redis.call('HINCRBY', workflows_key, workflow_root_id, 1)
end
if concurrency_key ~= '' and workflow_counts_key ~= '' and
   (state == 'ENQUEUED' or state == 'SCHEDULED' or state == 'AWAITING' or state == 'PROCESSING') then
    redis.call('HINCRBY', workflow_counts_key, workflow_root_id, 1)
end
if awaiting_parent_key ~= '' and state == 'AWAITING' then
    redis.call('SADD', awaiting_parent_key, job_id)
end
if state == 'ENQUEUED' then
    -- Registry membership lands in the same atomic call as the enqueue: a
    -- crash between the two would otherwise leave a durably ENQUEUED job in
    -- a queue the discovery paths cannot see.
    redis.call('SADD', queues_key, queue)
end
redis.call('ZADD', state_time_key, state_time, job_id)
redis.call('SADD', handler_key, job_id)
redis.call('HINCRBY', counts_key, state, 1)
return 'OK'
