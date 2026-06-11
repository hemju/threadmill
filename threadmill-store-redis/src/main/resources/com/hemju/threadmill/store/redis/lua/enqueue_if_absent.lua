-- Atomically insert a job unless the dedup key already points at a live job.

local dedup_key      = KEYS[1]
local job_key        = KEYS[2]
local active_key     = KEYS[3]
local state_time_key = KEYS[4]
local handler_key    = KEYS[5]
local counts_key     = KEYS[6]
local expiry_key     = KEYS[7]
local pending_key    = KEYS[8]
local workflows_key  = KEYS[9]
local workflow_counts_key = KEYS[10]
local awaiting_parent_key = KEYS[11]
local queues_key          = KEYS[12]

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
local expires_at       = tonumber(ARGV[14])
local now_ms           = tonumber(ARGV[15])
local job_prefix       = ARGV[16]
local workflow_root_id = ARGV[17]
local concurrency_key  = ARGV[18]
local concurrency_mode = ARGV[19]
local pending_member   = ARGV[20]
local pending_score    = tonumber(ARGV[21])

local existing = redis.call('HGET', dedup_key, 'job_id')
if existing then
    -- Deliberate non-KEYS access: the referenced job key is discovered from
    -- the dedup record INSIDE the script, so it cannot be passed in KEYS.
    -- Safe because every engine key carries the {threadmill} hash tag and
    -- therefore lives in one cluster slot (pinned by
    -- RedisKeysTest.allEngineKeysUseOneClusterSlot).
    local existing_expires = tonumber(redis.call('HGET', dedup_key, 'expires_at'))
    local existing_state = redis.call('HGET', job_prefix .. existing, 'state')
    if existing_expires ~= nil and existing_expires <= now_ms and
       (not existing_state or existing_state == 'SUCCEEDED' or existing_state == 'FAILED' or existing_state == 'DELETED' or existing_state == 'QUARANTINED') then
        redis.call('DEL', dedup_key)
        redis.call('ZREM', expiry_key, dedup_key)
    else
        return 'COALESCED:' .. existing
    end
end

if redis.call('EXISTS', job_key) == 1 then
    return 'EXISTS'
end

redis.call('HSET', dedup_key, 'job_id', job_id, 'expires_at', tostring(expires_at))
redis.call('ZADD', expiry_key, expires_at, dedup_key)
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
    redis.call('SADD', queues_key, queue)
end
redis.call('ZADD', state_time_key, state_time, job_id)
redis.call('SADD', handler_key, job_id)
redis.call('HINCRBY', counts_key, state, 1)
return 'OK'
