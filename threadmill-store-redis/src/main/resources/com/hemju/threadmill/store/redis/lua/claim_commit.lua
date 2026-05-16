-- Atomically commit a prepared claim. Java reads a candidate and prepares
-- the new serialized body first; this script verifies that candidate is
-- still ENQUEUED at the expected version and then commits body, scalars,
-- indexes, and counts together.

-- KEYS:
--   [1] job hash
--   [2] queue ZSET
--   [3] processing_all ZSET
--   [4] processing_for_node ZSET
--   [5] by_state_time ENQUEUED
--   [6] by_state_time PROCESSING
--   [7] counts hash
--   [8] concurrency counters HASH, or empty
--   [9] concurrency pending ZSET, or empty
--   [10] concurrency workflows HASH, or empty

-- ARGV:
--   [1] job id
--   [2] expected version
--   [3] new version
--   [4] new body
--   [5] node id
--   [6] heartbeat millis
--   [7] new attempts
--   [8] concurrency key, or empty
--   [9] concurrency mode, or empty
--   [10] workflow root id
--   [11] concurrency pending member, or empty
--   [12] workflow outstanding count

local job_key = KEYS[1]
local queue_key = KEYS[2]
local processing_all = KEYS[3]
local processing_node = KEYS[4]
local enqueued_state_time = KEYS[5]
local processing_state_time = KEYS[6]
local counts_key = KEYS[7]
local counters_key = KEYS[8]
local pending_key = KEYS[9]
local workflows_key = KEYS[10]

local job_id = ARGV[1]
local expected_version = tonumber(ARGV[2])
local new_version = ARGV[3]
local new_body = ARGV[4]
local node_id = ARGV[5]
local heartbeat_ms = tonumber(ARGV[6])
local attempts = ARGV[7]
local concurrency_key = ARGV[8]
local concurrency_mode = ARGV[9]
local workflow_root_id = ARGV[10]
local pending_member = ARGV[11]
local outstanding_count = tonumber(ARGV[12])

local function member_job_id(member)
    local sep = string.find(member, ':', 1, true)
    if sep == nil then
        return member
    end
    return string.sub(member, sep + 1)
end

local function member_matches(member, exclusive_only)
    return not exclusive_only or string.sub(member, 1, 10) == 'EXCLUSIVE:'
end

local function has_earlier_pending(exclusive_only)
    if pending_key == '' or pending_member == '' then
        return false
    end
    local score = redis.call('ZSCORE', pending_key, pending_member)
    if score == false then
        return true
    end
    local earlier = redis.call('ZRANGEBYSCORE', pending_key, '-inf', '(' .. score)
    for _, member in ipairs(earlier) do
        if member ~= pending_member and member_matches(member, exclusive_only) then
            return true
        end
    end
    local same_score = redis.call('ZRANGEBYSCORE', pending_key, score, score)
    for _, member in ipairs(same_score) do
        if member ~= pending_member and member_matches(member, exclusive_only) and member_job_id(member) < job_id then
            return true
        end
    end
    return false
end

if redis.call('EXISTS', job_key) == 0 then
    return 'VANISHED'
end

if redis.call('ZSCORE', queue_key, job_id) == false then
    return 'NOT_READY'
end

local persisted_version = tonumber(redis.call('HGET', job_key, 'version'))
if persisted_version ~= expected_version then
    return 'STALE'
end

local state = redis.call('HGET', job_key, 'state')
if state ~= 'ENQUEUED' then
    return 'NOT_READY'
end

if concurrency_key ~= '' then
    local active_hold = redis.call('HGET', workflows_key, workflow_root_id)
    if active_hold == false then
        local exclusive = tonumber(redis.call('HGET', counters_key, 'exclusive_in_flight') or '0')
        local shared = tonumber(redis.call('HGET', counters_key, 'shared_in_flight') or '0')
        if concurrency_mode == 'EXCLUSIVE' then
            if exclusive > 0 or shared > 0 or has_earlier_pending(false) then
                return 'BLOCKED'
            end
            redis.call('HINCRBY', counters_key, 'exclusive_in_flight', 1)
        else
            if exclusive > 0 or has_earlier_pending(true) then
                return 'BLOCKED'
            end
            redis.call('HINCRBY', counters_key, 'shared_in_flight', 1)
        end
        redis.call('HSET', workflows_key, workflow_root_id, tostring(outstanding_count))
    end
    redis.call('ZREM', pending_key, pending_member)
end

redis.call('ZREM', queue_key, job_id)
redis.call('ZREM', enqueued_state_time, job_id)
redis.call('ZADD', processing_all, heartbeat_ms, job_id)
redis.call('ZADD', processing_node, heartbeat_ms, job_id)
redis.call('ZADD', processing_state_time, heartbeat_ms, job_id)
redis.call('HINCRBY', counts_key, 'ENQUEUED', -1)
redis.call('HINCRBY', counts_key, 'PROCESSING', 1)

redis.call('HSET', job_key,
    'body', new_body,
    'state', 'PROCESSING',
    'owner_node_id', node_id,
    'owner_heartbeat_at', tostring(heartbeat_ms),
    'last_checkin_at', '',
    'current_state_at', tostring(heartbeat_ms),
    'version', new_version,
    'attempts', attempts
)
return 'OK'
