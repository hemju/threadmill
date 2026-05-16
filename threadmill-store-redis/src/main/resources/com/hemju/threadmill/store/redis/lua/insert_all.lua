-- Atomic batch insert. All-or-nothing: every job is inserted or none are.
--
-- Layout: 7 keys + 18 args per job, matching insert.lua exactly. The leading
-- ARGV[1] is the job count N (so the total ARGV length is 1 + 18*N).
--
-- KEYS[i*7+1 .. i*7+7] are the 7 keys for job i (0-based).
-- ARGV[i*18+2 .. i*18+19] are the 18 args for job i.
--
-- Returns 'OK' on success. On a duplicate id, returns 'EXISTS:N' where N is
-- the 0-based index of the offending job — and no writes have been made
-- (existence is pre-validated for every job before any write).

local n = tonumber(ARGV[1])
if n == nil or n <= 0 then
    return 'OK'
end

-- Pre-flight: every job id must not already exist.
local key_offset = 0
for i = 1, n do
    if redis.call('EXISTS', KEYS[key_offset + 1]) == 1 then
        return 'EXISTS:' .. tostring(i - 1)
    end
    key_offset = key_offset + 7
end

-- All clear — apply every insert.
key_offset = 0
local arg_offset = 1
for i = 1, n do
    local job_key        = KEYS[key_offset + 1]
    local active_key     = KEYS[key_offset + 2]
    local state_time_key = KEYS[key_offset + 3]
    local handler_key    = KEYS[key_offset + 4]
    local counts_key     = KEYS[key_offset + 5]
    local pending_key    = KEYS[key_offset + 6]
    local workflows_key  = KEYS[key_offset + 7]

    local job_id           = ARGV[arg_offset + 1]
    local body             = ARGV[arg_offset + 2]
    local state            = ARGV[arg_offset + 3]
    local queue            = ARGV[arg_offset + 4]
    local handler          = ARGV[arg_offset + 5]
    local priority         = ARGV[arg_offset + 6]
    local scheduled_at     = ARGV[arg_offset + 7]
    local owner_node       = ARGV[arg_offset + 8]
    local owner_heartbeat  = ARGV[arg_offset + 9]
    local last_checkin     = ARGV[arg_offset + 10]
    local state_time       = tonumber(ARGV[arg_offset + 11])
    local created_at       = ARGV[arg_offset + 12]
    local active_score     = tonumber(ARGV[arg_offset + 13])
    local workflow_root_id = ARGV[arg_offset + 14]
    local concurrency_key  = ARGV[arg_offset + 15]
    local concurrency_mode = ARGV[arg_offset + 16]
    local pending_member   = ARGV[arg_offset + 17]
    local pending_score    = tonumber(ARGV[arg_offset + 18])

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
    redis.call('ZADD', state_time_key, state_time, job_id)
    redis.call('SADD', handler_key, job_id)
    redis.call('HINCRBY', counts_key, state, 1)

    key_offset = key_offset + 7
    arg_offset = arg_offset + 18
end

return 'OK'
