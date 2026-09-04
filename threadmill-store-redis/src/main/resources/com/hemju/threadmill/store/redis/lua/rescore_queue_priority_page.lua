-- Rescore one bounded ZSCAN page from the current job hashes. Membership and
-- scalars are checked atomically, so a concurrent claim cannot be resurrected
-- and a concurrent replacement cannot be overwritten with a stale priority.
--
-- KEYS:
--   [1] queue or queue_unkeyed ZSET being rescored
--   [2] optional queue_enqueued_at ZSET (combined priority + age upgrade)
--
-- ARGV: 3 fixed args + member-id tail
--   [1] job-hash key prefix, including trailing colon
--   [2] expected queue name
--   [3] require_unkeyed: "1" for queue_unkeyed, otherwise "0"
--   [4..N] job ids captured by the ZSCAN page
--
-- Returns 'OK', 'MALFORMED_PRIORITY:<id>', or 'MALFORMED_STATE_TIME:<id>'.
-- A malformed return may follow successful updates earlier in the page;
-- rescoring is idempotent, so retrying after data repair is safe.

local job_prefix = ARGV[1]
local expected_queue = ARGV[2]
local require_unkeyed = ARGV[3] == '1'

for i = 4, #ARGV do
    local id = ARGV[i]
    if redis.call('ZSCORE', KEYS[1], id) then
        local job_key = job_prefix .. id
        local state = redis.call('HGET', job_key, 'state')
        local queue = redis.call('HGET', job_key, 'queue')
        local concurrency_key = redis.call('HGET', job_key, 'concurrency_key')
        if state ~= 'ENQUEUED'
                or queue ~= expected_queue
                or (require_unkeyed and concurrency_key and concurrency_key ~= '') then
            redis.call('ZREM', KEYS[1], id)
            if #KEYS == 2 then
                redis.call('ZREM', KEYS[2], id)
            end
        else
            local priority = redis.call('HGET', job_key, 'priority')
            local numeric_priority = tonumber(priority)
            if not numeric_priority
                    or numeric_priority ~= math.floor(numeric_priority)
                    or numeric_priority < -2147483648
                    or numeric_priority > 2147483647 then
                return 'MALFORMED_PRIORITY:' .. id
            end
            redis.call('ZADD', KEYS[1], 'XX', -numeric_priority, id)
            if #KEYS == 2 then
                local state_at = redis.call('HGET', job_key, 'current_state_at')
                if not tonumber(state_at) then
                    return 'MALFORMED_STATE_TIME:' .. id
                end
                redis.call('ZADD', KEYS[2], state_at, id)
            end
        end
    end
end

return 'OK'
