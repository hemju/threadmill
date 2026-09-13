-- Auxiliary admission indexes. Derived keys remain in the {threadmill} slot.
-- Every mutation calls these helpers in the same atomic script as its job write.
local function tm_pending_add(key, score, member, state, queue_keys)
    redis.call('ZADD', key, score, member)
    if string.sub(member, -10) == ':EXCLUSIVE' then
        redis.call('ZADD', key .. '__THREADMILL_EXCLUSIVE_SUFFIX__', score, member)
    end
    if state == 'ENQUEUED' then
        redis.call('ZADD', key .. '__THREADMILL_READY_SUFFIX__' .. queue_keys, score, member)
    end
end

local function tm_pending_remove(key, member, queue_keys)
    redis.call('ZREM', key, member)
    redis.call('ZREM', key .. '__THREADMILL_EXCLUSIVE_SUFFIX__', member)
    redis.call('ZREM', key .. '__THREADMILL_READY_SUFFIX__' .. queue_keys, member)
end

local function tm_queue_add(key, member)
    redis.call('HINCRBY', key, member, 1)
    redis.call('ZADD', key .. '__THREADMILL_ORDERED_SUFFIX__', 0, member)
end

local function tm_queue_remove(key, member)
    local remaining = redis.call('HINCRBY', key, member, -1)
    if remaining <= 0 then
        redis.call('HDEL', key, member)
        redis.call('ZREM', key .. '__THREADMILL_ORDERED_SUFFIX__', member)
    end
end

-- Stable maintenance keyset views, separate from time-ordered dashboard indexes.
local function tm_state_add(key, score, id)
    redis.call('ZADD', key, score, id)
    redis.call('ZADD', key .. '__THREADMILL_IDS_SUFFIX__', 0, id)
end

local function tm_state_remove(key, id)
    redis.call('ZREM', key, id)
    redis.call('ZREM', key .. '__THREADMILL_IDS_SUFFIX__', id)
end
