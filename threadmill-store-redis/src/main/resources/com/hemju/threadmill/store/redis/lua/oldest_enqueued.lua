-- Minimum current_state_at across a queue's ENQUEUED members. The queue ZSET
-- is priority-ordered, so its first member is the oldest HIGHEST-PRIORITY
-- job, not the oldest enqueued — an old low-priority job starving behind
-- fresh high-priority work would be invisible to the age gauge.
--
-- Job keys are built from ARGV[2] inside the script (the member set is
-- discovered here); every engine key shares the {threadmill} hash tag, so
-- single-slot cluster safety holds (see RedisKeysTest).
--
-- KEYS:
--   [1] queue ZSET
--
-- ARGV:
--   [1] job key prefix
--
-- Returns the minimum current_state_at in millis as a string, or '' when
-- the queue is empty.

local ids = redis.call('ZRANGE', KEYS[1], 0, -1)
local oldest = nil
for _, id in ipairs(ids) do
    local t = tonumber(redis.call('HGET', ARGV[1] .. id, 'current_state_at'))
    if t ~= nil and (oldest == nil or t < oldest) then
        oldest = t
    end
end
if oldest == nil then
    return ''
end
return tostring(oldest)
