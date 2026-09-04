-- Compare-and-remove one page of stale age-index members. Atomic per page so
-- a promotion landing between a Java-side membership check and removal cannot
-- lose a freshly re-added valid member.
--
-- KEYS:
--   [1] queue_enqueued_at ZSET
--   [2] authoritative queue ZSET
--
-- ARGV:
--   [1..N] job ids captured from the age-index ZSCAN page
--
-- Returns the number of stale members removed.

local removed = 0
for i = 1, #ARGV do
    if redis.call('ZSCORE', KEYS[2], ARGV[i]) == false then
        removed = removed + redis.call('ZREM', KEYS[1], ARGV[i])
    end
end
return removed
