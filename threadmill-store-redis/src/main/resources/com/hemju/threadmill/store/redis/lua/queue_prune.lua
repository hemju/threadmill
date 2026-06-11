-- Atomically remove a queue from the registry only while its ZSET is empty,
-- so pruning cannot race an insert that just re-populated the queue.
--
-- KEYS:
--   [1] queue ZSET
--   [2] queue registry SET
--
-- ARGV:
--   [1] queue name (registry member)
--
-- Returns 1 if pruned, 0 if the queue still has entries.

if redis.call('ZCARD', KEYS[1]) == 0 then
    redis.call('SREM', KEYS[2], ARGV[1])
    return 1
end
return 0
