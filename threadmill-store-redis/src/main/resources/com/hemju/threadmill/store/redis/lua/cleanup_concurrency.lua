-- KEYS: ordered counter registry, counter hash, pending ZSET, holds HASH,
-- outstanding workflow-member counts HASH. The script is atomic with enqueue,
-- claim and terminal writes; missing counters mean zero on the next claim.
if redis.call('EXISTS', KEYS[2]) == 0 then
    redis.call('ZREM', KEYS[1], KEYS[2])
    return 0
end
if tonumber(redis.call('HGET', KEYS[2], 'exclusive_in_flight') or '0') ~= 0
    or tonumber(redis.call('HGET', KEYS[2], 'shared_in_flight') or '0') ~= 0
    or redis.call('ZCARD', KEYS[3]) ~= 0
    or redis.call('HLEN', KEYS[4]) ~= 0
    or redis.call('HLEN', KEYS[5]) ~= 0 then
    redis.call('HDEL', KEYS[2], 'idle_since')
    return 0
end
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local idle_since = tonumber(redis.call('HGET', KEYS[2], 'idle_since'))
if not idle_since then
    redis.call('HSET', KEYS[2], 'idle_since', now)
    return 0
end
if now - idle_since < 60000 then return 0 end
redis.call('DEL', KEYS[2])
redis.call('ZREM', KEYS[1], KEYS[2])
return 1
