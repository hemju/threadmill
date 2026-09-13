-- Offline, idempotent per-job conversion. Existing pending scores retain micros.
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
local score = redis.call('ZSCORE', KEYS[2], ARGV[2])
    or redis.call('ZSCORE', KEYS[2], ARGV[3])
    or tostring(tonumber(ARGV[5]) * 1000)
tm_pending_remove(KEYS[2], ARGV[2], KEYS[4])
tm_pending_add(KEYS[2], score, ARGV[3], ARGV[4], KEYS[4])
if ARGV[7] == '1' then
    redis.call('ZREM', KEYS[3], ARGV[2])
    redis.call('ZADD', KEYS[3], score, ARGV[3])
end
if ARGV[4] == 'ENQUEUED' then
    redis.call('ZADD', KEYS[4] .. ':ordered', 0, ARGV[6])
end
return 1
