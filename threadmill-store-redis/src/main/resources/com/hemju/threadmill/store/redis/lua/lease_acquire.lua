-- Acquire or renew a single-holder lease. Returns OK if ARGV[1] holds
-- the lease after the call, BUSY otherwise.

local key = KEYS[1]
local holder = ARGV[1]
local ttl_ms = tonumber(ARGV[2])

local current = redis.call('GET', key)
if current == false or current == holder then
    redis.call('SET', key, holder, 'PX', ttl_ms)
    return 'OK'
end
return 'BUSY'
