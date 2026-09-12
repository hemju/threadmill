-- KEYS: owner processing index, global processing index, then <=500 job hashes.
-- ARGV: owner ID, heartbeat millis, then corresponding job ID/version pairs.
local now = tonumber(ARGV[2])
local updated = 0
for i = 3, #KEYS do
    local argument = 3 + (i - 3) * 2
    local fields = redis.call('HMGET', KEYS[i], 'state', 'owner_node_id', 'version', 'owner_heartbeat_at')
    if fields[1] == 'PROCESSING' and fields[2] == ARGV[1] and fields[3] == ARGV[argument + 1] then
        local heartbeat = math.max(tonumber(fields[4]) or 0, now)
        redis.call('HSET', KEYS[i], 'owner_heartbeat_at', tostring(heartbeat))
        redis.call('ZADD', KEYS[1], heartbeat, ARGV[argument])
        redis.call('ZADD', KEYS[2], heartbeat, ARGV[argument])
        updated = updated + 1
    end
end
return updated
