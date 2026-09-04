local no_key = '__THREADMILL_NO_KEY__'
if redis.call('HGET', KEYS[1], 'state') ~= 'ENQUEUED' then return 0 end
if redis.call('HGET', KEYS[1], 'version') ~= ARGV[3] then return 0 end
redis.call('HSET', KEYS[1], 'state', 'QUARANTINED', 'current_state_at', ARGV[2],
  'version', tostring(tonumber(ARGV[3]) + 1))
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[13], ARGV[1])
redis.call('ZADD', KEYS[4], tonumber(ARGV[2]), ARGV[1])
redis.call('HINCRBY', KEYS[5], 'ENQUEUED', -1)
redis.call('HINCRBY', KEYS[5], 'QUARANTINED', 1)
if KEYS[6] ~= no_key and ARGV[4] ~= '' then
  redis.call('ZREM', KEYS[6], ARGV[4])
end
if KEYS[12] ~= no_key and ARGV[4] ~= '' then
  redis.call('ZREM', KEYS[12], ARGV[4])
end
if ARGV[7] ~= '' then
  local remaining = redis.call('HINCRBY', KEYS[10], ARGV[7], -1)
  if remaining <= 0 then redis.call('HDEL', KEYS[10], ARGV[7]) end
else
  redis.call('ZREM', KEYS[11], ARGV[1])
end
if KEYS[8] ~= no_key and ARGV[5] ~= '' then
  local workflow_count = redis.call('HINCRBY', KEYS[8], ARGV[5], -1)
  if workflow_count <= 0 then redis.call('HDEL', KEYS[8], ARGV[5]) end
end
-- Release the hold share only when a hold actually exists for this root: a
-- never-claimed standalone job has none, and a phantom decrement would corrupt
-- the in-flight counter that other roots on the key rely on.
if KEYS[7] ~= no_key and KEYS[9] ~= no_key and ARGV[5] ~= '' and
   redis.call('HGET', KEYS[7], ARGV[5]) ~= false then
  local outstanding = redis.call('HINCRBY', KEYS[7], ARGV[5], -1)
  if outstanding <= 0 then
    redis.call('HDEL', KEYS[7], ARGV[5])
    local field = 'shared_in_flight'
    if ARGV[6] == 'EXCLUSIVE' then field = 'exclusive_in_flight' end
    local next_count = redis.call('HINCRBY', KEYS[9], field, -1)
    if next_count < 0 then redis.call('HSET', KEYS[9], field, '0') end
  end
end
return 1
