local key = KEYS[1]
local holder = ARGV[1]

if redis.call('GET', key) == holder then
    redis.call('DEL', key)
    return 'OK'
end
return 'BUSY'
