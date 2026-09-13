-- Read-only (time,id) keyset page from the existing scored state index.
-- KEYS[1]: by_state_time. ARGV: cutoff millis, limit, cursor millis or '', id.
-- ZRANK cannot resume a deleted cursor. Seek within its equal-score group by
-- binary search instead: bounded O(log^2 N + page), even for huge timestamp
-- ties, and no temporary members or persistent auxiliary indexes.
local first = 0
if ARGV[3] ~= '' then
    local score = ARGV[3]
    local low = redis.call('ZCOUNT', KEYS[1], '-inf', '(' .. score)
    local high = low + redis.call('ZCOUNT', KEYS[1], score, score)
    while low < high do
        local middle = math.floor((low + high) / 2)
        local member = redis.call('ZRANGE', KEYS[1], middle, middle)[1]
        if member <= ARGV[4] then low = middle + 1 else high = middle end
    end
    first = low
end
local eligible = redis.call('ZCOUNT', KEYS[1], '-inf', ARGV[1])
local last = math.min(eligible - 1, first + tonumber(ARGV[2]) - 1)
if first > last then return {} end
return redis.call('ZRANGE', KEYS[1], first, last, 'WITHSCORES')
