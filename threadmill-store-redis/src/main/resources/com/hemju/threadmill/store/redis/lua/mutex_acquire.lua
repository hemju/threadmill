-- Atomic acquire-or-refresh of a named cross-cluster mutex.
--
-- A separate SET-NX-then-PEXPIRE pair has a microsecond race window: the
-- key can expire between the SET-NX failing and the PEXPIRE landing, in
-- which case the caller would believe it holds a mutex that no longer
-- exists. Doing the whole sequence inside one Lua script removes the
-- window — Redis runs Lua atomically.
--
-- KEYS:
--   [1] mutex key
--
-- ARGV:
--   [1] holder identifier (string)
--   [2] lease TTL in milliseconds (positive integer)
--
-- Returns:
--   'ACQUIRED'  — the mutex was free and is now held by ARGV[1]
--   'REFRESHED' — ARGV[1] already held it; the lease has been extended
--   'HELD'      — another holder owns it; not acquired

local key    = KEYS[1]
local holder = ARGV[1]
local ttl    = tonumber(ARGV[2])

local current = redis.call('GET', key)
if not current then
    redis.call('SET', key, holder, 'PX', ttl)
    return 'ACQUIRED'
end
if current == holder then
    redis.call('PEXPIRE', key, ttl)
    return 'REFRESHED'
end
return 'HELD'
