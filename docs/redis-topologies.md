# Redis Topologies

Threadmill supports Redis standalone, Sentinel, and Cluster clients through one
configuration model.

## Standalone

```yaml
threadmill:
  store:
    redis:
      mode: standalone
      uri: redis://localhost:6379
```

## Sentinel

```yaml
threadmill:
  store:
    redis:
      mode: sentinel
      sentinel:
        master-name: mymaster
        nodes:
          - redis-sentinel-1:26379
          - redis-sentinel-2:26379
          - redis-sentinel-3:26379
```

## Cluster

```yaml
threadmill:
  store:
    redis:
      mode: cluster
      cluster:
        nodes:
          - redis-1:6379
          - redis-2:6379
        read-policy: master
```

All Threadmill keys use the `{threadmill}` hash tag. That keeps every multi-key
Lua script in one Redis Cluster slot and makes the v1 store Cluster-safe. It
also means Cluster is used for topology and failover, not for horizontal
distribution of Threadmill job keys across masters.

For production durability, enable Redis AOF, for example `appendonly yes`.
Threadmill's durability on Redis is bounded by the Redis persistence policy you
choose.

## Memory Policy

Threadmill requires Redis `maxmemory-policy noeviction`. Redis configured as a
cache (`allkeys-*` or `volatile-*`) is not a safe job store:

- `allkeys-*` can evict job hashes while leaving queue indexes, counts, or
  handler indexes behind.
- `volatile-*` can evict TTL-backed operational keys such as node heartbeats,
  maintenance leases, mutexes, and short claim locks.

`RedisJobStore` validates the policy at startup with `CONFIG GET
maxmemory-policy` and refuses to start when the policy is not `noeviction`. For
managed Redis products that block `CONFIG GET`, set
`threadmill.store.redis.no-eviction-externally-validated=true` only after
verifying the policy externally.

Monitor `evicted_keys`, `current_eviction_exceeded_time`, and rejected write
commands. If Redis runs out of memory under `noeviction`, Threadmill treats the
write failure as a store outage and dispatcher recovery probes perform a small
write before processing resumes.

## Reliability Model

Threadmill's Redis backend uses reliable-fetch semantics: claiming work never
destructively pops a payload. Java prepares the `PROCESSING` body, then
`claim_commit.lua` atomically verifies the current version/state/queue and
moves the job hash plus every index/count to `PROCESSING`. A crash before the
script leaves the job `ENQUEUED`; a crash after the script leaves a complete
`PROCESSING` record for orphan recovery.

Threadmill treats Redis as first-class durable storage, but only with the
production constraints above: AOF enabled, `noeviction`, and alerts on oldest
processing heartbeat, reclaim count, claim failures, rejected writes, and
queue depth.
