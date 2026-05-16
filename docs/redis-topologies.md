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
