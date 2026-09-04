# Redis Topologies

Threadmill supports Redis standalone, Sentinel, and Cluster clients through one
configuration model.

## Standalone

```yaml
threadmill:
  store:
    redis:
      mode: standalone
      uri: rediss://threadmill:${REDIS_PASSWORD}@localhost:6380?verifyPeer=FULL
```

The standalone URI is a Lettuce `RedisURI`. Use `redis://` without TLS or
`rediss://` with TLS; `verifyPeer` accepts `FULL`, `CA`, or `NONE`. `FULL`
verifies the certificate chain and hostname, `CA` verifies only the chain, and
`NONE` is appropriate only for disposable development environments.

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
        data-node-username: threadmill-data
        data-node-password: ${REDIS_DATA_PASSWORD}
        sentinel-username: threadmill-sentinel
        sentinel-password: ${REDIS_SENTINEL_PASSWORD}
        tls: true
        verify-mode: full
```

The data-node and Sentinel credentials are independent. Password-only
authentication is also supported by omitting the corresponding username.
Lettuce uses one TLS policy for Sentinel discovery and the discovered Redis
data nodes, so `tls` and `verify-mode` apply to both connection planes. The
legacy `sentinel.password` property remains a data-node password alias for
compatibility; new configuration should use the explicit `data-node-*` names.

## Cluster

```yaml
threadmill:
  store:
    redis:
      mode: cluster
      cluster:
        nodes:
          - redis-1:6380
          - redis-2:6380
        read-policy: master
        username: threadmill
        password: ${REDIS_CLUSTER_PASSWORD}
        tls: true
        verify-mode: full
```

`read-policy` remains fixed to `master`: Threadmill does not read mutable job
state from replicas. Every Cluster seed receives the configured ACL credentials
and TLS policy.

## TLS Trust and Custom Clients

`verify-mode` accepts `full`, `ca`, or `none` and defaults to `full`. The older
`verify-peer` boolean remains supported (`true` is `full`, `false` is `none`)
when `verify-mode` is absent. Certificates must chain to the JVM's
trust material; use the standard `javax.net.ssl.trustStore`,
`javax.net.ssl.trustStoreType`, and `javax.net.ssl.trustStorePassword` system
properties when a private CA is not already trusted. Disabling peer
verification is accepted only when TLS is enabled and should be limited to
disposable development environments.

Applications that need a private per-client trust root, mutual TLS, rotating
credentials, custom `ClientResources`, or another client policy should build a
`RedisClient` or `RedisClusterClient` and inject it. For example:

```java
var credentials = RedisCredentialsProvider.from(
    () -> RedisCredentials.just("threadmill", secretSource.currentPassword()));
var seed = RedisURI.builder()
    .withHost("redis-1.internal")
    .withPort(6380)
    .withAuthentication(credentials)
    .withSsl(true)
    .withVerifyPeer(SslVerifyMode.FULL)
    .build();
var ssl = SslOptions.builder()
    .trustManager(Path.of("redis-ca.pem").toFile())
    .keyManager(
        Path.of("threadmill-client.crt").toFile(),
        Path.of("threadmill-client.key").toFile(),
        null)
    .build();
var client = RedisClusterClient.create(seed);
client.setOptions(ClusterClientOptions.builder().sslOptions(ssl).build());
var store = new RedisJobStore(client);
```

The credential provider is resolved by Lettuce for new authentication events,
so its supplier can read the current secret. The same injected-client path is
used for Sentinel; its aggregate `RedisURI` must carry the independent data and
Sentinel credential providers. Redis requires client certificates by default
when TLS is enabled; the `keyManager` configuration above supplies one.

The caller retains client ownership; closing the store closes its connection,
not the injected client.

Topology descriptions and Threadmill-wrapped, configuration-owned initial
connection failures omit both ACL usernames and passwords. The wrapper retains
the safe topology summary, failure category, and original exception type chain,
but not the credential-bearing original exception messages.

All Threadmill keys use the `{threadmill}` hash tag. That keeps every multi-key
Lua script in one Redis Cluster slot and makes the v1 store Cluster-safe. It
also means Cluster is used for topology and failover, not for horizontal
distribution of Threadmill job keys across masters. Optional Lua key positions
use a `{threadmill}:no_key` sentinel rather than an empty string, so even jobs
without optional indexes preserve that one-slot guarantee.

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

Monitor `evicted_keys`, `current_eviction_exceeded_time`, and
`threadmill.store.writes.rejected`. If Redis runs out of memory under
`noeviction`, Threadmill treats the write failure as a store outage and
dispatcher recovery probes perform a small write before processing resumes.
The rejected-write meter counts failed attempts, including retries, while
excluding expected stale-version, oversize, invalid-argument, and duplicate-id
outcomes.

For managed Redis products that block `CONFIG GET`, document the external
validation in the application's runbook before setting the override. The
minimum operator checklist is: AOF persistence enabled, `maxmemory-policy
noeviction`, persistence/replication health alerts, rejected-write alerts, and
enough memory headroom for peak queued jobs plus retained terminal jobs.

## Reliability Model

Threadmill's Redis backend uses reliable-fetch semantics: claiming work never
destructively pops a payload. Java prepares the `PROCESSING` body, then
`claim_commit.lua` atomically verifies the current version/state/queue and
moves the job hash plus every index/count to `PROCESSING`. A crash before the
script leaves the job `ENQUEUED`; a crash after the script leaves a complete
`PROCESSING` record for orphan recovery.

Threadmill treats Redis as first-class durable storage, but only with the
production constraints above: AOF enabled, `noeviction`, and alerts on oldest
processing heartbeat (`threadmill.processing.oldest.heartbeat.age`), reclaim
count (`threadmill.jobs.orphan.reclaimed`), claim failures
(`threadmill.claim.failures`), rejected writes
(`threadmill.store.writes.rejected`), and queue depth
(`threadmill.queue.depth`).
