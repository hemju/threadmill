# Redis Topologies

Threadmill supports Redis standalone, Sentinel, and Cluster clients through one
configuration model. Every data node must run Redis 7.4 or later, including
replicas that may become primary. Startup validates the connected server's
version and no-eviction policy; externally validated managed deployments must
verify both independently on every node before opting out of these checks.

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
explicit `data-node-*` properties configure the Redis data connection.

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

`verify-mode` accepts `full`, `ca`, or `none` and defaults to `full`.
Certificates must chain to the JVM's
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

Redis replication is asynchronous: a primary can acknowledge a write that a
promoted replica never received. AOF `everysec` also permits loss of recent
local writes on a host failure. Threadmill does not issue a replication barrier
for each job write. At-least-once execution applies to jobs that survive the
configured datastore durability boundary; it does not promise zero loss of
acknowledged enqueues after every Redis failure. `WAIT` improves replication
coverage but does not make Redis strongly consistent. See the
[Redis replication contract](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/).

## Automated topology qualification

`RedisFailoverTest` runs on Redis 7.4 and 8.6 with two existing processing nodes,
queued and in-flight jobs, and an exclusive workflow competing for one key.
It hard-kills a primary in a three-Sentinel topology, hard-kills the owning
primary in a three-primary/three-replica Cluster, and moves the namespace's
slot while workers execute. Each scenario verifies that all 203 seeded jobs
finish and that no exclusive executions overlap. The primary-kill cases use
a fixture-only replication barrier after seeding and blocked claims, so they
test recovery of replicated work, not zero acknowledged-write loss.

Sentinel recovery also runs after deliberately suspending its processes long
enough to enter [TILT protection](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/#tilt-mode).
TILT suspends election activity until the clock/timer has been stable for 30
seconds. The test allows 90 seconds for that guard and election retries, then
requires the same promotion, hold preservation and complete drain. It does not
disable TILT or certify a 30-second failover objective. Preserve Sentinel event
logs when assessing recovery time on a busy or suspended host.

The fixture uses a five-second `down-after-milliseconds` setting. Sentinel's
[replica eligibility check](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/#replica-selection-and-priority)
allows disconnected time up to ten times that setting plus the observed master
down duration. An aggressive one-second setting can exclude the only replicated
candidate when TILT delays the initial down observation for 30 seconds. Test
deployment timing settings with clock/process pauses as well as ordinary kills;
raising a client timeout alone cannot make an ineligible replica promotable.

The complete shared storage contract also runs through standalone, Cluster,
and Sentinel on Redis 7.4. Separate security tests cover authenticated TLS,
mutual TLS, wrong credentials, and an untrusted server certificate. Reports
and process logs are written under `threadmill-store-redis/build/redis-topology/`.

These are bounded tests with independent Redis processes co-located in one
container. They do not certify separate hosts, network partitions, production
certificates, or multi-zone failure domains. Repeat qualification against the
deployment topology and follow the [1.0 soak plan](soak-plan-1.0.md).

Lua digests are computed locally. A script-cache miss loads and executes the
script on the key's current owner; it never requires a `SCRIPT LOAD` broadcast
to an unavailable former primary. Periodic/adaptive topology refresh and the
engine's retained terminal-save retries provide recovery after promotion.

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

## Upgrading existing Redis data

Index format 2 changes pending members from `MODE:id` to `id:MODE` and adds
queue-specific ready, exclusive-barrier, and ordered queue-key indexes. New
stores refuse nonempty legacy storage and incomplete migrations.

1. Stop **every worker and producer**, including scheduled application writes.
2. Take a Redis backup and retain the old application artifacts for recovery.
3. Call `RedisIndexMigration.migrate(client)` with a configured `RedisClient`
   (standalone/Sentinel) or `RedisClusterClient`. For example:

   ```java
   var client = RedisClient.create(System.getenv("THREADMILL_REDIS_URL"));
   try {
     long visited = RedisIndexMigration.migrate(client);
     System.out.println("Visited job records: " + visited);
   } finally {
     client.shutdown();
   }
   ```

4. Start the new workers and producers, then verify queue depth, claim progress,
   and workflow state. Review legacy FAILED records as described in
   [migration](migration.md#upgrading-persisted-failures).

The migrator refuses live registered workers and unknown future formats.
Producer shutdown is an operator responsibility because producers do not
register. It visits state indexes in bounded pages, preserves job bodies and
pending microsecond timestamps, and updates a format marker only after the
complete pass. An interrupted run may be repeated; keep all application writers
stopped throughout retries. The migration owns its connection, while the client
remains caller-owned. For managed Redis, migration credentials need access to
all Threadmill keys and the normal scripting commands.

Mixed old/new workers and in-place downgrade are unsupported. To roll back,
stop all new processes and restore the pre-upgrade backup; reconcile external
side effects before restarting old workers. Processing remains at-least-once,
so handlers must be idempotent.

The format-2 offline upgrade also rebuilds the ordered concurrency-counter
registry, including legacy hashes whose jobs were already retained away. It
routes its offline key scan to the owner of the `{threadmill}` slot. Runtime
cleanup uses bounded ordered pages and atomically verifies zero counters, no
pending jobs, no active holds, and no outstanding workflow members before
removing a hash. An upgrade must still stop all workers and producers.
