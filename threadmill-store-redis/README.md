# threadmill-store-redis

Redis backend for the `JobStore` SPI. Per-job HASH plus auxiliary indexes
kept in sync via atomic Lua scripts. Standalone, Sentinel, and Cluster
topologies are all supported.

## Topology

Choose via `RedisStoreConfig`:

- **Standalone** — single Redis instance, optionally with a password.
- **Sentinel** — master/replica with automatic failover via Sentinels. Redis
  data-node and Sentinel ACL credentials are configured independently; one TLS
  policy covers both connection planes, as required by Lettuce's Sentinel URI.
- **Cluster** — multi-node Cluster; **every engine key uses the single
  `{threadmill}` hash tag** so Lua scripts run inside one slot. This
  supports Cluster topology/failover; it deliberately does not shard job
  keys across masters. ACL authentication and verified TLS apply to every seed.

For example, an authenticated TLS Cluster can be configured directly:

```java
var config = new RedisStoreConfig.Cluster(
    List.of(new RedisStoreConfig.HostAndPort("redis-1", 6380)),
    "master",
    new RedisStoreConfig.Credentials("threadmill", clusterPassword),
    RedisStoreConfig.Tls.verified());
var store = new RedisJobStore(config);
```

`Credentials` also supports password-only authentication. Peer verification is
enabled by `Tls.verified()` and uses the JVM trust material. For custom Lettuce
TLS resources or client options, inject a caller-managed `RedisClient` or
`RedisClusterClient`; closing the store does not close an injected client.
Threadmill topology descriptions and wrapped connection failures never include
ACL usernames or passwords.

Durability is Redis-level. Run with `--appendonly yes`. Out of the box,
Redis is less durable than PostgreSQL — a crash within 1 s of a state
change may lose the state change depending on `appendfsync` setting.
Crash-mid-claim semantics are still correct (orphan recovery runs), the
durability question is whether the engine remembers what it did.

The published module uses Lettuce 6.8.2.RELEASE and forces its Netty graph to
the 4.1.137.Final security floor. Lettuce 6.8 promotes DNS resolution to a
compile dependency, so `netty-resolver-dns` and `netty-codec-dns` are expected
runtime dependencies of this module; do not exclude them as accidental
transitives. See [Dependency security](../docs/dependency-security.md) for the
advisory-specific reachability record.

Threadmill also requires `maxmemory-policy noeviction`. Eviction policies are
cache semantics, not durable job-store semantics: `allkeys-*` can split job
hashes from their indexes/counts, and `volatile-*` can delete TTL-backed
heartbeats, leases, mutexes, or claim locks. `RedisJobStore` validates the
policy on startup unless `RedisSafetyValidation.externallyValidatedMode()` is
used for managed Redis where `CONFIG GET` is unavailable.

## Key layout

Every key lives under `{threadmill}:` so multi-key Lua scripts route to
the same Cluster slot. The `userSegment` encoding is `Base64Url(value)` so
queue / handler / dedup-key user input cannot escape the namespace. Optional
Lua key positions use `{threadmill}:no_key`, never an empty key, so absent
indexes remain in the same slot too.

| Key | Type | Purpose |
|---|---|---|
| `{threadmill}:job:{id}` | HASH | Per-job state. Fields: `body`, `state`, `queue`, `priority`, `handler_signature`, `scheduled_at`, `owner_node_id`, `owner_heartbeat_at`, `last_checkin_at`, `current_state_at`, `created_at`, `workflow_root_id`, `concurrency_key`, `concurrency_mode`, `version`. |
| `{threadmill}:queue:{queue}` | ZSET | ENQUEUED job ids per queue. Score `-priority` so `ZRANGE LIMIT 0 N` returns highest-priority first; Redis breaks equal-score ties lexicographically by the UUIDv7 job-id member. This exactly matches `(priority DESC, id)` across the full `int` priority and timestamp ranges. |
| `{threadmill}:scheduled` | ZSET | SCHEDULED ids scored by `scheduled_at` (millis since epoch). |
| `{threadmill}:awaiting` | ZSET | AWAITING ids scored by state-entry time. |
| `{threadmill}:processing` | ZSET | Global PROCESSING ids scored by `owner_heartbeat_at`. Used by orphan recovery. |
| `{threadmill}:processing:{node}` | ZSET | Per-node PROCESSING ids (same score). Lets `touchOwnerHeartbeat` rescore one ZSET, not scan globally. |
| `{threadmill}:by_handler:{handler}` | SET | Members are job ids. Powers `findByHandlerSignature`. |
| `{threadmill}:by_state_time:{STATE}` | ZSET | Ids scored by `current_state_at`. Used for retention. |
| `{threadmill}:counts` | HASH | State → cardinality. `HINCRBY` inside every state-changing script. **Never** `SCARD` / `ZCARD` for live counts. |
| `{threadmill}:queues` | SET | Active queue names (membership maintained by `claim_commit`). |
| `{threadmill}:queue_keys:{queue}` | HASH | Concurrency key → count of ENQUEUED keyed jobs of that key in the queue. The claim path uses a rotating bounded HSCAN cursor, so one pass stays bounded even at high key cardinality. |
| `{threadmill}:queue_unkeyed:{queue}` | ZSET | ENQUEUED unkeyed job ids, scored like the queue ZSET. The unkeyed claim lane never pages past keyed work. |
| `{threadmill}:queue_enqueued_at:{queue}` | ZSET | Every ENQUEUED job id in the queue, scored by `current_state_at` millis. `oldestEnqueuedAt` (the `threadmill.queue.oldest.enqueued.age` gauge and the dashboard queue view) reads its head with one `ZRANGE 0 0 WITHSCORES`, so the age gauge never scans the priority-ordered queue ZSET. Maintained inside the same atomic scripts as queue membership. |
| `{threadmill}:queue_pauses` | HASH | Paused queue → reason. |
| `{threadmill}:cron_task_namespace:{namespace}` | SET | Cron task names owned by one reconciliation namespace. |
| `{threadmill}:cron_task_namespaces` | SET | Known recurring reconciliation namespaces. |
| `{threadmill}:nodes` | SET | Known NodeIds. |
| `{threadmill}:node:heartbeat:{node}` | STRING with TTL | Key existence is the heartbeat; TTL is the timeout. |
| `{threadmill}:lease:maintenance` | STRING | Maintenance-lease holder; refreshed via `lease_acquire.lua`. |
| `{threadmill}:no_key` | Reserved sentinel | Placeholder for an absent optional Lua `KEYS` entry; never stores data. |
| `{threadmill}:dedup:{queue}:{dedupKey}` | STRING | Dedup record. |
| `{threadmill}:dedup_expiry` | ZSET | Dedup record expiries; maintenance cleanup reads this. |
| `{threadmill}:concurrency:{key}:counters` | HASH | Per-key in-flight counts (`exclusive_in_flight`, `shared_in_flight`). |
| `{threadmill}:concurrency:{key}:pending` | ZSET | Pending concurrency members, scored by enqueue-time micros. |
| `{threadmill}:concurrency:{key}:pending_root:{root}` | ZSET | Per workflow-root mirror of `pending` (same members and scores), kept only for members whose workflow root differs from their own job id. Lets the claim path find active-hold members without scanning the pending population. |
| `{threadmill}:concurrency:{key}:workflows` | HASH | Workflow root id → active outstanding hold count. Presence means the workflow currently owns the key. |
| `{threadmill}:concurrency:{key}:workflow_counts` | HASH | Workflow root id → total non-terminal job count. Maintained incrementally so claim does not scan active jobs. |
| `{threadmill}:concurrency:{key}:claim_lock` | STRING | Short-lived mutex around per-key claim bookkeeping. |

## Development reset

`RedisJobStore.dropThreadmillKeys()` deletes every key matching
`{threadmill}:*` and leaves non-Threadmill Redis keys alone. It is intended for
local or ephemeral environments where deleting all Threadmill jobs, recurring
tasks, dedup records, queue pauses, leases, heartbeats, and concurrency
bookkeeping is acceptable.

Spring Boot can run the same reset before creating an auto-configured Redis
store:

```yaml
threadmill:
  store:
    redis:
      reset-on-start: true
      allow-destructive-reset: true
```

Stop other Threadmill nodes before using this in a shared Redis deployment.
This is not a production cleanup or migration mechanism.

## Lua script inventory

Located under `src/main/resources/com/hemju/threadmill/store/redis/lua/`,
loaded once at startup by `LuaScripts`. Each script is atomic at the Redis
server (single-threaded execution).

| Script | What it does |
|---|---|
| `insert.lua` | Atomically insert one job: HSET the hash, ZADD active index, ZADD by-state-time, SADD by-handler, HINCRBY counts. EXISTS-check guards against duplicates. |
| `insert_all.lua` | Atomic batch insert. Two-pass: first EXISTS-checks every job id; if all clear, applies every HSET / ZADD / HINCRBY in one script. Either every row lands or none do. |
| `enqueue_if_absent.lua` | Producer-side dedup: insert iff `(queue, dedupKey)` isn't already mapped to an active job. |
| `save_atomic.lua` | Version-matched conditional update — the optimistic-lock save. |
| `claim_commit.lua` | The reliable-fetch claim. Java prepares the PROCESSING body first, then this script verifies version / state / queue membership and commits body, scalars, indexes (queue → processing + per-node), attempts, owner heartbeat, and counts together. Consults concurrency counters, pending members, workflow counts, and workflow holds before committing. A crash before this script leaves the job ENQUEUED; a crash after leaves a complete PROCESSING record for orphan recovery. |
| `touch_heartbeat.lua` | Rescore every owned PROCESSING id in the per-node ZSET. Does not bump optimistic-lock version. |
| `replace_job.lua` | Atomic in-place definition swap for non-running jobs. Moves the row between queue ZSETs if the queue changes. |
| `soft_delete.lua` | Move a job to DELETED, removing it from active indexes and per-handler set, decrementing counts. |
| `mutex_acquire.lua` | Acquire-or-refresh a named mutex with a millisecond-precision lease. One Lua call removes the race window that `SET NX` + `PEXPIRE` would have. |
| `lease_acquire.lua` | Compare-and-renew for the maintenance lease. |
| `lease_release.lua` | Compare-and-delete for the maintenance lease. |
| `dedup_delete.lua` | Compare-and-delete an expired dedup record without erasing a concurrent replacement. |
| `retention_delete.lua` | State-checked hard deletion with atomic index and count cleanup. |
| `queue_prune.lua` | Remove an empty queue from the registry without racing a concurrent insert. |

## Reliable-fetch claim

Never a destructive `BLPOP` / `ZPOPMIN`. The flow is:

1. Java gathers candidates from bounded, key-driven lanes — unkeyed heads
   from `{threadmill}:queue_unkeyed:{queue}`, per-concurrency-key
   pending-order head runs discovered through a rotating HSCAN over
   `{threadmill}:queue_keys:{queue}`,
   and active-workflow-hold members via the `pending_root` mirrors — then
   sorts them by queue-ZSET score and UUID member, exactly `(priority DESC,
   id)`. Per-key admission reads
   and queue-score probes are asynchronously pipelined. Each pass is bounded
   by a key-page budget and never scales with backlog depth or requires one
   network round trip per registered key.
2. For each candidate, Java prepares the new body with the `PROCESSING`
   state-history entry appended and the version bumped.
3. `claim_commit.lua` verifies version / state / queue membership plus the
   concurrency admission rules (it is the single admission authority — the
   gathering reads are unlocked approximations) and commits the new body +
   every index update + counts in one atomic call.

A crash before step 3 leaves the job in ENQUEUED. A crash after step 3
leaves a complete PROCESSING record for the orphan-recovery path.

## Capabilities

`supportsRichSearch = false`. Redis cannot do deep ad-hoc metadata search;
features that need it should skip-or-degrade on Redis. `supportsExactCounts`
and `supportsConcurrencyGroups` are both `true`.

## Lettuce / connection management

Default client is Lettuce. The store either owns the client (constructed
from a `RedisURI` or `RedisStoreConfig`) or borrows one (passed in by the
host). `close()` shuts down the connection; the client is shut down too
only if the store owns it.

## Lua return-value conventions

Mixed-type Lua returns confuse Lettuce's `CommandOutput`. Pick one shape
per script:

- Always-string (`OK` / `STALE` / `EXISTS` / …) → `ScriptOutputType.VALUE`.
- Always-int → `ScriptOutputType.INTEGER`.
- Always list of strings → `ScriptOutputType.MULTI`.

## Build

```
./gradlew :threadmill-store-redis:test
```

Runs against a `redis:7-alpine` Testcontainer. 72 tests: 61 contract + 9
regression + 2 keys-tests.
