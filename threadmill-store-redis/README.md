# threadmill-store-redis

Redis backend for the `JobStore` SPI. Per-job HASH plus auxiliary indexes
kept in sync via atomic Lua scripts. Standalone, Sentinel, and Cluster
topologies are all supported.

## Topology

Choose via `RedisStoreConfig`:

- **Standalone** — single Redis instance, optionally with a password.
- **Sentinel** — master/replica with automatic failover via Sentinels.
- **Cluster** — multi-node Cluster; **every engine key uses the single
  `{threadmill}` hash tag** so Lua scripts run inside one slot. This
  supports Cluster topology/failover; it deliberately does not shard job
  keys across masters.

Durability is Redis-level. Run with `--appendonly yes`. Out of the box,
Redis is less durable than PostgreSQL — a crash within 1 s of a state
change may lose the state change depending on `appendfsync` setting.
Crash-mid-claim semantics are still correct (orphan recovery runs), the
durability question is whether the engine remembers what it did.

## Key layout

Every key lives under `{threadmill}:` so multi-key Lua scripts route to
the same Cluster slot. The `userSegment` encoding is `Base64Url(value)` so
queue / handler / dedup-key user input cannot escape the namespace.

| Key | Type | Purpose |
|---|---|---|
| `{threadmill}:job:{id}` | HASH | Per-job state. Fields: `body`, `state`, `queue`, `priority`, `handler_signature`, `scheduled_at`, `owner_node_id`, `owner_heartbeat_at`, `last_checkin_at`, `current_state_at`, `created_at`, `workflow_root_id`, `concurrency_key`, `concurrency_mode`, `version`. |
| `{threadmill}:queue:{queue}` | ZSET | ENQUEUED job ids per queue. Score `-priority * 1e13 + enqueue_micros` so `ZRANGE LIMIT 0 N` returns highest-priority, oldest-first. |
| `{threadmill}:scheduled` | ZSET | SCHEDULED ids scored by `scheduled_at` (millis since epoch). |
| `{threadmill}:awaiting` | ZSET | AWAITING ids scored by state-entry time. |
| `{threadmill}:processing` | ZSET | Global PROCESSING ids scored by `owner_heartbeat_at`. Used by orphan recovery. |
| `{threadmill}:processing:{node}` | ZSET | Per-node PROCESSING ids (same score). Lets `touchOwnerHeartbeat` rescore one ZSET, not scan globally. |
| `{threadmill}:by_handler:{handler}` | SET | Members are job ids. Powers `findByHandlerSignature`. |
| `{threadmill}:by_state_time:{STATE}` | ZSET | Ids scored by `current_state_at`. Used for retention. |
| `{threadmill}:counts` | HASH | State → cardinality. `HINCRBY` inside every state-changing script. **Never** `SCARD` / `ZCARD` for live counts. |
| `{threadmill}:queues` | SET | Active queue names (membership maintained by `claim_commit`). |
| `{threadmill}:queue_pauses` | HASH | Paused queue → reason. |
| `{threadmill}:nodes` | SET | Known NodeIds. |
| `{threadmill}:node:heartbeat:{node}` | STRING with TTL | Key existence is the heartbeat; TTL is the timeout. |
| `{threadmill}:lease:maintenance` | STRING | Maintenance-lease holder; refreshed via `lease_acquire.lua`. |
| `{threadmill}:dedup:{queue}:{dedupKey}` | STRING | Dedup record. |
| `{threadmill}:dedup_expiry` | ZSET | Dedup record expiries; maintenance cleanup reads this. |
| `{threadmill}:concurrency:{key}:counters` | HASH | Per-key in-flight counts (`exclusive_in_flight`, `shared_in_flight`). |
| `{threadmill}:concurrency:{key}:pending` | ZSET | Pending concurrency members, scored by enqueue-time micros. |
| `{threadmill}:concurrency:{key}:workflows` | HASH | Workflow root id → active outstanding hold count. Presence means the workflow currently owns the key. |
| `{threadmill}:concurrency:{key}:workflow_counts` | HASH | Workflow root id → total non-terminal job count. Maintained incrementally so claim does not scan active jobs. |
| `{threadmill}:concurrency:{key}:claim_lock` | STRING | Short-lived mutex around per-key claim bookkeeping. |

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

## Reliable-fetch claim

Never a destructive `BLPOP` / `ZPOPMIN`. The flow is:

1. Java reads candidate ids from `{threadmill}:queue:{queue}` (ZRANGE).
2. For each candidate, Java prepares the new body with the `PROCESSING`
   state-history entry appended and the version bumped.
3. `claim_commit.lua` verifies version / state / queue membership and
   commits the new body + every index update + counts in one atomic call.

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
