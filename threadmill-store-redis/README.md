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

Threadmill also requires `maxmemory-policy noeviction`. Eviction policies are
cache semantics, not durable job-store semantics: `allkeys-*` can split job
hashes from their indexes/counts, and `volatile-*` can delete TTL-backed
heartbeats, leases, mutexes, or claim locks. `RedisJobStore` validates the
policy on startup unless `RedisSafetyValidation.externallyValidatedMode()` is
used for managed Redis where `CONFIG GET` is unavailable.

## Key layout

Every key lives under `{threadmill}:` so multi-key Lua scripts route to
the same Cluster slot. The `userSegment` encoding is `Base64Url(value)` so
queue / handler / dedup-key user input cannot escape the namespace.

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
| `{threadmill}:layout:queue_enqueued_at` | STRING | Marker that the age index has been backfilled from a pre-index layout (v0.2.1 and earlier). See *Layout upgrades*. |
| `{threadmill}:layout:queue_priority` | STRING | `rescored` after an exact pass while legacy-scoring nodes may remain; `priority_only_v1` after the final pass. See *Layout upgrades*. |
| `{threadmill}:queue_pauses` | HASH | Paused queue → reason. |
| `{threadmill}:cron_task_namespace:{namespace}` | SET | Cron task names owned by one reconciliation namespace. |
| `{threadmill}:cron_task_namespaces` | SET | Known recurring reconciliation namespaces. |
| `{threadmill}:nodes` | SET | Known NodeIds. |
| `{threadmill}:node:heartbeat:{node}` | STRING with TTL | Key existence is the heartbeat; TTL is the timeout. |
| `{threadmill}:node:layout:{node}` | STRING with heartbeat TTL | Monotonic Redis layout version maintained by a live node: `1` knows the age index but writes legacy priority scores; `>=2` maintains both current layouts. Missing or malformed means v0.2.1 or earlier. |
| `{threadmill}:lease:maintenance` | STRING | Maintenance-lease holder; refreshed via `lease_acquire.lua`. |
| `{threadmill}:dedup:{queue}:{dedupKey}` | STRING | Dedup record. |
| `{threadmill}:dedup_expiry` | ZSET | Dedup record expiries; maintenance cleanup reads this. |
| `{threadmill}:concurrency:{key}:counters` | HASH | Per-key in-flight counts (`exclusive_in_flight`, `shared_in_flight`). |
| `{threadmill}:concurrency:{key}:pending` | ZSET | Pending concurrency members, scored by enqueue-time micros. |
| `{threadmill}:concurrency:{key}:pending_root:{root}` | ZSET | Per workflow-root mirror of `pending` (same members and scores), kept only for members whose workflow root differs from their own job id. Lets the claim path find active-hold members without scanning the pending population. |
| `{threadmill}:concurrency:{key}:workflows` | HASH | Workflow root id → active outstanding hold count. Presence means the workflow currently owns the key. |
| `{threadmill}:concurrency:{key}:workflow_counts` | HASH | Workflow root id → total non-terminal job count. Maintained incrementally so claim does not scan active jobs. |
| `{threadmill}:concurrency:{key}:claim_lock` | STRING | Short-lived mutex around per-key claim bookkeeping. |

## Layout upgrades

When upgrading from v0.2.1, roll workers out normally. Current nodes advertise
monotonic layout version `2` next to their heartbeat (future higher versions
remain compatible); the layout markers remain
intermediate until every live legacy-node heartbeat is gone and a final exact
pass completes. Stop any producer-only process on the old release before the
rollout completes because it has no node heartbeat and therefore cannot be
detected. If both layouts need work on the same start, Threadmill rescans each
primary queue once for both the priority score and age index. Start and finish
messages include the member count and key so a large upgrade is visible in
application logs.

### Queue priority score

v0.2.1 encoded both priority and enqueue time into one floating-point ZSET
score. A one-level priority difference was only `10^13` microseconds, so a
lower-priority job more than about 115.74 days older could sort first. The
current layout scores only `-priority`; all `int` values are exactly
representable, and the UUIDv7 member provides the contract's job-id
tie-break.

When `{threadmill}:layout:queue_priority` is absent or `rescored`, store
construction pages every registered queue and unkeyed-queue index with
`ZSCAN`. One bounded Lua call per page rechecks membership and current job
state, reads the current priority, applies `ZADD XX`, and prunes dangling or
non-ENQUEUED members. A job claimed between `ZSCAN` and the update cannot be
resurrected, and a concurrently replaced priority cannot be overwritten by a
stale Java-side read. Malformed priority data fails startup with the job id.

The marker becomes `rescored` after the first exact pass. It stays there while
any live node advertises an older layout version. After the last such heartbeat
expires, a current node performs one final exact pass in the background and
writes `priority_only_v1`; a cluster-wide, lease-backed mutex ensures only one
node performs that pass, and no restart is needed. Later starts use two bounded
score-range checks per queue and repair legacy-score drift outside the valid
integer score range. The probe misses only the one priority whose legacy
priority and timestamp terms cancel into that range (priority 177 around 2026)
for a roughly 71.6-minute enqueue window; it is defence for old producer-only
processes that cannot be observed through heartbeats, not proof of exactness.

**Mixed-version guarantees.** Old nodes may temporarily reintroduce legacy
scores while the marker is `rescored`, so strict priority ordering is restored
by each new-node start and finally guaranteed after the terminal pass. The
score layout never controls job state: mixed versions do not lose, duplicate,
or resurrect jobs, and claims remain atomic. Stop old producer-only processes
before completion as described above.

**API compatibility.** The deprecated `RedisKeys.queueScore(int, long)`
overload remains for the v0.2.2 patch release and is marked for removal. Its
enqueue-time argument is ignored, so callers compile with the new priority-only
ordering semantics; migrate to `queueScore(int)`.

### Per-queue age index

The age index `{threadmill}:queue_enqueued_at:{queue}` did not exist before
v0.2.2. The `{threadmill}:layout:queue_enqueued_at` STRING records how far
the upgrade has progressed, and every store construction advances it before
serving a read:

- **Absent** (data written by v0.2.1 or earlier) or **`backfilled`** (a
  rolling upgrade in progress): every registered queue is reconciled exactly.
  The registry is paged with `SSCAN`, each queue ZSET with `ZSCAN`; members
  get their `current_state_at` through pipelined `HGET`s and are `ZADD`ed;
  a second pass prunes index members the queue ZSET no longer holds through a
  per-page compare-and-remove Lua call. The walk runs from Java, never as one
  Lua call, so it does not hold the server, and it is idempotent, so several
  new nodes starting together may all run it. The state becomes
  `backfilled`.
- **`backfilled` → `complete`** as soon as no old-release node is live.
  New-release nodes write `{threadmill}:node:layout:{nodeId}` next to their
  heartbeat with the same TTL, so a live heartbeat without it identifies a
  release that predates this index. The transition is attempted at every start and, so the
  last old node's exit needs no restart, from `oldestEnqueuedAt` once that
  node's heartbeat has expired: a final exact reconciliation in a background
  virtual thread, then the state write. Until then the read pays one `GET`
  and a node-registry probe; afterwards nothing.
- **`complete`**: two `ZCARD`s per queue at start, no scan, and a re-walk only
  where the index and queue cardinalities disagree.

**Upgrade procedure.** Roll the new release out node by node as described
above. Stop any producer-only process still on the old release (a Spring application
that enqueues but runs no `ProcessingNode`) before the rollout completes:
such a process has no heartbeat, so the store cannot see it, and a job it
enqueues after the layout is `complete` is missing from the index until it
drains or until a start finds the cardinalities disagree.

**Mixed-version guarantees.** Job processing is never affected — the claim
path does not read this index. The gauge never reports a job that is no
longer ENQUEUED: `oldestEnqueuedAt` verifies its head against the queue ZSET,
the authority for "ENQUEUED in this queue", and drops a head that has left it
through the same compare-and-remove call, so a retry or promotion that
re-enqueues the job in between cannot lose the valid member. It may
under-report a queue whose oldest job was enqueued by an old-release node
until that job drains, any new-release node starts, or the layout finalizes,
whichever comes first.

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
| `rescore_queue_priority_page.lua` | Atomically rescore one bounded migration page from current job hashes, with membership/state rechecks, stale-member pruning, and optional age-index maintenance. |
| `prune_stale_age_index_members.lua` | Compare-and-remove a bounded age-index page against authoritative queue membership. |
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
