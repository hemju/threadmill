# Configuration Reference

All durations must be positive. Queue, cron task, mutex, metadata, and tag names
must be nonblank, at most 128 characters, and contain no control characters.

| Setting | Default | Notes |
|---|---:|---|
| `workerCount` | `10` | Workers in the default lane. |
| `pollInterval` | `500ms` | Dispatcher fallback idle poll interval. Scheduled promotion and same-JVM producers sharing a `LocalWakeBus` can dispatch sooner. |
| `claimHeartbeat` | `15s` | Owner heartbeat refresh cadence. |
| `maintenancePollInterval` | `1s` | Master-only maintenance tick for recurring materialization, scheduled promotion, and orphan reclaim. |
| `retentionInterval` | `1h` | Master-only cadence for succeeded-job cleanup, expired dedup cleanup, and stale node-heartbeat cleanup. |
| `heartbeatTimeout` | `60s` | Orphan and node heartbeat expiry. |
| `maintenanceLeaseDuration` | `60s` | Store-backed leadership lease. Must be greater than `claimHeartbeat`. |
| `nodeHeartbeatRetention` | `10m` | How long old node registry entries remain visible after their last heartbeat. Must be greater than `heartbeatTimeout`. |
| `jobTimeout` | `5m` | Per-job execution timeout. |
| `maxConsecutiveDispatcherFailures` | `10` | Circuit-breaker pause threshold. |
| `retryInitialBackoff` | `5s` | Default retry backoff. |
| `defaultMaxAttempts` | `5` | Attempts including the first run. |
| `claimBatchSize` | `10` | Maximum jobs claimed per poll. |
| `defaultQueue` | `default` | Queue used by scheduler convenience methods. |
| `storeOutagePollInterval` | `5s` | Probe interval while paused by store outage. |
| `shutdownGracePeriod` | `10s` | Time to wait for in-flight jobs on close. |
| `checkInMinInterval` | `5s` | Minimum interval between persisted check-in/progress/log flushes for one job. |
| `noProgressTimeout` | `15m` | Timeout after the most recent check-in when a handler stops making progress. |
| `queueFamilyDiscoveryInterval` | `1s` | Core API setting for queue-family discovery cadence. Spring binds this as `threadmill.queue-family.discovery-interval`. |
| `queueFamilyRetentionAfterEmpty` | `30s` | Core API setting for queue-family empty-queue retention. Spring binds this as `threadmill.queue-family.retention-after-empty`. |
| `logMaxRatePerSecond` | `100` | Accepted per-job log entries per second. Extra entries are dropped. |
| `logMaxEntries` | `1000` | Maximum stored per-job log entries. |
| `logMaxBytes` | `256KB` | Maximum stored per-job log message bytes. |
| `maxDedupTtl` | `30d` | Maximum producer-side deduplication TTL. |

## Queue-Family Properties

Spring Boot binds these under `threadmill.queue-family.*`.

| Setting | Default | Notes |
|---|---:|---|
| `threadmill.queue-family.discovery-interval` | `1s` | How often a pattern lane discovers matching enqueued queues and refreshes cached weights. |
| `threadmill.queue-family.retention-after-empty` | `30s` | How long an empty queue remains in a lane working set before being dropped. |

Spring Boot binds most core values directly under `threadmill.*`, for example
`threadmill.worker-count`, `threadmill.claim-batch-size`, and
`threadmill.maintenance-lease-duration`.

## Redis Store Properties

Spring can auto-create a Redis store when `threadmill.store.redis.*` is set.

| Setting | Default | Notes |
|---|---:|---|
| `threadmill.store.redis.mode` | `standalone` | `standalone`, `sentinel`, or `cluster`. |
| `threadmill.store.redis.uri` | | Standalone Redis URI. |
| `threadmill.store.redis.sentinel.master-name` | | Sentinel master name. |
| `threadmill.store.redis.sentinel.nodes` | | Sentinel node list in `host:port` form. |
| `threadmill.store.redis.sentinel.password` | | Optional password. |
| `threadmill.store.redis.cluster.nodes` | | Cluster seed nodes in `host:port` form. |
| `threadmill.store.redis.cluster.read-policy` | `master` | Only master reads are allowed for engine correctness. |
| `threadmill.store.redis.no-eviction-externally-validated` | `false` | Set only for managed Redis where `CONFIG GET maxmemory-policy` is unavailable and operators have verified `noeviction` out of band. |

Redis Cluster uses one `{threadmill}` hash slot. See
[redis-topologies.md](redis-topologies.md).

## Spring Recurring Properties

| Setting | Default | Notes |
|---|---:|---|
| `threadmill.spring.recurring-namespace` | `spring.application.name` | Namespace whose annotation-driven recurring tasks are reconciled at startup. If neither value is set, Threadmill only upserts discovered tasks and does not delete stale ones. |
