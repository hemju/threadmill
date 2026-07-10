# Configuration Reference

All durations must be positive. Queue, cron task, mutex, metadata, and tag names
must be nonblank, at most 128 characters, and contain no control characters.

| Setting | Default | Notes |
|---|---:|---|
| `workerCount` | `10` | Workers in the default lane. |
| `pollInterval` | `500ms` | Dispatcher fallback idle poll interval. Scheduled promotion and same-JVM producers sharing a `LocalWakeBus` can dispatch sooner. |
| `claimHeartbeat` | `15s` | Owner heartbeat refresh cadence. |
| `maintenancePollInterval` | `1s` | Master-only maintenance tick for recurring materialization, scheduled promotion, and orphan reclaim. |
| `retentionInterval` | `1h` | Master-only cadence for retention sweeps: hard-delete of old SUCCEEDED/FAILED/DELETED/QUARANTINED jobs, expired dedup cleanup, and stale node-heartbeat cleanup. |
| `succeededRetention` | `7d` | Age after which SUCCEEDED jobs are hard-deleted. |
| `failedRetention` | `30d` | Age after which FAILED jobs are hard-deleted. |
| `deletedRetention` | `7d` | Age after which DELETED jobs are hard-deleted. |
| `quarantinedRetention` | `30d` | Age after which QUARANTINED jobs are hard-deleted. |
| `heartbeatTimeout` | `60s` | Orphan and node heartbeat expiry. |
| `maintenanceLeaseDuration` | `60s` | Store-backed leadership lease. Must be greater than `claimHeartbeat`. |
| `nodeHeartbeatRetention` | `10m` | How long old node registry entries remain visible after their last heartbeat. Must be greater than `heartbeatTimeout`. |
| `jobTimeout` | `5m` | Per-job execution timeout. |
| `maxConsecutiveDispatcherFailures` | `10` | Circuit-breaker pause threshold. |
| `retryInitialBackoff` | `5s` | Default retry backoff. |
| `defaultMaxAttempts` | `5` | Attempts including the first run. |
| `claimBatchSize` | `10` | Maximum jobs claimed per poll. |
| `defaultQueue` | `default` | Queue a lane-less node polls (the default lane). NOT the queue `Scheduler` convenience methods write to — those always use the literal `"default"`; set a matching lane or pass an explicit queue. |
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

## In-Memory Store Property

Spring does not silently select volatile storage. When neither a durable store
nor an application-provided `JobStore` is available, startup fails unless the
development/test-only in-memory store is explicitly enabled.

| Setting | Default | Notes |
|---|---:|---|
| `threadmill.store.memory.enabled` | `false` | Set to `true` only when losing all jobs on process restart is acceptable. |

## PostgreSQL Store Properties

Spring auto-creates a PostgreSQL store when a `DataSource` bean exists and
`threadmill-store-postgres` is on the classpath, unless the application defines
its own `JobStore` bean.

| Setting | Default | Notes |
|---|---:|---|
| `threadmill.store.postgres.schema-mode` | `migrate` | `migrate`, `validate`, `none`, or `drop-and-migrate`. Auto-configured Postgres stores run this action before `PostgresJobStore` is created. |
| `threadmill.store.postgres.allow-destructive-schema-reset` | `false` | Must be `true` for `drop-and-migrate`. This drops only Threadmill-owned schema objects and destroys stored jobs. |

See [postgres-schema.md](postgres-schema.md) for manual DDL, validation, and
reset guidance.

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
| `threadmill.store.redis.reset-on-start` | `false` | Delete all keys under Threadmill's Redis namespace before creating the auto-configured store. Intended for disposable development environments only. |
| `threadmill.store.redis.allow-destructive-reset` | `false` | Must be `true` for `reset-on-start`. This destroys all Threadmill jobs, recurring tasks, dedup records, queue pauses, leases, and concurrency bookkeeping in the Redis namespace. |

Redis Cluster uses one `{threadmill}` hash slot. See
[redis-topologies.md](redis-topologies.md).

## Remote Wake Properties

Remote wake is a latency hint for cross-node workers. Polling remains the
correctness fallback.

| Setting | Default | Notes |
|---|---:|---|
| `threadmill.remote-wake.enabled` | `true` | Auto-create Postgres `LISTEN`/`NOTIFY` or Redis Pub/Sub wake hints for Spring auto-configured durable stores. In-memory and custom stores stay local-wake only unless the application provides a `RemoteWakeChannel` bean. |
| `threadmill.remote-wake.channel` | backend default | Optional channel override. Defaults to `threadmill_wake` for Postgres and `{threadmill}:wake` for Redis. Set this when multiple isolated Threadmill deployments share one Postgres database or Redis instance. |

## Spring Recurring Properties

| Setting | Default | Notes |
|---|---:|---|
| `threadmill.spring.enqueue-mode` | `after_commit` | `after_commit`, `join_transaction`, or `immediate`. `join_transaction` is Spring + Postgres only. |
| `threadmill.spring.recurring-namespace` | `spring.application.name` | Namespace whose annotation-driven recurring tasks are reconciled at startup. If neither value is set, Threadmill only upserts discovered tasks and does not delete stale ones. |
