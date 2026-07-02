# threadmill-store-postgres

PostgreSQL 18+ backend for the `JobStore` SPI. Single consolidated baseline
migration, partial indexes on the hot paths, `SELECT … FOR UPDATE SKIP LOCKED`
claim, trigger-maintained per-state counts, and store-backed maintenance
leadership.

## Version floor — PostgreSQL 18 only

The store refuses to start against pre-18 servers. `PostgresJobStore`'s
constructor runs `SHOW server_version_num` and throws `JobEngineFatalException`
if the value is below `180000`. The regression test (`PostgresVersionGateTest`)
boots a `postgres:17-alpine` container and asserts the refusal.

This is deliberate. We use PG18 syntax confidently — partial indexes the
planner won't second-guess, `RETURNING` where it saves a round-trip, modern
trigger semantics — without back-compat shims.

## Schema

Created by the consolidated `V1__baseline.sql` plus future additive migrations under
`src/main/resources/com/hemju/threadmill/store/postgres/migrations/`. The
`MigrationRunner` bootstraps `threadmill_schema_history` itself (`CREATE IF NOT
EXISTS`) before recording shipped migrations.

### `threadmill_jobs`

Source of truth for one row per job. Body column holds the wire form; scalar
columns are denormalised for the engine's hot queries.

| Column | Type | Notes |
|---|---|---|
| `id` | `UUID` | PRIMARY KEY. UUIDv7, client-generated. |
| `state` | `TEXT` | The state-machine state (`ENQUEUED`, `PROCESSING`, etc.). |
| `queue` | `TEXT` | Queue name. |
| `priority` | `INT` | Higher value claimed first. Defaults to 0. |
| `handler_signature` | `TEXT` | Fully-qualified handler class name. |
| `scheduled_at` | `TIMESTAMPTZ` | Set for `SCHEDULED` jobs. |
| `owner_node_id` | `UUID` | NodeId that currently owns this job (PROCESSING only). |
| `owner_heartbeat_at` | `TIMESTAMPTZ` | Last heartbeat from the owner. |
| `last_checkin_at` | `TIMESTAMPTZ` | Last handler-side `ctx.checkIn()`. |
| `current_state_at` | `TIMESTAMPTZ` | When the row entered its current state. |
| `version` | `BIGINT` | Optimistic-lock version. |
| `body` | `TEXT` | Serialized `JobSnapshot` (JSON wire form). |
| `created_at` | `TIMESTAMPTZ` | When the row was first inserted. |
| `concurrency_key` | `TEXT` | Per-key concurrency identity (Phase 12). |
| `concurrency_mode` | `TEXT` | `EXCLUSIVE` or `SHARED`. |
| `workflow_root_id` | `UUID` | NOT NULL; self for non-workflow jobs. |
| `parent_job_id` | `UUID` | Parent for workflow successors; NULL for standalone jobs. |

### Indexes

| Name | Shape | Used for |
|---|---|---|
| `threadmill_jobs_enqueued_idx` | `(queue, priority DESC, id) WHERE state='ENQUEUED'` | Claim path. |
| `threadmill_jobs_scheduled_idx` | `(scheduled_at) WHERE state='SCHEDULED'` | Due-for-promotion. |
| `threadmill_jobs_processing_idx` | `(owner_heartbeat_at) WHERE state='PROCESSING'` | Oldest-processing-heartbeat metric. |
| `threadmill_jobs_processing_liveness_idx` | `(GREATEST(owner_heartbeat_at, COALESCE(last_checkin_at, owner_heartbeat_at))) WHERE state='PROCESSING'` | Orphan recovery (latest liveness marker: heartbeat or check-in). |
| `threadmill_jobs_handler_idx` | `(handler_signature)` | Find-by-handler. |
| `threadmill_jobs_state_time_idx` | `(state, current_state_at)` | Retention. |
| `threadmill_jobs_dashboard_search_idx` | `(state, queue, handler_signature, current_state_at DESC, id)` | Dashboard/API search. |
| `threadmill_jobs_concurrency_pending_idx` | `(concurrency_key, current_state_at, id) WHERE state IN (ENQUEUED, SCHEDULED, AWAITING)` | Claim-time concurrency pending check. |
| `threadmill_jobs_workflow_outstanding_idx` | `(concurrency_key, workflow_root_id) WHERE state NOT IN (SUCCEEDED, FAILED, DELETED, QUARANTINED)` | Workflow-root outstanding count. |
| `threadmill_jobs_awaiting_parent_idx` | `(parent_job_id, current_state_at, id) WHERE state='AWAITING' AND parent_job_id IS NOT NULL` | Workflow successor promotion. |

### Auxiliary tables

- `threadmill_schema_history` — migration ledger (bootstrapped by `MigrationRunner`).
- `threadmill_nodes` — node-registry heartbeats.
- `threadmill_metadata` — key/value store metadata. Reserved for future use.
- `threadmill_cron_tasks` + `threadmill_cron_task_state` — recurring task identity
  and bookkeeping. Identity (definition) and state (last/next run) are split so
  re-registering a task never resurrects stale timing.
- `threadmill_cron_task_ownership` — namespace-owned recurring tasks for
  startup reconciliation. Deleting a cron task cascades ownership rows.
- `threadmill_mutexes` — cross-cluster named mutexes with a `expires_at` lease.
- `threadmill_leases` — store-backed leadership leases for `MaintenanceCycle`.
- `threadmill_dedup_keys` — producer-side deduplication. Cleanup gated on the
  referenced job being terminal. Indexed on `expires_at` (sweep) and `job_id`
  (FK cascade check and the retention sweep's still-referenced probe).
- `threadmill_concurrency_groups` — per-key in-flight `(exclusive, shared)`
  counts. Updated in the same transaction as the job state transition.
- `threadmill_concurrency_workflow_holds` — workflow-root outstanding counts.
- `threadmill_queue_pauses` — per-queue pause primitive.
- `threadmill_job_counts` — per-state counts, maintained by an AFTER
  INSERT/UPDATE/DELETE trigger on `threadmill_jobs`. **Never** write a
  `COUNT(*)`-over-`threadmill_jobs` query — the regression test
  `perStateCountsReadFromCounterTableNotFromJobsTable` uses `EXPLAIN` to assert
  the plan does not touch the jobs table.

## Claim semantics

```sql
SELECT id, body, version
  FROM threadmill_jobs
 WHERE state = 'ENQUEUED' AND queue = ?
 ORDER BY priority DESC, id
 LIMIT n
 FOR UPDATE SKIP LOCKED;
```

then per-row:

```sql
UPDATE threadmill_jobs
   SET state = 'PROCESSING', owner_node_id = ?, owner_heartbeat_at = ?,
       current_state_at = ?, version = ?, body = ?
 WHERE id = ?;
```

both inside the same JDBC transaction. The `SKIP LOCKED` is the engine's
defence against contention: N nodes claiming concurrently never block each
other on the same row.

## Deadlock retry

Every write is wrapped in `DeadlockRetry.run(...)`. It recognises SQLSTATEs
`40P01` (`deadlock_detected`) and `40001` (`serialization_failure`), retries
with exponential backoff and jitter, and rethrows any other `SQLException`
unchanged.

## Migration runner

`MigrationRunner` is intentionally tiny — no Flyway / Liquibase. Migrations
are SQL files on the classpath under
`com/hemju/threadmill/store/postgres/migrations/`, named `V<n>__<description>.sql`.
The explicit `SHIPPED_MIGRATIONS` list is what makes it native-image friendly
and order-deterministic. New migrations append to the list; the runner does
not classpath-scan.

Hosts that already use Flyway can call `emitPendingSql()` to obtain the SQL
text for still-pending migrations and apply it themselves. Use
`emitCleanInstallSql()` to emit the full clean-install DDL without touching the
database. `validate()` checks that the database has exactly the shipped
migration history. `dropThreadmillObjects()` drops only Threadmill-owned schema
objects for disposable dev/test reinitialization.

Spring Boot auto-configured Postgres stores run `migrate()` by default. Set
`threadmill.store.postgres.schema-mode=validate` when your deployment pipeline
applies DDL separately, `none` to skip schema handling, or `drop-and-migrate`
with `threadmill.store.postgres.allow-destructive-schema-reset=true` for
ephemeral environments. `drop-and-migrate` destroys stored Threadmill jobs and
is not a production upgrade strategy.

## Connection pool

The host owns the pool. `PostgresJobStore` accepts a `javax.sql.DataSource`;
it does not create or close one. Recommended floor for the pool size:
`workerCount + claimBatchSize + headroom` so claim and maintenance never
contend with handler-side queries.

Set `reWriteBatchedInserts=true` on the pgJDBC URL to get the full
round-trip win from `JobStore.insertAll(...)`.

## Build

```
./gradlew :threadmill-store-postgres:test
```

Runs against a `postgres:18-alpine` container via Testcontainers. Requires
Docker / Podman / Colima / OrbStack.
