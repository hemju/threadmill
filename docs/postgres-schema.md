# PostgreSQL Schema

Threadmill's PostgreSQL store targets PostgreSQL 18+. The store constructor
checks `server_version_num` and refuses older servers before any job query runs.

## Spring Boot Schema Modes

When Spring Boot auto-configures `PostgresJobStore` from an application
`DataSource`, it handles the Threadmill schema before constructing the store.
Applications that define their own `JobStore` bean own schema handling
themselves.

| Property | Default | What |
|---|---|---|
| `threadmill.store.postgres.schema-mode` | `migrate` | `migrate`, `validate`, `none`, or `drop-and-migrate`. |
| `threadmill.store.postgres.allow-destructive-schema-reset` | `false` | Required for `drop-and-migrate`. |

`migrate` applies every pending Threadmill migration under a PostgreSQL advisory
lock. `validate` fails startup unless `threadmill_schema_history` exactly
matches the migrations shipped in the running library. `none` performs no DDL or
validation. `drop-and-migrate` drops Threadmill-owned objects and recreates the
schema; it destroys stored jobs and is intended only for disposable dev/test
databases.

## Manual DDL

The full clean-install DDL is the checked-in migration SQL:

[`V1__baseline.sql`](../threadmill-store-postgres/src/main/resources/com/hemju/threadmill/store/postgres/migrations/V1__baseline.sql)
plus the additive migrations that followed it (currently
[`V2__cron_task_overrides.sql`](../threadmill-store-postgres/src/main/resources/com/hemju/threadmill/store/postgres/migrations/V2__cron_task_overrides.sql),
[`V3__integrity_constraints.sql`](../threadmill-store-postgres/src/main/resources/com/hemju/threadmill/store/postgres/migrations/V3__integrity_constraints.sql),
[`V4__cron_state_timing_fingerprint.sql`](../threadmill-store-postgres/src/main/resources/com/hemju/threadmill/store/postgres/migrations/V4__cron_state_timing_fingerprint.sql),
and
[`V5__cron_state_nudge.sql`](../threadmill-store-postgres/src/main/resources/com/hemju/threadmill/store/postgres/migrations/V5__cron_state_nudge.sql),
and
[`V6__cron_task_exclusive.sql`](../threadmill-store-postgres/src/main/resources/com/hemju/threadmill/store/postgres/migrations/V6__cron_task_exclusive.sql)).

For applications that want to apply SQL from their own deployment system,
`MigrationRunner` can emit the same statements:

```java
String sql = new MigrationRunner(dataSource).emitCleanInstallSql();
```

Apply that output to a clean PostgreSQL 18+ database. It creates
`threadmill_schema_history`, the Threadmill tables, indexes, trigger function,
trigger, and one migration-history row per shipped migration.

For an existing schema, use pending SQL instead:

```java
String sql = new MigrationRunner(dataSource).emitPendingSql();
```

`emitPendingSql()` reads `threadmill_schema_history` and returns only migrations
that have not been recorded yet. Teams using Flyway or Liquibase can run this
output during deployment and then start the application with:

```yaml
threadmill:
  store:
    postgres:
      schema-mode: validate
```

## Upgrades

Threadmill uses a deliberately small migration runner, not Flyway or Liquibase.
The default `migrate` mode validates every already-applied migration description
and checksum before it applies pending files; an edited shipped migration fails
startup instead of allowing fleet schema drift.
Migrations are classpath SQL files named `V<n>__<description>.sql`, and the
runner applies them in numeric order. The shipped list is explicit for
native-image friendliness.

The entire v1 schema is consolidated into one `V1__baseline.sql`, so a fresh
database installs in a single step. After release, new schema changes must be
additive `V2__*.sql`, `V3__*.sql`, and so on — the baseline is never edited
again.

## Reinitialization

For local or ephemeral databases, Spring can recreate the schema:

```yaml
threadmill:
  store:
    postgres:
      schema-mode: drop-and-migrate
      allow-destructive-schema-reset: true
```

This drops only Threadmill-owned tables and functions, then runs migrations. It
does not drop the database or schema, but it does delete all Threadmill jobs,
cron definitions, dedup records, queue pauses, leases, and metrics counters.
Use normal forward migrations for production.

### Execution update revision (V7)

`V7__execution_revision.sql` adds `threadmill_jobs.execution_revision` as a
non-negative bigint with default zero. Progress/log/check-in writes compare and
advance it without changing the state version. Claim resets it. Existing rows
upgrade in place; stop old workers before starting workers that use this revision.

Migration `V8__maintenance_scan.sql` adds `(state, id)` for resumable maintenance
pages. Recovery never uses offset pagination across a population it mutates.

Migration `V9__queue_monitoring.sql` adds `threadmill_queue_counts`, with up to
16 counter shards per queue, and an ENQUEUED partial index on
`(queue, current_state_at)`. Queue depth and queue discovery aggregate the counter
table; they no longer scan queued jobs. Individual shards may be negative; only
the sum is meaningful. Insert, state change, queue replacement and delete update
the counters in the same transaction as the job. The migration locks writes while
backfilling existing jobs and installing the trigger. Schedule migration downtime
for the backfill and index build on large installations.

Use `:threadmill-soak:benchmarkPostgresMonitoring` for the opt-in PostgreSQL 18
benchmark with 10k/100k/1m jobs, four pooled claimers and a concurrent monitoring
query mix. It writes `threadmill-soak/build/soak/postgres-monitoring/claims.csv`.
This short benchmark measures claim cost; it does not establish durability or
long-running retention capacity.

V9 also indexes `(state, current_state_at DESC, id)` for state-only dashboard
history pages. Matching the full ordering avoids a large sort when many jobs
share one transition timestamp, a case exercised by the monitoring benchmark.

`V10__idle_concurrency_groups.sql` adds a partial ordered index for zero-count
groups. Maintenance locks a bounded page and removes a group only after checking
for active workflow holds and nonterminal jobs. Group acquisition retries if a
conflict row disappears before its row lock is obtained, so cleanup cannot leave
a new claim without the group lock.
