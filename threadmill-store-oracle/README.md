# threadmill-store-oracle

Oracle Database 19c+ backend for the `JobStore` SPI. It passes the same shared
`AbstractJobStoreContractTest` as the in-memory, PostgreSQL, and Redis stores,
and uses standard JDBC only: applications bring their own Oracle driver.

```kotlin
implementation("com.hemju.threadmill:threadmill-store-oracle:1.1.1")
runtimeOnly("com.oracle.database.jdbc:ojdbc11:23.26.3.0.0") // or the driver your platform mandates
```

```java
DataSource dataSource = ...; // host-owned pool; the store never creates or closes it
new OracleMigrationRunner(dataSource).migrate();
JobStore store = new OracleJobStore(dataSource);
```

With Spring Boot, `threadmill-spring-boot` wires the store from the
application's `DataSource`; see [Oracle schema](../docs/oracle-schema.md) and
[configuration](../docs/configuration.md#oracle-store-properties).

## Requirements, checked at startup

`OracleJobStore`'s constructor refuses to start (with `JobEngineFatalException`)
unless:

| Requirement | Why |
|---|---|
| Oracle Database **19c or later** | 19c is the oldest release in long-term support; the SQL uses only the 19c feature set (no `BOOLEAN`, no `IF NOT EXISTS`, no 23ai syntax). |
| Database character set **`AL32UTF8`** | Job bodies and names must round-trip every Unicode character, including 4-byte ones. `AL32UTF8` is the default since 12.2. |
| Session **`NLS_COMP=BINARY`** (the default) | Keyset predicates and the claim path's key ranges rely on binary comparisons matching index order. |
| An Oracle `DataSource` | A PostgreSQL (or other) `DataSource` fails with a message naming the product. |

The schema user needs `CREATE TABLE`, `CREATE TRIGGER`, and tablespace quota —
nothing else (no `CREATE TYPE`, no `DBMS_*` grants, no Advanced Queuing). A
user without DDL rights can run with schema mode `validate` or `none` after a
DBA applies the emitted DDL; see [Oracle schema](../docs/oracle-schema.md).

The B-tree key limit of an 8 KiB block (≈6,400 bytes) bounds the widest index
(`state`, `queue`, `handler_signature`, time, id ≈ 4.6 KB). Databases with
smaller default blocks are not supported.

## Schema

Installed by `V1__baseline.sql` under
`src/main/resources/com/hemju/threadmill/store/oracle/migrations/`, the same
tables as PostgreSQL apart from the unused `threadmill_metadata`. Type mapping:

| Concept | Oracle type | Notes |
|---|---|---|
| Job / node / workflow ids | `RAW(16)` | Big-endian UUID bytes; RAW compares unsigned byte by byte, which equals `JobId`'s canonical natural order. |
| Timestamps | `TIMESTAMP(9)` holding UTC | Nanosecond-exact for `Instant`, bound as `LocalDateTime` at UTC, so neither the JVM nor the session time zone matters. |
| Job body, recurring payload, pause reason | `CLOB` | The body is the source of truth; scalar columns are denormalised for the hot queries. |
| Booleans | `NUMBER(1)` with a `0/1` check | `BOOLEAN` is 23ai-only. |
| Names | `VARCHAR2(n CHAR)` | Concurrency and dedup keys are `VARCHAR2(256 BYTE)`, matching their UTF-8 byte caps. |

Oracle stores the empty string as `NULL`. The store maps it back where a value
may legitimately be empty (a recurring payload, a mutex holder), and compares
mutex holders with `DECODE`, which treats two `NULL`s as equal.

### Partial indexes as indexed virtual columns

Oracle has no partial indexes. Each PostgreSQL partial index becomes indexed
`VIRTUAL` columns of the form `CASE WHEN <predicate> THEN <column> END` — for
example `keyed_queue`, `keyed_key`, `keyed_at`, `keyed_id` for ENQUEUED keyed
jobs. Rows that do not match the predicate have all-`NULL` keys, which an Oracle
B-tree does not store, so each index holds only the rows its queries need.
Virtual columns (rather than expression indexes) let queries name the columns
directly and let the 19c optimizer read index order: a per-key top-n stops after
n entries instead of sorting the key's whole range.

| Index | Serves |
|---|---|
| `threadmill_jobs_unkeyed_idx` | Unkeyed claim lane, `(queue, -priority, id)`. |
| `threadmill_jobs_keyed_idx` | Keyed claim lane: per-queue loose key scan and per-key heads in `(current_state_at, id)` order. |
| `threadmill_jobs_pending_idx`, `threadmill_jobs_exclusive_idx` | Claim-time admission (earlier pending / earlier EXCLUSIVE job in a key). |
| `threadmill_jobs_workflow_idx` | Workflow-hold outstanding counts and hold members. |
| `threadmill_jobs_scheduled_idx`, `threadmill_jobs_liveness_idx`, `threadmill_jobs_heartbeat_idx` | Promotion, orphan recovery, monitoring. |
| `threadmill_jobs_awaiting_idx` | Workflow successor promotion. |
| `threadmill_jobs_queue_age_idx` | Oldest ENQUEUED job per queue. |
| `threadmill_jobs_retention_idx`, `threadmill_jobs_state_id_idx`, `threadmill_jobs_state_page_idx`, `threadmill_jobs_search_idx`, `threadmill_jobs_handler_idx` | Retention, maintenance scans, dashboard. |
| `threadmill_concurrency_idle_idx` | Idle concurrency-group reclamation, over a `RAW` binary sort key. |

Per-state and per-queue counts live in `threadmill_job_counts` and
`threadmill_queue_counts`, maintained by one row trigger and sharded 16 ways by
session id; only the per-state/per-queue `SUM` is meaningful.

## How the claim works

`claimReady` locks candidates with `FOR UPDATE SKIP LOCKED`, so contending
workers never collide and never wait:

- **Unkeyed lane** — an index-ordered locking cursor. Oracle rejects a row limit
  on a locking query (`ORA-02014`), so the store fetches only its page; Oracle
  locks `SKIP LOCKED` rows as they are fetched, which a regression test pins.
- **Keyed lane** — distinct keys come from a recursive `MIN` loose scan (one
  index probe per key, 128 keys per pass with a rotating per-queue cursor); each
  key contributes its earliest heads through a `ROWNUM` top-n branch. Candidates
  are then locked by primary key with `SKIP LOCKED`.
- **Admission** — `EXISTS` range probes bounded at each candidate's own
  `current_state_at` decide the leapfrog rule without reading a key's backlog.

Lateral joins are deliberately avoided: inside a correlated top-n lateral view
Oracle keeps a sort over each key's whole range, and a collection-driven lateral
top-n returned wrong results on a current release. The hot statements carry
index hints, and `OracleQueryPlanTest` runs them with typed binds and asserts
the executed plans under the 19c optimizer feature set.

## Other Oracle specifics

- **Transactions.** Every self-owned write commits explicitly and restores the
  connection's auto-commit mode, so pools that hand out `autoCommit=false`
  connections work. Deadlocks (`ORA-00060`) and serialization failures
  (`ORA-08177`) are retried. Oracle rolls back only a failed statement, not the
  transaction; the deduplicated enqueue uses a savepoint to coalesce onto a
  concurrent producer's job inside a joined transaction too.
- **CLOB binding.** Values longer than 8,000 characters are written through a
  temporary `Clob`, freed after the statement. Binding a long string directly
  made the Oracle driver split a surrogate pair (an emoji) that straddled its
  ~32 KiB chunk boundary, corrupting the body.
- **Locale.** The driver derives `NLS_LANGUAGE`, and so `NLS_SORT`, from the
  client JVM locale. Text keyset pages therefore order in binary explicitly
  (`NLSSORT(..., 'NLS_SORT = BINARY')` or a `RAW` sort key), so a German or
  French JVM pages exactly like an English one.
- **Server time.** Leases and mutexes compare `SYS_EXTRACT_UTC(SYSTIMESTAMP)`
  on the server, so a node with a skewed clock cannot steal one.
- **No remote wake.** Oracle has no lightweight equivalent of `LISTEN`/`NOTIFY`
  (`DBMS_ALERT` serializes signalling transactions). Same-JVM wakes still work;
  other nodes pick up new work within their `pollInterval`.

## Migrations

`OracleMigrationRunner` applies `V<n>__<description>.sql` files listed in
`SHIPPED_MIGRATIONS` (no classpath scanning). Statements end with a line holding
only `/`, so the files and the emitted SQL run unchanged in SQL*Plus or SQLcl.
Oracle commits each DDL statement, so the runner records progress after every
statement, resumes a crashed migration at the first unrecorded statement, and
serializes migrators with a row lock on `threadmill_schema_lock` held on a
second connection. Details: [Oracle schema](../docs/oracle-schema.md).

## Tests

```bash
./gradlew :threadmill-store-oracle:test                                            # Oracle 23ai Free container
./gradlew :threadmill-store-oracle:test -PoracleImage=gvenzl/oracle-xe:21-slim-faststart   # 21c XE (amd64; CI)
./gradlew :threadmill-store-oracle:test -PoracleJdbcUrl=jdbc:oracle:thin:@//host:1521/SVC -PoracleUser=tm_test -PoraclePassword=...
```

There is no freely downloadable 19c container image. CI runs 23ai Free in the
main build and 21c XE (the closest free release to 19c, without any 23ai-only
syntax) in a separate job; every session runs with
`OPTIMIZER_FEATURES_ENABLE='19.1.0'`. Before a release, run the suite against a
real 19c database with the `oracleJdbcUrl` properties above — it deletes all
Threadmill rows in that schema, so use a disposable one. The plan tests need
`SELECT ANY DICTIONARY` (or `V$SESSION`/`V$SQL`/`V$SQL_PLAN` access) and skip
otherwise.

| Test | Covers |
|---|---|
| `OracleJobStoreContractTest` | The shared contract suite, through connections that start with `autoCommit=false`. |
| `OracleJobStoreRegressionTest` | Lock-as-you-fetch claiming, concurrent claims, keyed rotation, quarantine, CLOB supplementary characters, time-zone independence, empty strings, locale-independent paging, counters. |
| `OracleQueryPlanTest` | Executed plans of the hot and periodic statements. |
| `OracleMigrationRunnerTest` | Resume after a crash, DBA-applied SQL, concurrent migrators, edited migrations, constraints. |
| `OracleServerTest`, `OracleDeadlockRetryTest`, `OracleStatementSplitTest` | Startup gate, retry classification, statement splitting. |
