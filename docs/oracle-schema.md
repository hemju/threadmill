# Oracle Schema

Threadmill's Oracle store targets Oracle Database 19c and later. The store
constructor checks the release, the character set that holds application text
(see [Character sets and text types](#character-sets-and-text-types)), and the
session's `NLS_COMP=BINARY` (the default), and refuses anything else before any
job query runs. Module-level design notes — type mapping, virtual-column
indexes, the claim path — are in the
[`threadmill-store-oracle` README](../threadmill-store-oracle/README.md).

## Privileges

The schema user needs only:

- `CREATE TABLE` and `CREATE TRIGGER` (the `RESOURCE` role, or the individual
  privileges), and
- quota on its default tablespace.

No `CREATE TYPE`, `DBMS_*` execute grants, Advanced Queuing, or `SELECT ANY
DICTIONARY` is required at runtime. A user without DDL rights can still run
Threadmill after a DBA applies the schema (see [Manual DDL](#manual-ddl)) and
the application uses schema mode `validate` or `none`.

## Character Sets and Text Types

Threadmill stores text that applications supply (queue and recurring-task
names, handler and payload type names, concurrency and deduplication keys,
mutex names and holders, pause reasons, job bodies, recurring payloads) in
columns of one of two text types:

| Text type | Columns | Required character set | Use for |
|---|---|---|---|
| `standard` (default) | `VARCHAR2`, `CLOB` | Database character set `AL32UTF8` | Every Unicode database |
| `national` | `NVARCHAR2`, `NCLOB` | National character set `AL16UTF16` | Databases whose character set is not Unicode, for example `WE8MSWIN1252` |

A `VARCHAR2` or `CLOB` column holds text in the database character set. In a
`WE8MSWIN1252` database, every character outside that set (CJK, emoji, …) would
be stored as `¿`, so the startup check refuses the standard type there and
names the setting to change. The national type keeps every character,
including 4-byte ones, whatever the database character set is: its columns use
the national character set, `AL16UTF16` on practically every Oracle database.
The other national character set Oracle allows, `UTF8` (CESU-8), is refused.

Columns for Threadmill's own fixed ASCII values (job states, concurrency modes,
trigger kinds, missed-run policies, time-zone ids, the lease name) are
`VARCHAR2` under both types.

```yaml
threadmill:
  store:
    oracle:
      text-type: national
```

Without Spring Boot, pass the same type to the runner and the store:

```java
new OracleMigrationRunner(dataSource, OracleTextType.NATIONAL).migrate();
JobStore store = new OracleJobStore(dataSource, OracleTextType.NATIONAL);
```

Choose the type before the schema is created; it cannot be switched on an
existing schema. The store binds each value with the form of use of its column:
a database-character-set bind would lose characters before reaching an
`NVARCHAR2` column, and a national bind compared with a `VARCHAR2` column
converts the column, so its index is no longer used. The migration history
records the type through its checksums, so `migrate` and `validate` refuse a
schema created for the other type with a message naming both; `none` does not
check.

National column lengths count UTF-16 code units, the unit of Java's
`String.length()`. The 128-character name limit and the 256-byte key limits fit
unchanged. Mutex holders are limited to 2,000 code units instead of 4,000
bytes, because `NVARCHAR2` holds at most 4,000 bytes under
`MAX_STRING_SIZE=STANDARD`.

## Spring Boot Schema Modes

When Spring Boot auto-configures `OracleJobStore` from an application
`DataSource`, it handles the Threadmill schema before constructing the store.
Applications that define their own `JobStore` bean own schema handling
themselves.

| Property | Default | What |
|---|---|---|
| `threadmill.store.oracle.schema-mode` | `migrate` | `migrate`, `validate`, `none`, or `drop-and-migrate`. |
| `threadmill.store.oracle.allow-destructive-schema-reset` | `false` | Required for `drop-and-migrate`. |
| `threadmill.store.oracle.text-type` | `standard` | `standard` or `national`; see [Character sets and text types](#character-sets-and-text-types). |

`migrate` applies pending migrations (and resumes an interrupted one).
`validate` fails startup unless `threadmill_schema_history` records exactly the
shipped migrations, each complete and with a matching checksum for the
configured text type. `none` performs
no DDL or validation. `drop-and-migrate` drops Threadmill-owned tables and
recreates the schema; it destroys stored jobs and is intended only for
disposable dev/test schemas.

When both `threadmill-store-postgres` and `threadmill-store-oracle` are on the
classpath, Spring picks the store from `threadmill.store.jdbc-type`
(`postgres` or `oracle`), else from the `spring.datasource.url` scheme, else
PostgreSQL. See [configuration](configuration.md#oracle-store-properties).

## Migrations Are Not Atomic on Oracle

Oracle commits every DDL statement implicitly, so a migration cannot be
applied in one transaction. `OracleMigrationRunner` compensates:

- **Progress per statement.** `threadmill_schema_history` records, for each
  migration, how many of its statements completed and whether the migration
  finished. A DML statement commits together with its progress record.
- **Resume.** After a crash, the next `migrate()` continues at the first
  unrecorded statement. That statement's DDL may already have committed before
  the crash prevented its progress record, so for that one statement an
  "already exists" error (`ORA-00955`, `ORA-01430`, `ORA-02260`, …) counts as
  done. Any other failure stops the run with the statement number; fix the
  cause and rerun.
- **One migrator at a time.** Migrators serialize on a row lock in
  `threadmill_schema_lock`, held on a second connection because each DDL
  statement would commit, and so release, a lock taken on the migrating
  connection. A waiting node logs once and gives up after five minutes with an
  actionable message.
- **Validation first.** Every applied migration's description and SHA-256
  checksum is checked before anything is applied; an edited shipped migration,
  or a history row from a newer Threadmill, fails startup.

The whole v1 schema is one `V1__baseline.sql`. After release, schema changes
ship as additive `V2__*.sql`, `V3__*.sql`, … files; the baseline is never
edited again. The shipped list is explicit (no classpath scanning) for
native-image friendliness.

## Manual DDL

The checked-in migration,
[`V1__baseline.sql`](../threadmill-store-oracle/src/main/resources/com/hemju/threadmill/store/oracle/migrations/V1__baseline.sql),
is a template for both text types: each application-text column names its
standard type in double braces (`queue {{VARCHAR2(128 CHAR)}}`). Do not run it
directly; emit the SQL for your text type from the runner. Every emitted
statement ends with a line holding only `/`, the SQL*Plus convention, so the
output runs unchanged in SQL*Plus or SQLcl:

```java
var runner = new OracleMigrationRunner(dataSource, OracleTextType.STANDARD); // or NATIONAL
String install = runner.emitCleanInstallSql();
String pending = runner.emitPendingSql();
```

`emitCleanInstallSql()` returns the history-table DDL, every migration, and one
completed history row per migration. `emitPendingSql()` is read-only: it reads
`threadmill_schema_history` (prepending its DDL if the table does not exist yet)
and returns only what is missing, including the remainder of an interrupted
migration. Run the output with SQL*Plus or SQLcl, stop at the first error, and
then start the application with:

```yaml
threadmill:
  store:
    oracle:
      schema-mode: validate
```

## Reinitialization

For local or ephemeral schemas, Spring can recreate the schema:

```yaml
threadmill:
  store:
    oracle:
      schema-mode: drop-and-migrate
      allow-destructive-schema-reset: true
```

This drops only Threadmill-owned tables (`CASCADE CONSTRAINTS PURGE`, with
their indexes and trigger); the small `threadmill_schema_lock` table is kept
because the reset holds its lock. It deletes all Threadmill jobs, recurring
definitions, dedup records, queue pauses, leases, and counters. Use forward
migrations in production.

## Operational Notes

- **Statistics.** The claim path's hot statements carry index hints, so plans do
  not depend on fresh optimizer statistics, but keep the regular automatic
  statistics job enabled for the rest.
- **Temporary LOBs.** Job bodies longer than 8,000 characters are written
  through temporary LOBs (`CLOB`, or `NCLOB` for the national text type) that the store frees after each statement; size the
  `TEMP` tablespace for the concurrent write rate of large jobs.
- **No cross-node wake.** Nodes pick up work enqueued on another node within
  their `pollInterval` (default 500 ms); same-JVM wakes are immediate.
- **Driver.** The store uses standard JDBC. It was tested with `ojdbc11`
  23.26.3; older drivers that report no batch update counts are not supported.
