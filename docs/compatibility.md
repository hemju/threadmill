# Compatibility contract for the 1.0 candidate

Threadmill provides **at-least-once delivery**. A recovered or retried job can
execute again; handlers and external side effects must be idempotent. The changes
in issue [#135](https://github.com/hemju/threadmill/issues/135) harden that contract.
They do not make execution exactly once. This document defines the proposed 1.0
compatibility boundary; a release still requires the recorded validation and
endurance gates in the [release checklist](release-checklist.md).

## Supported platform and storage

Java 25 is required. PostgreSQL requires 18 or later. Redis data nodes require
7.4 or later and `noeviction`; validate every node that may become a primary.
The exact tested dependency versions are in Gradle/npm locks and the release
validation artifacts. A supported major/minimum is not evidence that every
future server version has been qualified.

A Redis namespace occupies one `{threadmill}` Cluster slot. Cluster provides
topology and failover integration for that namespace, not horizontal distribution
of its job load. Atomic Lua operations do not guarantee acknowledged-write
survival across primary failure; configure persistence/replication for the
application's recovery-point requirements. See [Redis topology guidance](redis-topologies.md).

## Public API and SPI

Application APIs are the command/handler model, scheduler, documented engine
configuration, execution context, interception, and optional adapters. Public
configuration records and snapshot records are source/binary API: adding a record
component is a breaking change even when JSON can default it. Recompile existing
0.x applications and custom stores against this candidate. Do not mix Threadmill
module versions in one process.

Before 1.0, custom stores must implement the complete `JobStore` contract,
including monotonic execution revisions, bounded maintenance scans, retention
pages, concurrency metadata reclamation, capabilities, and atomic bulk budgets.
Implement `touchExecutionHeartbeats(nodeId, activeClaims, now)` with exact
job-ID/state-version/owner checks and the 500-claim bound. The engine renews only
confirmed active execution/finalization contexts; mapping this operation to the
old owner-wide heartbeat can strand a claim whose acknowledgement was lost.
Use `ForwardingJobStore` for decorators and run the shared store contract plus
the reflection-based decorator coverage. A capability describes actual behavior;
unsupported operations must fail explicitly. In particular, Redis job search
requires state and rejects queue/handler filters. SQL and memory support those
filters before pagination. Pagination under concurrent writes is not a snapshot;
Redis equal-time ties use descending canonical ids at millisecond precision.

The persistence rules are stable requirements: versions advance only after a
confirmed write; execution updates compare state version and attempt-local
revision; atomic batches reject wholly above 1,000 jobs or 8 MiB of encoded bodies;
initial JSON jobs reserve lifecycle space. `insert` starts a version at 1 and
rejects an input whose persisted version would have to move backwards. Importing
historical records is an offline migration, not a new-job insert.

Maintenance pages use exclusive cursors. `scanJobs` and `scanCronTasks` return at
most 500 records; `deleteFinishedPage` inspects at most 100 and returns actual
deletions plus an opaque `RetentionCursor`. Keep its cutoff/state fixed across
pages; recent records do not consume the candidate budget. Zero deletions does
not mean a pass is complete. The optional `deleteIdleQueueMetadata` operation
defaults to no work; forwarding decorators must pass it through.
Retention preserves live dedup keys, predecessors with waiting children, and
failures with pending or unknown retry decisions. The older
`deleteFinishedOlderThan` convenience method inspects the first page only.

Failure policy is resolved before the FAILED write and persisted as
`FailureDecision`. The final cleanup hook, `onProcessingFinished`, runs on every
execution exit, even when an outcome notification cannot be delivered. Cleanup
must not infer a durable success from a locally mutated job. Metrics and tracing
identify an execution by its context instance; an orphan finalizer must not close
another execution's thread-bound scope.

Dashboard DTOs are a separate HTTP contract. State-history diagnostics use
`reason` and `message`; the server redacts sensitive content according to
permissions. Clients must tolerate additive response fields and unknown optional
values. The UI displays public client-error ProblemDetail messages as text and
never interprets them as HTML. Operator writes retain authorization, CSRF and
optimistic-version checks.

## Upgrade from v0.3.0

1. Back up the datastore and record the application/Threadmill versions and
   configuration. Review legacy FAILED jobs: their exception-specific retry
   decisions were not persisted. Explicitly retry or delete those jobs according
   to application policy; the candidate preserves unknown outcomes and waiting
   children rather than guessing.
2. Stop every old worker **and producer**, including Spring instances that can
   enqueue. Gracefully finish work where possible; interrupted processing jobs
   retain ownership/heartbeat evidence for normal orphan recovery. Allow old
   registrations/leases to expire before running the Redis offline migrator.
3. For PostgreSQL, apply the current `MigrationRunner` or its emitted SQL to the
   existing database. V1–V6 remain byte-for-byte unchanged. V7 adds the execution
   revision, V8 adds maintenance scanning, V9 adds queue counters/monitoring
   indexes, V10 indexes idle concurrency metadata, and V11 adds the time/ID
   retention index while dropping the redundant two-column state/time index.
   The runner validates every
   recorded description/checksum and refuses unknown future migration versions.
   V7 validates its non-negative revision constraint with a full table scan
   under `ACCESS EXCLUSIVE`; V9 backfills counters under a table lock. These
   migrations run with all application instances stopped. Allow a maintenance
   window sized for the retained population and verify the resulting counts.
   Splitting constraint creation and validation inside the runner's same
   transaction would not release V7's table lock sooner.
4. For Redis, run `RedisIndexMigration.migrate(...)` with a caller-owned standalone,
   Sentinel or Cluster client. It acquires a migration lease, marks the namespace
   incomplete, converts legacy pending-member order and rebuilds auxiliary
   indexes, then marks format 2 complete. It preserves job bodies, versions,
   ownership, workflow holds, dedup, pauses and recurring/nudge state. A failed run
   is resumable; rerun it after resolving the failure. New stores reject a
   nonempty legacy/incomplete namespace. Producers have no registry, so the
   migrator cannot independently prove they are stopped.
5. Start only candidate workers/producers. Verify queue/state counts, paused
   queues, recurring ownership/nudges, orphan recovery, retry disposition and
   workflow continuation before restoring full traffic.

**Rolling mixed-version operation from 0.3.0 is unsupported.** Old workers can
rewrite away retry decisions/revisions and use obsolete Redis indexes. PostgreSQL
migration validation and Redis format checks protect new startup paths; they do
not remotely fence already-running old binaries. Coordinate the deployment.

**Downgrade requires a restore.** Stop the candidate and restore the pre-upgrade
backup with the matching old application and Threadmill binaries. Running the old
binary against a migrated store, removing history rows, or changing a Redis
format marker is not a supported rollback. Work accepted after the backup must
be reconciled through application idempotency/outbox records.

Frozen tests retain original v0.3.0 serializer bytes for eight states, Unicode,
processing liveness, workflow relationships and version 7. PostgreSQL tests install
the exact released V1–V6 SQL, populate it, then migrate and verify bytes/counters
and operational state. Redis tests exercise nonempty legacy-index conversion
through both standalone and Cluster clients, including an acquired exclusive
workflow hold. These tests cover the shipped upgrade path; they are not a promise
of arbitrary payload-shape migration.

## Changes after 1.0

Compatible minor releases may add optional fields/operations with explicit
backward-compatible defaults, but must retain documented behavior and golden
fixtures. Breaking public signatures, record components, wire interpretations or
storage semantics require a major-version migration plan. Future releases must
state whether mixed-version operation is supported and prove it before calling
an upgrade rolling. Keep historical SQL immutable and ship additive migrations;
Redis representation changes require an explicit format gate and migration.

Handler and payload class names are durable type tags. Drain affected work or
perform an application-owned offline data migration before renaming a type or
changing its payload shape. There is no runtime alias or unrestricted polymorphic
payload migration mechanism.
