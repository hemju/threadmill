# Changelog

## Unreleased

- Fixed Postgres self-owned writes being silently rolled back when the host
  `DataSource` hands out connections with `autoCommit=false` (issue #111).
  Queue pauses, execution and node heartbeats, maintenance leases, retention,
  mutexes, recurring definitions/state, and the expired-dedup fallback now use
  explicit Threadmill-owned transactions, restoring the connection's prior
  auto-commit mode after commit or rollback.
- The recurring materializer now repairs a timing-fingerprint mismatch found
  during its under-mutex definition reload (issue #112). The current task
  definition wins: Threadmill preserves run bookkeeping and pending nudge
  demand, recomputes timing forward from the tick, and does not fire or
  `CATCH_UP` the obsolete trigger's overdue backlog.
- Added fixed process-separated nudge simulations for Postgres and Redis
  (issue #114). They hard-kill a maintenance leader after an accepted nudge
  and prove the standby serves it, then hard-kill a producer after its durable
  work write but before its nudge and prove the regular recurring backstop
  drains the row. Cross-process JSON-lines traces record process ids,
  leadership, trigger origins, and the verified event ordering.
- Recurring tasks can declare claim-time exclusivity (issue #110). A
  `CronTask` gains an `exclusive` flag, surfaced as
  `@Recurring(exclusive = true)` and as an `exclusive` parameter on
  `Scheduler.defineRecurring`. Every materialized instance — scheduled,
  caught-up, or manually triggered from the dashboard — is claimed under the
  derived key `recurring:<name>` in `ConcurrencyMode.EXCLUSIVE`, so the store
  refuses to admit a second instance while one is processing. This replaces
  the per-handler advisory lock a singleton sweep usually grows. It does not
  close the lease-expiry reclaim window: reclaim releases the concurrency slot
  as part of the terminal failure save, so handlers stay idempotent by
  contract. Postgres adds the additive migration
  `V6__cron_task_exclusive.sql`; Redis stores the flag as a cron-task hash
  field with the existing overwrite-on-re-upsert semantics.
- The recurring pile-up guard no longer opens a window between a failed
  instance and its retry (issue #110). `FAILED` is not terminal — a retry may
  follow — but the guard treated every `FAILED` as finished, so a materializer
  tick landing between the failure save and `RetryInterceptor`'s reschedule
  save could create a fresh instance beside a retrying one. A `FAILED`
  instance now blocks while its retry budget is not provably spent *and* the
  failure is younger than a five-second handoff grace. A retry-exhausted
  instance still never blocks, and the age bound keeps an instance that is
  terminal under a per-exception-type policy — which the guard cannot read —
  from blocking its task until the stranded-failure recovery scan reaches it.
- `DashboardPayloads.CronTaskView` gains an `exclusive` component.
- Documented the three overlap windows and the reclaim window's
  unclosability, with fencing guidance for effects that must not happen twice
  (issue #110).

## 0.2.0

- **On-demand materialization for recurring tasks ("nudge", issue #108).**
  `Scheduler.nudgeRecurring(taskName)` (Spring: `JobScheduler.nudgeRecurring`)
  requests that a registered recurring task materialize an instance as soon
  as possible, turning frequent pollers into wake-driven pumps with a slow
  backstop schedule. The nudge goes through the normal machinery — same
  pile-up guard, same missed-run policy — with four guarantees: at least one
  run starts after every accepted nudge (a nudge during an in-flight run
  produces exactly one follow-up after it completes, because the in-flight
  run may have read its inputs before the nudge's triggering write
  committed); a burst of nudges coalesces to at most the current run plus one
  follow-up; the nudge is a durable store write consumed by the maintenance
  tick, so there is no transient signal to lose (worst-case latency one
  `maintenancePollInterval`, default 1 s); and a nudged run never moves the
  schedule — a cron task's next fire stays the regular wall-clock match and
  an interval trigger's phase is preserved. Nudging an unknown task throws;
  nudging a disabled task throws — an explicit pause wins — and an
  enabled-flip clears any pending nudge. The coalescing bound is
  failure-free: consistent with at-least-once, a crash between the follow-up's
  insert and the request's clear can produce an extra run, never lose one.
  Storage: additive Postgres
  migration `V5__cron_state_nudge.sql` adds
  `threadmill_cron_task_state.nudge_requested_at` plus a store-generated,
  never-reset `nudge_revision` — the revision, not the collision-prone
  wall-clock timestamp, is the compare-and-clear identity; Redis stores both
  as schedule-state hash fields. `CronTaskScheduleState` gained read-only
  `nudgeRequestedAt` / `nudgeRevision` components.
- **Spring nudges are after-commit in every enqueue mode**, including
  `join_transaction`. Validation fails fast at call time, the write lands on
  commit, and a rollback discards it — identical semantics in all three
  modes. This is the one write that deliberately does not join the caller's
  transaction: coalescing is one store cell per task, so a joined nudge would
  hold that row's write lock for the whole business transaction and serialize
  every concurrent producer of that task (silently — correct at low rate,
  collapsing under load), and all it would buy is closing a crash window the
  design explicitly does not need closed. An in-JVM per-task coalescer
  additionally bounds the store write rate under bursts, and never retains a
  caller beyond its own covering write (follow-up generations run on a
  dedicated virtual thread).
- **Spring: nudge by handler class**, `jobScheduler.nudgeRecurring(OutboxPump.class)`.
  A `@Recurring` task's durable identity defaults to the handler's
  fully-qualified class name, so the string overload forced callers to
  hard-code it and broke on a rename or package move; the class overload
  resolves the registered name through the handler registry and matches the
  rest of this API, where the handler class is always the first argument.
  Nudging a registered handler that is not `@Recurring` fails loudly. The
  string overload remains for tasks registered imperatively through the core
  `Scheduler`, where the caller chooses the name.
- **Breaking (SPI):** `JobStore` gained two abstract operations,
  `requestCronNudge(name, requestedAt)` → `ACCEPTED | UNKNOWN_TASK |
  DISABLED` and `clearCronNudge(name, observedRevision)`. Third-party store
  implementations must add both before recompiling (old binaries throw
  `AbstractMethodError` when nudged). Implementation contract: acceptance
  must atomically check task existence + enabled and advance a monotonic,
  never-reset revision; the clear must compare-and-clear on that revision
  and must not touch it; `upsertCronTaskState` must preserve both nudge
  fields; a nudge must never resurrect state for a deleted task. The
  contract test suite (`AbstractJobStoreContractTest`) pins all of this.
- Recurring instances now carry `threadmill.cron.origin` metadata
  (`schedule` / `nudge` / `manual`), surfaced in three places: handler code
  (`JobExecutionContext.cronOrigin()`), the dashboard API (`JobSummary.cronOrigin`,
  deliberately visible on redacted read-level views — the value set is
  closed, so no metadata can leak through it), and Micrometer
  (`threadmill.jobs.recurring.runs{origin=schedule|nudge|manual|other}`,
  cardinality-clamped). The operations console renders it as a badge beside
  the handler, so a nudged run is distinguishable from a scheduled one at a
  glance.

## 0.1.4

- **Breaking (Spring):** renamed `@Job(maxRetries)` to `@Job(maxAttempts)`
  (issue #104). The value always counted total attempts — `maxRetries = 1`
  meant one attempt and zero retries — so every existing number keeps its
  runtime meaning under the honest name; only the attribute name changes.
  Non-positive values other than the `-1` sentinel now fail startup with a
  descriptive error instead of being silently replaced by the engine default:
  the natural "disable retries" spelling `0` used to run up to five attempts.
  The rename also reaches `ThreadmillJobRegistry.Registration`: its component
  changes from `int maxRetries` to a nullable `Integer maxAttempts`
  (constructor, accessor, and binary signature change; `null` means "no
  explicit budget"). Code that constructs registrations directly or reads
  them via `ThreadmillRecurringRegistrar.recurring()` must follow the new
  shape.
- Per-job retry metadata is now stamped only for an explicit
  `@Job(maxAttempts)` (issue #104). Previously the resolved default was
  stamped on every Spring-enqueued job, so per-job metadata permanently
  shadowed per-exception-type `RetryInterceptor` policies. Jobs without an
  explicit budget now fall through to the policy chain, and `@Recurring`
  handlers without an explicit budget register their `CronTask` with a null
  retry budget, so later `threadmill.default-max-attempts` changes are
  honored by already-registered tasks.
- Fixed startup re-registration wiping an overdue recurring `next_run_at`
  (issue #105). `Scheduler.upsertCron` now preserves the schedule state —
  including an overdue next-run and an interval trigger's phase — while the
  re-registered schedule is unchanged, so the task's `MissedRunPolicy`
  decides what happens to restart-missed firings: `CATCH_UP` materialises
  every missed fire (capped per tick) and `DROP` collapses the backlog into
  one make-up run shortly after startup. A real edit — changed trigger,
  changed zone on a cron trigger, or re-enabling a disabled task — still
  recomputes the next-run from now so an edited cron never fires stale
  times. `CronExpression` gained source-based value equality to support the
  unchanged-schedule comparison.
- `CronTaskScheduleState` now carries a `timingFingerprint` recording which
  trigger timing its `next_run_at` was computed from, written atomically with
  the state row (additive Postgres migration
  `V4__cron_state_timing_fingerprint.sql`; a Redis hash field; legacy rows
  read as null and simply recompute once). The unchanged-schedule decision
  reads this fingerprint rather than comparing stored task definitions, so a
  crash between the separate task-definition and state writes can never pair
  a new trigger with old timing undetectably — the retry detects the
  mismatch and recomputes. The fingerprint deliberately excludes the zone
  for interval triggers (`CronTask` documents the zone as ignored there), so
  nodes with different system-default zones do not treat the same interval
  as a schedule edit.
- `DROP` missed-run recovery is now phase-exact and nominally stamped: the
  single make-up instance represents the most recent nominal fire (its
  `cronFireTime()` carries that nominal time, not the recovery wall-clock),
  and the next run advances from the nominal fire — an every-6h task that
  was due at 06:00 and recovered at 07:00 next fires at 12:00, not 13:00.
  Behavioral note: with overdue state now surviving restarts, an existing
  `DROP` task whose boundary passed during downtime fires once shortly after
  startup instead of silently skipping to the next boundary — the same
  behavior it always had across a live materializer stall.
- Patched OSV-flagged dependencies for the release: PostgreSQL JDBC 42.7.12,
  Netty 4.1.136.Final, Micrometer 1.15.12, a test-classpath constraint on
  `tools.jackson.core:jackson-databind` 3.1.5 (Spring Boot 4.0.7's BOM still
  pins 3.1.4), and refreshed dashboard build dependencies (`nanoid`,
  `postcss`). `npm audit` and the OSV scan report zero known vulnerabilities
  for the release lockfiles.

## 0.1.3

- Fixed Spring lifecycle ordering so `ProcessingNode` starts before remote-wake
  subscription and stops after the subscription closes. Spring applications
  now fail fast when no durable store is configured; the in-memory store must
  be explicitly enabled with `threadmill.store.memory.enabled=true`.
- Required the per-task correctness mutex for recurring definition
  registration, reconciliation, dashboard mutation, and deletion. Contended
  mutations now fail explicitly instead of proceeding without their lock.
- Retained terminal-save responsibility through store outages. A worker keeps
  retrying `PROCESSING -> SUCCEEDED` / `FAILED` with capped backoff until the
  write commits or node shutdown begins, preventing completed attempts from
  being stranded behind refreshed owner heartbeats.
- Hardened persisted invariants with defensive `JobSnapshot` copies, bounded
  handler type names, safe `ProcessingNode` lifecycle behavior, startup
  migration checksum/description validation, and additive PostgreSQL V3
  integrity constraints.
- Bounded Redis claim candidate discovery with rotating `HSCAN` cursors and
  pipelined per-key probes, avoiding backlog-wide registry reads under large
  keyed workloads.
- Added Gradle wrapper verification, dependency locking and SHA-256 metadata,
  immutable GitHub Action pins, and mandatory `META-INF/LICENSE` / `NOTICE`
  inspection for every published JAR. Refreshed dashboard build tooling to
  patched Babel, esbuild, and Vite releases; `npm audit` reports zero known
  vulnerabilities for the release lockfile.
- Upgraded Jackson, Spring Boot, Spring Framework, Spring Security, Netty,
  PostgreSQL JDBC, Logback, and AssertJ to patched maintenance releases. The
  release OSV gate now scans every committed Gradle and npm lockfile instead of
  passing an unsupported version-catalog file to the scanner.
- Synchronized installation versions, release/security guidance, API names,
  dashboard security documentation, Redis execution-model documentation, and
  soak-result wording.

## 0.1.2

- Fixed `@Job(timeout)` and `@Job(maxRetries)` being silently ignored for
  `@Recurring` handlers (issue #84). A `CronTask` now carries an optional
  per-instance `timeout` and `maxAttempts`; `RecurringMaterializer` and the
  dashboard's manual trigger stamp them onto every materialized instance as
  `JobRunner.META_TIMEOUT_SECONDS` / `RetryInterceptor.META_MAX_ATTEMPTS`, so
  the annotations behave identically on the recurring and enqueue paths.
  Previously every recurring instance ran under the global `jobTimeout` and the
  default retry budget, so a long-running recurring job could be interrupted
  mid-run and retried (duplicating external side effects).
- Added a per-instance `timeout` / `maxAttempts` parameter to
  `Scheduler.defineCronTask`, `defineIntervalTask`, and `defineRecurring` for
  core (non-Spring) callers. Existing signatures are unchanged (`null` keeps
  the engine defaults).
- Added the additive Postgres migration `V2__cron_task_overrides.sql`
  (nullable `timeout_seconds` and `max_attempts` on `threadmill_cron_tasks`);
  existing rows default to the engine behaviour. Redis stores the same as
  optional hash fields cleared by an override-less re-registration.

## 0.1.1

- Fixed `Dispatcher` release of claimed-but-unrun jobs (node-tag mismatch,
  dispatch failure, shutdown mid-batch): releases now route through the single
  failure path as `FailureCause.SHUTDOWN`, so the job is rescheduled
  immediately without consuming a retry attempt and its claim-time concurrency
  slot is freed. Previously the release attempted an illegal
  PROCESSING→SCHEDULED transition, always threw, and left the job to orphan
  reclaim.
- Fixed the default job id and `createdAt` to derive from a single clock read
  in `Job.Builder.build()`, keeping UUIDv7 id ordering consistent with the
  engine's in-key claim-admission order.
- Validated with a 12-hour PostgreSQL endurance soak (mixed workload,
  ~5.5 million jobs, 3 nodes, node churn every 10 minutes): passed with all
  invariants green.

## 0.1.0

First public release under the Apache-2.0 license.


- Added claim-time per-key concurrency with `ConcurrencyMode`, workflow-root
  inheritance, store-backed enforcement in memory/Postgres/Redis, and
  documentation for import/export and tenant event-processing shapes.
- Added queue-family lanes with anchored `*` / `?` patterns, stride-scheduled
  `QueueWeights`, discovery retention, Spring configuration, and soak coverage
  across all stores.
- Fixed workflow-root concurrency release for failed intermediate workflow
  steps by abandoning descendants that can no longer be promoted.
- Added the Spring ergonomic API: `@Job`, typed handler discovery, and
  `JobScheduler`.
- Added Redis standalone, Sentinel, and Cluster configuration. Cluster uses a
  single `{threadmill}` hash slot for v1 Lua correctness.
- Added producer-side deduplication with `Created` / `Coalesced` results.
- Added long-running job check-ins, progress updates, bounded logs, and
  no-progress timeout handling.
- Removed the experimental alternate framework module and public positioning
  for now. The core remains framework-agnostic so additional integrations can
  be added later.
