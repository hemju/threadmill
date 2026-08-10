# Changelog

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
