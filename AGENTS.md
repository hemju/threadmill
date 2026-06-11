# AGENTS.md — Threadmill

This is the standing brief for any AI agent (or human) working on Threadmill. Read it fully before touching the repository.

**Project status:** v1 feature-complete, ready for first release. Three storage backends (in-memory, PostgreSQL 18+, Redis with standalone/Sentinel/Cluster), the processing engine, scheduling and recurring APIs, the per-queue pause primitive, the bulk-enqueue path, claim-time per-key concurrency with workflow inheritance, queue-family lanes, Spring Boot integration with explicit enqueue transaction modes (`after_commit`, `join_transaction`, `immediate`), Micrometer metrics, optional OpenTelemetry tracing, the data-first dashboard model, Spring dashboard API, static React dashboard UI, docs, examples, and a soak/load module are all in place. Store-backed maintenance leadership, crash-safe Redis claim, producer-side deduplication, long-running job check-ins, idle-worker wake signal, cross-node remote wake hints, and failure-detail truncation are part of the production-readiness baseline.

---

## 1. What Threadmill is

Threadmill is a modern, lightweight **background job-processing library for Java**. The design uses well-understood techniques (a durable job store, optimistic concurrency control, polling worker loops, an explicit state machine) and is intended for two use shapes:

1. A drop-in scheduler for an existing internal codebase.
2. A standalone open-source library.

The delivery guarantee is **at-least-once**: a job may run more than once (for example after a node crash mid-execution). Handlers must be idempotent. State this loudly in any user-facing docs.

---

## 2. Platform and technology

- **Java 25 (LTS) only.** Build with `--release 25`. No support for older JDKs, no multi-release JARs, no compatibility shims.
  - Virtual threads are the default worker model.
  - Scoped values (final in Java 25), not `ThreadLocal`, propagate per-execution context (job id, attempt number, log/MDC) into handler code and any virtual threads it spawns.
  - No preview features in the public API.
- **Build tool:** Gradle (≥ 9.5), multi-module. The wrapper is committed.
- **Frameworks:** usable from Spring Boot. The core is framework-agnostic so additional adapters can return later as real integrations. Native image remains a future target — minimise reflection.
- **Storage backends:** PostgreSQL and Redis are both first-class, plus an in-memory store for tests and local development. All three satisfy one storage SPI and pass one shared contract test suite.
  - **PostgreSQL 18+ only.** Build, tests, and example all run against `postgres:18-alpine`. No back-compat shims for earlier majors.

---

## 3. Module map

Modules are physically separate. No storage implementation and no UI may ever be bundled into `threadmill-core`.

| Module | Base package | Contents |
|---|---|---|
| `threadmill-core` | `com.hemju.threadmill.core` | Job model, state machine, `JobStore` SPI, serialization, processing engine, interceptors, scheduling API. **No storage implementation, no UI, no framework code.** |
| `threadmill-store-postgres` | `com.hemju.threadmill.store.postgres` | PostgreSQL job store, schema, in-process migration runner. |
| `threadmill-store-redis` | `com.hemju.threadmill.store.redis` | Redis job store, atomic Lua scripts. |
| `threadmill-store-memory` | `com.hemju.threadmill.store.memory` | In-memory job store for tests and dev — held to the same contract as the real backends, not a simplified fake. |
| `threadmill-spring-boot` | `com.hemju.threadmill.spring` | Spring Boot 4.x auto-configuration. |
| `threadmill-test-support` | `com.hemju.threadmill.test` | Abstract `JobStore` contract test plus shared fixtures. |
| `threadmill-metrics` | `com.hemju.threadmill.metrics` | Micrometer integration: per-state gauges, processed / failed counters, processing-time timer. |
| `threadmill-tracing` | `com.hemju.threadmill.tracing` | Optional OpenTelemetry API integration: processing spans and a tracing `JobStore` decorator. |
| `threadmill-dashboard-api` | `com.hemju.threadmill.dashboard.api` | Spring-free dashboard contract: `EngineSnapshot`, DTOs, service logic, permissions, and audit contracts. |
| `threadmill-dashboard-ui` | n/a | Static React/Tailwind/shadcn operations console packaged as reusable classpath assets. |
| `threadmill-dashboard-spring` | `com.hemju.threadmill.dashboard.spring` | Spring MVC/Security dashboard adapter: JSON endpoints, redaction, authorization, audit hooks, operator actions, and optional UI mounting. |
| `threadmill-soak` | `com.hemju.threadmill.soak` | Fixed soak regression suite plus tunable sustained load/performance harnesses. Tagged regression tests are excluded from `check`; harness artifacts live under `build/soak/`. |
| `threadmill-simulation` | `com.hemju.threadmill.simulation` | Correctness simulations: short fixed invariant verifier for all three stores, plus worker-churn simulations against shared Postgres / Redis. JSON-lines traces live under `build/simulation/`. |
| `threadmill-example` | `com.example.threadmill` | Compiled user-facing examples only: in-memory getting-started, manual Postgres / Redis workers, and submitter examples. |

---

## 4. Where things live

- `AGENTS.md` — this file. `CLAUDE.md` is a symlink to it; rewrite this file in place.
- `README.md` — public-facing project overview.
- `docs/` — public getting-started, Spring quickstart, configuration, Redis topology, deduplication, long-running jobs, operations, troubleshooting, migration, and release-checklist docs.
- `.agents/skills/` — repo-local workflows for release, regression, and operability checks.
- `gradle/libs.versions.toml` — version catalog (every external dependency and the formatter are pinned here).
- `buildSrc/` — Gradle convention plugins (`threadmill.java-base`, `threadmill.java-module`).
- `.git-blame-ignore-revs` — points at the mechanical Spotless reformat commit; configure once per checkout with `git config blame.ignoreRevsFile .git-blame-ignore-revs`.
- Private local-only working notes are gitignored and not committed.

---

## 5. Canonical vocabulary

Use these terms for these concepts. Refining a name during implementation is allowed only if the change lands consistently across the whole codebase and the change is recorded here.

| Concept | Canonical term |
|---|---|
| The product / library | **Threadmill** |
| Per-application-instance execution engine | `ProcessingNode` |
| Loop that claims and dispatches work | `Dispatcher` |
| Master-only housekeeping loop (promotion, recurring, orphan scan, retention) | `MaintenanceCycle` |
| Cluster membership: heartbeat, registry, master election | `NodeRegistry` |
| A queue with reserved capacity inside one node | `QueueLane` |
| The persistence SPI | `JobStore` |
| Cross-node wake hint SPI | `RemoteWakeChannel` |
| In-memory / Postgres / Redis stores | `InMemoryJobStore`, `PostgresJobStore`, `RedisJobStore` |
| Serialized description of the work to run | `JobSpec` |
| A typed argument inside the spec | `JobArgument` |
| A recurring (cron / interval) definition | `CronTask` |
| Bookkeeping next to a recurring definition | `CronTaskScheduleState` |
| The command + handler job model | `JobPayload` + `JobHandler` |
| SPI that resolves a `JobHandler` (host DI or reflection) | `JobHandlerResolver` |
| Built-in resolvers | `ReflectiveJobHandlerResolver`, `SpringJobHandlerResolver`, `CdiJobHandlerResolver` |
| Lifecycle interception SPI | `JobInterceptor` |
| Built-in interceptors | `RetryInterceptor`, `WorkflowInterceptor` |
| Per-execution context object | `JobExecutionContext` (concrete: `ExecutionContext`) |
| Per-job log / progress reporting | `JobLog`, `JobProgress` |
| Atomic in-place definition swap | `JobReplacement` (engine helper: `JobReplacements`) |
| User-facing API for enqueuing / scheduling / recurring | `Scheduler` |
| Per-job timeout / circuit-breaker support | `CircuitBreaker`, `JobRunner` |
| Claim-time per-key concurrency mode | `ConcurrencyMode` |
| Dynamic queue-pattern lane | `QueueFamily` |
| Queue-family weighting | `QueueWeights` |
| Workflow root scalar | `workflow_root_id` |
| Idle-worker → dispatcher signal | `WakeSignal` |
| Spring after-commit scheduler wrapper | `TransactionAwareJobScheduler` |
| Spring transaction-joined scheduler wrapper | `TransactionJoinedJobScheduler` |
| Database objects | `threadmill_jobs`, `threadmill_cron_tasks`, `threadmill_cron_task_state`, `threadmill_cron_task_ownership`, `threadmill_nodes`, `threadmill_metadata`, `threadmill_job_counts`, `threadmill_schema_history`, `threadmill_mutexes`, `threadmill_leases`, `threadmill_concurrency_groups`, `threadmill_concurrency_workflow_holds`, `threadmill_queue_pauses`, `threadmill_dedup_keys` |
| Configuration namespace | `threadmill.*`, `threadmill.queue-family.*` |
| Stale-version exception | `StaleJobException` |
| Oversize-job exception | `OversizedJobException` |
| Fatal engine exception | `JobEngineFatalException` |
| Illegal state-transition exception | `IllegalJobTransitionException` |

**Job states:** `AWAITING`, `SCHEDULED`, `ENQUEUED`, `PROCESSING`, `SUCCEEDED`, `FAILED`, `DELETED`, `QUARANTINED` (jobs that cannot be deserialized / instantiated), and `PROCESSED` (reserved for an external-trigger feature: handler returned, job awaiting an external signal).

---

## 6. Core design principles

These are acceptance criteria, not suggestions.

- **The optimistic-lock `version` belongs to persisted state.** Never increment it on the in-memory job object before the write is confirmed. A failed save must leave the in-memory job exactly reusable.
- **The job object handed to user code is not the object the engine serializes.** Snapshot user-touchable areas (metadata, log, progress) before serialization; concurrent mutation during serialization must be impossible by construction.
- **One state machine, centralised and tested.** Illegal transitions throw; never coerced silently.
- **Bound the job size.** Reject oversize jobs at creation; fail cleanly at save without corrupting the version.
- **One failure code path.** Orphan reclaim, per-job timeout, and a thrown exception all funnel through the same state transition and the same interceptor hooks.
- **Assume user code can hang.** Per-job timeouts are core, with an interrupt/cancellation path.
- **A circuit breaker over a long-lived loop must decay on success**, and must not be tripped by a single poison job (quarantine it instead).
- **A store outage pauses the cluster; it does not kill it.** The dispatcher's circuit breaker pauses processing and probes the store until it returns.
- **The `JobStore` SPI is expressed in operations and guarantees, not SQL**, so a relational and a key-value store both satisfy one contract test honestly. A capabilities descriptor documents real differences.
- **The job model carries relationships and a result slot from the outset**, so workflows / batches / external-trigger jobs need no model migration.
- **Claim-time concurrency is scheduling metadata, not work description.** `concurrencyKey`, `ConcurrencyMode`, and `workflow_root_id` live on `Job` / `JobSnapshot`, never `JobSpec`.
- **No framework types in `threadmill-core`.** Anything that needs Spring or Jakarta lives in an integration module.

---

## 7. Build, test, run

The build uses Gradle (≥ 9.5) and the project's Java 25 toolchain.

- `./gradlew build` — compile + test + assemble all modules.
- `./gradlew check` — tests + Spotless formatting check.
- `./gradlew test` — run the test suite.
- `./gradlew :threadmill-core:test` — test a single module.
- `./gradlew :threadmill-store-memory:test` — run the contract suite against the in-memory store.
- `./gradlew spotlessApply` — auto-format every Java / Kotlin / `*.gradle.kts` / Markdown / `.gitignore` file to the project style.
- `./gradlew spotlessCheck` — formatting check only (run by `check`).
- `./gradlew :threadmill-soak:soakRegression` — run the fixed soak regression suite (in-memory throughput, real PostgreSQL + Redis throughput, induced container-pause recovery). Not part of `check`.
- `./gradlew :threadmill-soak:soakMemory` / `soakPostgres` / `soakRedis` / `soakAll` — run the tunable load soak harness.
- `./gradlew :threadmill-simulation:simulate` — run the short correctness simulation against all three backends.
- `./gradlew :threadmill-simulation:simulateWorkerChurnPostgres` / `simulateWorkerChurnRedis` — run worker-process churn simulations against shared local datastores.
- `./gradlew :threadmill-example:run` — compile and run the public getting-started example.
- `./gradlew productionCheck` — release-candidate validation: clean, check, Javadoc, real store tests, soak, dependency scan hook, example, and artifact inspection.
- Integration tests for the real backends use **Testcontainers** and need a working container runtime (Docker / Podman / Colima / OrbStack).

The Gradle wrapper is committed; new clones run `./gradlew` without a system Gradle install.

---

## 8. Testing expectations

Testing is a first-class deliverable; treat the test suite as equal in weight to the code.

- The `JobStore` contract is an **abstract test suite written first**. Every store (in-memory, Postgres, Redis) extends `AbstractJobStoreContractTest` and is held to the **identical** 62-test suite — that is the only thing guaranteeing all three backends behave the same.
- Integration tests run against **real PostgreSQL and real Redis via Testcontainers**. Never mock the datastore — the hard bugs live in the datastore's real locking, atomicity, and encoding behaviour.
- **Every bug becomes a named, permanent regression test** added with (or before) the fix. See the regression-coverage matrix in §11.
- Concurrency tests (N simulated nodes claiming from one store), serialization round-trip tests (including 4-byte Unicode and oversized payloads), and the soak/load module are first-class.
- Apply coverage and/or mutation-testing gates to `threadmill-core` and the stores as the suite grows.

---

## 9. Conventions and definition of done

**Coding conventions**

- Modern, idiomatic Java 25. Prefer immutability; prefer composition. Keep the public API small and deliberately named — public names are expensive to change later.
- **Use `var` for local variables whenever the type is clear from the right-hand side** (constructor calls, factory methods, builders, obvious method returns). Do not use `var` when the inferred type would be less readable than spelling it out. `var` is only for locals — never for fields, parameters, or return types.
- **Never use fully-qualified names inside method bodies, type signatures, or annotations.** Always add an `import` (or `import static`) at the top of the file, even for one-off references and even when an import would conflict — disambiguate by renaming a local symbol rather than by spelling out a package. The only legitimate exception is when two types with the same simple name must coexist in one file; in that case the less common one stays fully qualified at its use site, and that decision is worth a one-line comment. Spotless enforces the import order; reviewers should treat `java.util.List<com.hemju...>` style as a defect.
- **`main` methods do not carry the `public` modifier.** Java 25 (JEP 512) recognises a main method with any access modifier; declare entry points as `static void main(String[] args)` (or `static void main()` when the args are unused). The `public` keyword on `main` is dropped everywhere in this codebase.
- No framework types in `threadmill-core`.
- Public types are documented with Javadoc that stands alone.
- Source style is enforced by Spotless (Palantir Java Format for Java; ktfmt `kotlinlang` style for Kotlin / `*.gradle.kts`). Import order is fixed: `java`, `javax`, `jakarta`, third-party, `com.hemju`. Trailing whitespace and EOL newlines are enforced on Java, Kotlin, gradle scripts, `*.md`, and `.gitignore`. The formatter version is pinned in `gradle/libs.versions.toml` so everyone produces byte-identical output.

**Commit conventions**

- Conventional Commits: `feat:`, `fix:`, `test:`, `docs:`, `refactor:`, `chore:`, `build:`, `style:`. A clear scope (`feat(core): …`, `fix(store-redis): …`) is helpful.

**Definition of done (every change)**

1. Code complete and reviewed against §6.
2. Tests complete: unit + contract + integration + regression as applicable, passing on the real datastores where relevant.
3. Documentation updated (Javadoc + any affected guide).
4. `AGENTS.md` updated where vocabulary, conventions, or status moved.
5. `./gradlew check` is green (tests + Spotless).

---

## 10. Working method

- You are trusted to do your own research. Validate assumptions against current Java 25, PostgreSQL, Redis, and Spring documentation; prototype where useful; choose the best design within §6. If you find a better approach than this file sketches, take it and record the rationale here.
- Build reusable **skills** for recurring project-specific tasks (adding a store, turning a bug into a named regression test, running the soak suite, formatting a contribution) so they are done consistently rather than re-derived.
- Work in small, coherent commits; the `check` task must stay green at each commit.

---

## 11. Design decisions and follow-on work

This section is the project's memory: the load-bearing decisions worth knowing before changing anything nearby, the regression-coverage matrix, and the open items.

### Core model and stores

- **Job ids are UUIDv7.** `JobId.newId()` builds a 48-bit Unix-millis prefix + version-7 nibble + RFC 4122 variant + random tail. Time-ordered ids matter for index locality in Postgres and iteration order in Redis.
- **`Job.version` is persisted state.** Update only via `Job.adoptVersion(long)`, which the store calls **after** a successful write. A failed save — `StaleJobException`, `OversizedJobException`, or any other throw — leaves the in-memory version unchanged.
- **State is an append-only history.** No single mutable `state` field on `Job`; `currentState()` is the last element of `stateHistory()`. Transitions route through `JobStateMachine.requireLegal`.
- **The engine serializes a `JobSnapshot`, never the live `Job`.** `Job.snapshot()` copies user-touchable areas under the job's monitor, so a serialization cannot observe a torn write by construction.
- **Stores keep the wire form, not the live object.** The in-memory store deliberately round-trips through the serializer on every operation so the serializer is exercised continuously; the real backends do the same.
- **`claimReady` is atomic across nodes.** In-memory: single mutex. Postgres: `SELECT … FOR UPDATE SKIP LOCKED` + version-matched UPDATE in one transaction. Redis: a single Lua script. The contract test runs concurrent virtual-thread workers against a pre-populated queue and asserts no double-claim.
- **Concurrency groups are enforced at claim time.** `SHARED` jobs for one key run together only while no earlier pending `EXCLUSIVE` job exists; `EXCLUSIVE` jobs run alone. Workflow successors inherit the root job's key/mode and hold the key until the last descendant terminates.
- **Orphan recovery routes through `FAILED`, never directly to `ENQUEUED`.** This funnels orphan, timeout, and exception failures through one code path with one set of interceptor hooks. `PROCESSING → ENQUEUED` is explicitly illegal in the state machine.
- **JSON is the default wire format.** `JsonJobSerializer` uses Jackson and accepts a host-supplied `ObjectMapper` so applications can reuse the mapper they already configure. The serializer is the only place that enforces the size cap. Handler / payload class renames are explicit migrations: use `TypeNameAliases` for compatible name moves, `PayloadMigrations` for breaking JSON shape changes, and `JobDefinitionMigrator` to rewrite already-persisted non-running jobs.

### Postgres-specific

- **PostgreSQL 18 or later, enforced at startup.** `PostgresJobStore`'s constructor runs `SHOW server_version_num` and throws `JobEngineFatalException` if it's below 180000. The regression test (`PostgresVersionGateTest`) boots a `postgres:17-alpine` container and asserts the refusal. There are no back-compat shims for older majors; the migration SQL and queries are written for PG18 syntax exclusively.
- **One consolidated baseline plus additive migrations.** All pre-release Postgres schema work is collapsed into `V1__baseline.sql`; later changes ship as additive migrations (`V2__*.sql`, `V3__*.sql`, etc.). The `MigrationRunner` and its explicit `SHIPPED_MIGRATIONS` list remain.
- **Body column is `text`, not `jsonb`.** The body is not queried; the indexed scalar columns are. Text avoids the jsonb parser tax on every write and keeps the on-disk form identical to the wire form.
- **Indexes are partial, matched to query shapes.** `WHERE state='ENQUEUED'` for the claim path, `WHERE state='SCHEDULED'` for promotion, `WHERE state='PROCESSING'` for orphan recovery. New states that need to be targeted by a query get their own partial index.
- **Concurrency groups are persisted bookkeeping, not inferred scans.** `threadmill_concurrency_groups` stores per-key in-flight shared/exclusive counts; `threadmill_concurrency_workflow_holds` stores workflow-root outstanding counts. Claim and release update these rows in the same transaction as the job state transition.
- **Postgres claim decisions are scalar-first.** `claimReady` pages locked candidate rows with scalar columns, locks distinct concurrency group rows once per page in sorted order, batch-loads counters / active holds / earliest pending order, and only fetches/deserializes bodies for jobs it will actually claim.
- **Concurrency pending checks use a partial index.** `threadmill_jobs_concurrency_pending_idx` covers `(concurrency_key, current_state_at, id)` for `ENQUEUED` / `SCHEDULED` / `AWAITING`; the regression test asserts the plan uses it. The `(current_state_at, id)` pair is the total in-key pending order.
- **Workflow outstanding counts use a partial index.** `threadmill_jobs_workflow_outstanding_idx` covers `(concurrency_key, workflow_root_id)` for non-terminal jobs so the first workflow hold acquisition is indexed.
- **Per-state counts come from `threadmill_job_counts`, maintained by an AFTER INSERT/UPDATE/DELETE trigger.** Never write a `COUNT(*)`-over-`threadmill_jobs` query. The `perStateCountsReadFromCounterTableNotFromJobsTable` test uses `EXPLAIN` to assert the plan does not touch the jobs table.
- **Migration runner bootstraps `threadmill_schema_history` itself** (`CREATE IF NOT EXISTS`). Migration SQL must not recreate it. Add new migrations as `V<n>__<description>.sql` under `src/main/resources/com/hemju/threadmill/store/postgres/migrations/` and register them in `MigrationRunner.SHIPPED_MIGRATIONS` (the runner does not classpath-scan; the explicit list is intentional for native-image compatibility).
- **Deadlocks on busy queue tables are normal.** Every write goes through `DeadlockRetry.run(...)` (recognises SQLSTATE `40P01` / `40001`, exponential backoff with jitter).
- **Testcontainers ≥ 2.0.** Module names use the `testcontainers-` prefix. `PostgreSQLContainer` lives in `org.testcontainers.postgresql` and is non-generic.
- **The host owns the connection pool.** The store accepts a `javax.sql.DataSource`; it does not create or close one.
- **Spring Boot Postgres schema handling is explicit.** Auto-configured Postgres stores run `threadmill.store.postgres.schema-mode=migrate` by default before constructing `PostgresJobStore`. `validate` is for externally-applied DDL, `none` skips schema handling, and `drop-and-migrate` requires `threadmill.store.postgres.allow-destructive-schema-reset=true` because it destroys Threadmill job data.

### Redis-specific

- **Crash-safe reliable-fetch claim.** Never a destructive pop. Java prepares the PROCESSING body first, then `claim_commit.lua` verifies version/state/queue membership and commits body, scalars, indexes, attempts, owner heartbeat, and counts together. A crash before the script leaves the job ENQUEUED; a crash after the script leaves a complete PROCESSING record for orphan recovery.
- **Cluster-safe key layout.** Every engine key starts with `{threadmill}:`, so every Lua script receives keys in one Redis Cluster slot. This supports Cluster topology/failover in v1; it deliberately does not shard job keys across masters.
- **Redis development reset deletes the Threadmill namespace.** `RedisJobStore.dropThreadmillKeys()` removes every key matching `{threadmill}:*` and leaves other Redis keys alone. Spring Boot wires this only when `threadmill.store.redis.reset-on-start=true` and `threadmill.store.redis.allow-destructive-reset=true`; it is for disposable dev/test data, not production cleanup.
- **Every active-state structure is a ZSET keyed under a fixed prefix.** ENQUEUED uses per-queue ZSETs; SCHEDULED / AWAITING / PROCESSING use single global ZSETs. PROCESSING is also indexed per-node so `touchOwnerHeartbeat` is one rescore call.
- **Counts live in `threadmill:counts` HASH** and are `HINCRBY`'d inside every state-changing Lua script. Never `SCARD` / `ZCARD` for live counts.
- **Mutex acquire-or-refresh is one Lua call** (`mutex_acquire.lua`). A separate SET-NX + GET + PEXPIRE has a microsecond race window where the key can expire between the SET-NX failing and the PEXPIRE running; the Lua script removes that window.
- **`findByHandlerSignature` uses a per-handler SET.** Members are added on insert and removed on hard-delete.
- **Capabilities advertise honestly.** `supportsRichSearch=false` — Redis cannot do deep ad-hoc metadata search; features that need it must skip-or-degrade on Redis.
- **Durability.** Redis-level. Run with `--appendonly yes`. Documented in the module Javadoc.
- **No eviction.** Redis for Threadmill is a durable job store, not a cache. `RedisJobStore` requires `maxmemory-policy noeviction` at startup; `allkeys-*` can split job hashes from indexes/counts, and `volatile-*` can delete TTL-backed heartbeats, leases, mutexes, or claim locks. Managed Redis that blocks `CONFIG GET` may use the explicit externally-validated override only after ops verifies the policy out of band.
- **Lua return value conventions.** Mixed-type Lua returns confuse Lettuce's `CommandOutput`. Pick one shape per script: always-string (`OK` / `STALE` / `EXISTS` / `VANISHED` / `ACQUIRED` / `REFRESHED` / `HELD`) → `ScriptOutputType.VALUE`; always-int → `INTEGER`; list of strings → `MULTI`.
- **Dedup records do not use Redis TTL.** They live as explicit keys plus an expiry index so a long-pending active job does not lose enqueue deduplication before completion.
- **Redis concurrency state is indexed under the engine slot.** Per-key counters, pending ZSETs, active workflow-hold HASHes, and workflow-count HASHes live below `{threadmill}:concurrency:{key}:...`; `claim_commit.lua` consults those structures before moving a job to `PROCESSING`, and state-changing scripts keep them in sync with the job hash and active-state indexes. Pending ZSET score ties are broken by job id, keyed inserts take the same short per-key claim lock used by claims so workflow child insertion cannot race the first root claim, and workflow outstanding counts are maintained incrementally instead of scanning active job hashes.

### Engine

- **One failure code path.** `JobRunner.recordFailure(...)` is the only place "a job failed" is expressed. Exceptions, timeouts, and orphan reclaim all flow through it with the right `FailureCause`. Adding a new failure source means routing through this method, not adding a parallel transition path.
- **Failed workflow steps abandon still-waiting descendants.** `WorkflowInterceptor` moves AWAITING workflow successors to `DELETED` when their predecessor fails or is quarantined, recursively. That keeps workflow-root concurrency from being held forever by descendants that can no longer become runnable.
- **Timeout detection.** The watchdog sets an `AtomicBoolean` *and* interrupts; the runner reads the flag, not just the interrupt status, because `InterruptedException` clears the interrupt before the catch block runs.
- **Retry is an interceptor, not engine code.** `RetryInterceptor` schedules the next attempt by transitioning the job to `SCHEDULED` with a backoff. Precedence is fully realised: per-job metadata override (`threadmill.retry.maxAttempts`, `threadmill.retry.initialBackoffSeconds`) > per-exception-type policy (most-specific class match wins) > global default.
- **`touchOwnerHeartbeat` never bumps `version`.** It is a non-state-changing operation; bumping version would cause spurious `StaleJobException` for an in-flight worker holding the version from claim time. All three stores agree.
- **Master election is a store-backed maintenance lease.** `NodeRegistry` records its heartbeat and acquires/renews `JobStore.acquireOrRenewMaintenanceLease(...)`; `MaintenanceCycle` runs only on the lease holder. Postgres stores this in `threadmill_leases`; Redis uses a TTL key plus Lua compare-and-renew / compare-and-release. The maintenance holder also removes stale node-registry heartbeat records older than `ProcessingNodeConfig.nodeHeartbeatRetention()` so churn-heavy deployments do not accumulate unbounded node rows / set entries.
- **Maintenance work has independent cadences.** `maintenancePollInterval` drives latency-sensitive recurring materialization, scheduled promotion, and orphan reclaim. `claimHeartbeat` refreshes owner heartbeats. `retentionInterval` drives slow cleanup of succeeded jobs, expired dedup keys, and stale node records.
- **Scoped values, not ThreadLocal.** `EngineScopedValues.CURRENT` is bound around `handler.run(...)`; virtual threads the handler spawns inherit it.
- **Storage fault tolerance.** The `Dispatcher`'s `CircuitBreaker` pauses the loop on a failure-count threshold and probes `store.capabilities()` to detect recovery. The cluster pauses, never crashes.
- **Wakeups are latency hints, not correctness.** `LocalWakeBus` connects same-JVM schedulers, recurring materialization, and scheduled-job promotion to matching dispatchers so claimable work does not wait out `pollInterval`; Spring wires the shared bus automatically, while manual core wiring must pass the same bus to `Scheduler` and `ProcessingNode`. `RemoteWakeChannel` publishes the same hints across nodes for durable stores: Postgres uses `LISTEN`/`NOTIFY` on `threadmill_wake`, Redis uses Pub/Sub on `{threadmill}:wake`. Wake publish/listen failures are ignored because `pollInterval` remains the correctness fallback.
- **Quarantine.** Handler-resolution and payload-deserialization failures land in `QUARANTINED`. `RetryInterceptor` deliberately skips `FailureCause.QUARANTINE` so a permanently broken job never causes a retry storm.
- **Long-running check-ins.** `JobExecutionContext.checkIn`, `updateProgress`, and `log` mutate the live job and are persisted by a per-job coalescing flush path. These writes never bump `version` and are conditional on the job still being `PROCESSING` and owned by the same node. Once a job checks in, `noProgressTimeout` replaces wall-clock `jobTimeout`.
- **Concurrency release piggybacks on the single terminal transition.** Success, exception failure, wall-clock timeout, no-progress timeout, orphan reclaim, and quarantine all leave `PROCESSING` through the persisted save path, so the next job for that key can claim after the transition lands.

### Producer deduplication

- **Dedup prevents duplicate enqueues, not duplicate executions.** At-least-once delivery and handler idempotency still apply.
- **Dedup identity is `(queue, dedupKey)`.** `dedupKey` is nonblank and at most 256 UTF-8 bytes; `dedupTtl` is positive and bounded by `maxDedupTtl` (default 30 days).
- **Expired keys still coalesce while the referenced job is active.** Maintenance removes expired dedup rows/keys only when the referenced job is terminal or gone.
- **Postgres uses `threadmill_dedup_keys`.** Do not use a partial unique index with `NOW()`; Postgres requires this as a table plus transactional enqueue semantics.

### Scheduling

- **Identity vs schedule-state for recurring.** `CronTask` is identity; `CronTaskScheduleState` is bookkeeping. Re-registering a task is never allowed to silently resurrect stale timing — `Scheduler.upsertCron` recomputes `nextRunAt` from the trigger relative to now, and preserves `inFlightJobId`. This is the catch-up-storm fix.
- **Missed-run policy is a contract.** `DROP` (default) materialises only the latest fire on a tick. `CATCH_UP` (opt-in) materialises every missed fire. The choice is per-task. Tests cover both modes.
- **Pile-up guard.** `RecurringMaterializer` refuses to materialise the next instance while the previous instance's `inFlightJobId` points to a non-terminal job.
- **Recurring ownership reconciliation is namespace-scoped.** `Scheduler.reconcileRecurring(namespace, desiredTasks)` upserts the desired definitions and deletes only tasks previously recorded as owned by that namespace. Spring annotation-driven recurring defaults the namespace from `threadmill.spring.recurring-namespace`, then `spring.application.name`; without either, startup only upserts discovered tasks and leaves stale cleanup manual.
- **Per-queue lanes are the starvation fix.** A `ProcessingNode` builder can declare multiple `QueueLane(name, workers)` entries; each gets its own `Dispatcher` with its own `Semaphore`. The `Scheduler.SYSTEM_QUEUE = "system"` constant is the canonical home for recurring / system jobs that must not be starved.
- **Queue-family lanes discover active queues.** `ProcessingNode.Builder.lane(pattern, workers, QueueWeights)` creates one shared-capacity lane for queues matching anchored `*` / `?` patterns. Weights are resolved once per discovery cadence, zero pauses a queue, and empty queues remain in the working set until `queueFamilyRetentionAfterEmpty` to avoid bursty rediscovery churn.
- **Queue-family wildcard shape follows the contract examples.** `project:*` matches one queue-name segment (`project:42`) and does not match `project:42:sub`; regex, character classes, double-star, `%`, and `_` are rejected at construction.
- **Priority within a queue.** Postgres orders by `priority DESC, id`. Redis ZSET score encodes `-priority * 1e13 + time_micros`. In-memory store sorts by `byPriorityDescThenId()`. All three agree on ordering.
- **Cron expression scope.** Five-field standard cron. Richer expressions (last-day-of-month, business days) can subclass or compose `CronExpression`, not rewrite it.
- **Javadoc trap.** Never write `*/` inside a Javadoc comment — even inside `{@code}` — it terminates the comment and trips the compiler.

### Advanced features

- **Results.** `JobExecutionContext.setResult(value)` is captured (typed) and persisted on the `SUCCEEDED` transition. The `JobResult` slot on `Job` was part of the model from the start.
- **Workflows.** `WorkflowInterceptor` listens on `onProcessingSucceeded` and promotes any `AWAITING` jobs whose `JobRelationship.parentId` equals the just-succeeded job's id.
- **Mutexes.** `JobStore.tryAcquireMutex(name, holder, leaseDuration)` is reentrant for the same holder. Leases expire so a dead holder cannot deadlock the system forever. `leaseDuration` must be strictly positive — enforced by `Mutexes.requirePositive(...)` in all three stores. Postgres uses ON-CONFLICT-DO-UPDATE-WHERE-expired; Redis uses the single-script `mutex_acquire.lua` plus a compare-and-delete Lua for release.
- **Node tags.** `ProcessingNode.Builder.tag(...)` adds a tag to this node. Jobs declare required tags via the metadata key `Dispatcher.REQUIRED_TAGS_META` (comma-separated). After claim, the dispatcher releases mismatched jobs to `SCHEDULED` with a short backoff. Dispatcher-side filtering; store-side filtering can be added later if profiling demands it.
- **Job replacement / transformation.** `JobStore.replaceJob(id, expectedVersion, JobReplacement)` swaps spec, queue, priority, or `scheduled_at` atomically on an `ENQUEUED` / `SCHEDULED` / `AWAITING` job. `PROCESSING` and terminal states reject (returns `false`); stale version throws `StaleJobException`. Public API: `Scheduler.replace(...)` / `Scheduler.replaceSpec(...)`. The Redis impl uses `replace_job.lua` so the body, scalars, and active-state index move atomically.

### Spring adapter

- **Spring Boot test classpath:** do not pull `slf4j-simple` — Spring Boot ships Logback, and the dual binding crashes the bootstrap. Use Spring's logging defaults.
- **`SpringJobHandlerResolver`** first tries a bean lookup; falls back to autowire-by-type so handlers can be either `@Component` beans or constructor-injected types.
- **`@Job` + `JobScheduler` are the preferred Spring API.** The registry discovers annotated `JobHandler<P>` beans, infers `P`, and fails startup if two handlers claim the same payload type. `JobScheduler` verifies the handler/payload pair at enqueue time so mistakes fail before a job is written.
- **No static facade.** Keep enqueue APIs injectable (`JobScheduler` for Spring, `Scheduler` for manual/core use) so applications can test, decorate, and configure them through their host container.

### Observability

- **Data first, UI second.** `EngineSnapshot.of(store)` returns counts, queue depths, oldest enqueued times, oldest processing heartbeat, node heartbeats, cron tasks, and store capabilities. The mountable UI is additive on top.
- **Metrics integration via Micrometer.** Per-state gauges, queue-depth gauges, oldest enqueued / processing heartbeat age gauges, processed / failed counters, refresh-error counter, claim-latency timer, and processing-time timer. Wired in via `JobInterceptor` and store operational queries — no special success path in the engine.
- **Tracing integration via OpenTelemetry API only.** `threadmill-tracing` is optional and does not pull an SDK/exporter. `ThreadmillTracing.asInterceptor()` emits one span per processing attempt, and `TracingJobStore` decorates store operations without changing behaviour. Spring auto-config adds user-provided `JobInterceptor` beans to the node.

### Formatting

- **Spotless gates `check`.** `./gradlew check` runs `spotlessCheck` alongside the tests. `./gradlew spotlessApply` auto-fixes.
- **Java is formatted with Palantir Java Format** (pinned in `gradle/libs.versions.toml`). 4-space indent, ~120-char lines, readable line breaks. Plus `removeUnusedImports`, fixed `importOrder` (`java`, `javax`, `jakarta`, third-party, `com.hemju`), `trimTrailingWhitespace`, `endWithNewline`.
- **Kotlin** (the convention plugins and every `*.gradle.kts`) is formatted with **ktfmt** in its `kotlinlang` style.
- **Markdown and `.gitignore`** get trim-trailing-whitespace + EOL-newline hygiene.
- **Local working notes stay out of formatting and artifacts.** If you add a new Spotless target, keep private local-only directories excluded.

### Regression coverage matrix

Every hard-won failure mode that has come up during development, and the test that catches its return:

| Failure mode | Test |
|---|---|
| Optimistic-lock version desync (in-memory increment before save confirmed) | `AbstractJobStoreContractTest.saveAtomicThrowsOnStaleVersion` + `failedSaveLeavesJobReusable` |
| Oversize-job corrupting in-memory version | `AbstractJobStoreContractTest.oversizeInsertIsRejectedCleanly` + `failedSaveLeavesJobReusable` |
| Vanished-job tolerance | `AbstractJobStoreContractTest.findVanishedJobIsEmpty` + `softDeleteOfVanishedIsNoOp` |
| 4-byte Unicode round-trip | `AbstractJobStoreContractTest.fourByteUnicodeRoundTrip` + `PostgresJobStoreRegressionTest.fourByteUnicodeRoundTripsThroughJsonBodyAndMetadata` + `JsonJobSerializerTest.fourByteUnicodeRoundTripsExactly` |
| Concurrent atomic claim across nodes | `AbstractJobStoreContractTest.claimReadyIsAtomicAcrossNodes` + `PostgresJobStoreRegressionTest.claimReadyIsAtomicAcrossManyConcurrentVirtualThreads` + `RedisJobStoreRegressionTest.claimReadyDoesNotLoseJobsWhenManyConcurrentNodesContend` |
| Postgres deadlock-retry recognition | `DeadlockRetryTest` (5 cases) + `PostgresJobStoreRegressionTest.deadlockRetryRecognisesDeadlockSqlState` |
| Per-state count must not scan the jobs table | `PostgresJobStoreRegressionTest.perStateCountsReadFromCounterTableNotFromJobsTable` (uses `EXPLAIN`) |
| Redis Lua-atomicity / crash-mid-processing | `RedisJobStoreRegressionTest.crashMidProcessingLeavesJobInProcessingIndexForOrphanRecovery` + `afterClaimIndexesAndHashAreInSync` + `everyMultiKeyTransitionLeavesNoOrphanIndexEntries` |
| Redis Cluster multi-key slot safety | `RedisKeysTest.allEngineKeysUseOneClusterSlot` |
| Producer dedup atomicity / expiry semantics | `AbstractJobStoreContractTest.enqueueIfAbsentCoalescesConcurrentProducers` + `expiredDedupKeyCreatesNewJobAfterTerminal` |
| Execution check-ins persist without version bumps | `AbstractJobStoreContractTest.executionUpdatesPersistWithoutBumpingVersion` |
| Long-running check-ins override wall-clock timeout but still fail on no progress | `ProcessingNodeTest.regularCheckInsLetLongRunningJobsOutliveWallClockTimeout` + `stalledCheckInJobFailsAfterNoProgressTimeout` |
| Per-job log bounds are exact | `JobLogTest.trimsOldestEntriesWhenEntryLimitIsExceeded` + `trimsOldestEntriesWhenByteLimitIsExceeded` |
| Orphan reclaim flows through the single failure path | `ProcessingNodeTest.orphanReclaimRoutesThroughTheSameFailurePathAndFiresOnProcessingFailed` |
| Per-job timeout interrupts a hung handler and runs failure interceptors | `ProcessingNodeTest.interruptsAHungJobAndRoutesItThroughTheSameFailurePath` |
| Poison job is quarantined without crashing the loop | `ProcessingNodeTest.poisonJobIsQuarantinedWithoutCrashingTheLoop` |
| Circuit-breaker decay on success | `CircuitBreakerTest.successDecaysTheCounter` |
| Store-outage cluster pause/resume | `StoreOutageTest.clusterPausesAndResumesAroundAStoreOutage` |
| Recurring catch-up-storm (DROP policy) | `SchedulingTest.dropPolicyDoesNotCauseACatchUpStorm` |
| Recurring CATCH_UP policy fires every missed run | `SchedulingTest.catchUpPolicyMaterializesEveryMissedFire` |
| Per-queue starvation prevention | `SchedulingTest.systemLaneIsNotStarvedByAdHocFlood` |
| Priority within a queue | `SchedulingTest.higherPriorityWithinAQueueIsClaimedFirst` |
| Retry precedence (per-job > per-exception-type > default) | `AdvancedFeatureTest.perJobOverrideBeatsPerExceptionTypePolicyBeatsGlobalDefault` |
| Mutex holder-crash does not block forever | `AdvancedFeatureTest.mutexExpiresSoADeadHolderDoesNotBlockForever` |
| Mutex contract: null / non-positive lease rejected; exclusive + reentrant + expires | `AbstractJobStoreContractTest.mutexRejectsNullLease` + `mutexRejectsNonPositiveLease` + `mutexLeaseSemantics` |
| Redis mutex sub-second leases honour millisecond precision | `RedisJobStoreRegressionTest.mutexLeaseIsExclusiveReentrantAndExpires` |
| Stale node-registry entries accumulate under process churn | `AbstractJobStoreContractTest.staleNodeHeartbeatsCanBeDeleted` + `ProcessingNodeTest.maintenanceRemovesStaleNodeHeartbeats` |
| Workflow successor promotion when predecessor succeeds | `AdvancedFeatureTest.aWorkflowSuccessorIsPromotedWhenItsPredecessorSucceeds` |
| Tagged-job routing on tagged vs untagged nodes | `AdvancedFeatureTest.taggedJobIsNotRunByUntaggedNodeAndIsLaterRunByTaggedNode` |
| Concurrency / no version desync under heavy contention | `InMemoryJobStoreConcurrencyTest.noVersionDesyncUnderHeavyContention` |
| In-memory owner heartbeat races terminal save and resurrects PROCESSING | `InMemoryJobStoreConcurrencyTest.heartbeatRefreshCannotResurrectTerminalJob` |
| State-machine illegal-transition rejection | `JobStateMachineTest.criticalIllegalTransitionsThrow` (covers PROCESSING→ENQUEUED, SCHEDULED→PROCESSING, ENQUEUED→ENQUEUED, QUARANTINED outbound, PROCESSED outbound) |
| Atomic job replacement (apply / stale / wrong-state / vanished) | `AbstractJobStoreContractTest.replaceJobAppliesNewDefinition` + `replaceJobThrowsOnStaleVersion` + `replaceJobRejectsNonReplaceableState` + `replaceJobOnVanishedIsFalse` + the in-memory engine variants in `JobReplacementTest` |
| Per-key read/write concurrency admits adjacent readers but serializes writers | `AbstractJobStoreContractTest.sharedJobsWithSameConcurrencyKeyClaimTogether` + `exclusiveJobsWithSameConcurrencyKeySerialize` |
| Concurrent nodes both passing an exclusive-key claim check | `AbstractJobStoreContractTest.concurrentClaimersCannotBothClaimExclusiveJobsForSameConcurrencyKey` |
| Later readers starving an earlier writer | `AbstractJobStoreContractTest.sharedJobDoesNotLeapfrogEarlierPendingExclusiveForSameKey` |
| Same-timestamp in-key pending jobs bypass strict order | `AbstractJobStoreContractTest.sameTimestampPendingOrderUsesJobIdTieBreaker` |
| Concurrency-blocked high-priority window hides claimable same-key work | `AbstractJobStoreContractTest.claimReadyScansPastBlockedSameKeyPriorityWindow` |
| Concurrency-blocked hot key hides independent lower-priority work | `AbstractJobStoreContractTest.claimReadyScansPastBlockedHotKeyToOtherKeys` |
| Writer/read invariant under sustained contention | `AbstractJobStoreContractTest.concurrencyKeyContentionMaintainsReadWriteInvariants` |
| Workflow-root concurrency released too early | `AbstractJobStoreContractTest.workflowConcurrencyIsInheritedThroughSimpleChain` + `workflowConcurrencyWithBranchingReleasesAfterAllSiblingsTerminate` + `workflowConcurrencyPartialFailureReleasesAfterEveryChildIsTerminal` |
| Workflow child inserted after root claim undercounts the workflow hold | `AbstractJobStoreContractTest.workflowChildInsertedAfterRootClaimKeepsWorkflowHold` + `RedisJobStoreRegressionTest.keyedInsertWaitsForConcurrencyClaimLockAndPreservesWorkflowHold` |
| Redis workflow outstanding count regresses to active-job scans or leaks across retry/delete | `RedisJobStoreRegressionTest.workflowCountsTrackChildrenBeforeFirstClaimAndReleaseAfterLastTerminalJob` + `workflowCountsTrackRetryBackIntoPendingState` + `workflowCountsTrackSoftDeleteOfPendingKeyedJob` |
| Postgres workflow outstanding count regresses to table scan | `PostgresJobStoreRegressionTest.workflowOutstandingCountUsesPartialIndex` |
| Postgres batched pending lookup regresses to table scan | `PostgresJobStoreRegressionTest.batchedConcurrencyPendingLookupUsesPartialIndex` |
| Failed intermediate workflow step leaves waiting descendants holding a concurrency key forever | `AbstractJobStoreContractTest.workflowConcurrencyIntermediateFailureAbandonsWaitingDescendants` |
| Poisoned workflow root holding a key forever | `AbstractJobStoreContractTest.poisonedWorkflowRootReleasesConcurrencyKeyOnQuarantine` |
| Concurrency hold not released on every processing exit | `ProcessingNodeTest.successfulConcurrencyJobReleasesKeyForNextJob` + `exceptionFailureReleasesConcurrencyKeyForNextJob` + `timeoutFailureReleasesConcurrencyKeyForNextJob` + `noProgressTimeoutReleasesConcurrencyKeyForNextJob` + `orphanReclaimReleasesConcurrencyKeyForNextJob` + `quarantineReleasesConcurrencyKeyForNextJob` |
| Postgres concurrency pending check regresses to table scan | `PostgresJobStoreRegressionTest.concurrencyPendingCheckUsesPartialIndex` |
| Queue-family pattern admits non-contract queue names | `QueueFamilyTest.patternMatchingIsAnchoredAndSupportsStarAndQuestion` + `rejectsPatternFormsOutsideTheQueueFamilyGlobShape` |
| Queue-family dynamic weights called on every claim | `ProcessingNodeTest.queueFamilyDynamicWeightsAreResolvedOncePerDiscoveryCadence` |
| Queue-family zero-weight pause accidentally drains work | `ProcessingNodeTest.queueFamilyZeroWeightQueueIsNotDrained` |
| Queue-family weighted dispatch becomes batchy or unfair | `ProcessingNodeTest.queueFamilyUniformWeightsSpreadFirstClaimsAcrossMatchingQueues` + `queueFamilyStaticWeightsPreferHighWeightQueueSmoothly` |
| Empty queue retained forever in a queue-family lane | `ProcessingNodeTest.queueFamilyEmptyQueueIsDroppedAfterRetentionWindow` |
| Concurrency-blocked queue-family member starves other queues | `ProcessingNodeTest.queueFamilyLaneSkipsConcurrencyBlockedQueueAndDrainsAnother` |
| Concurrent EXCLUSIVE-key claimers collectively stall (reference issue #694 shape) | `AbstractJobStoreContractTest.unblockUnderContentionDoesNotSkipKeys` |
| Fail path consults `RetryInterceptor` for every cause | `ProcessingNodeTest.failPathRoutesThroughRetryInterceptorForAllCauses` |
| Scheduler restart double-enqueues a recurring task | `SchedulingTest.schedulerRestartDoesNotDoubleEnqueueACronTask` |
| Oversized failure metadata blocks the FAILED save | `JsonJobSerializerTest.oversizedFailureMetadataDoesNotBlockSave` + `ProcessingNodeTest.oversizedExceptionTraceDoesNotBlockFailedTransition` |
| Paused queue starves unpaused queues | `ProcessingNodeTest.pausedQueueDoesNotStarveUnpausedQueues` + `AbstractJobStoreContractTest.pausingOneQueueDoesNotAffectAnother` |
| Per-queue pause primitive contract (idempotency, claim-skip, isolation) | `AbstractJobStoreContractTest.pauseQueueIsIdempotent` + `resumeQueueIsIdempotent` + `listPausedQueuesReflectsPausesAndResumes` + `claimReadyReturnsEmptyForPausedQueue` |
| Bulk-enqueue atomicity (size-check rejects whole batch, no version desync) | `AbstractJobStoreContractTest.insertAllAtomicallyAdvancesVersionForEverySuccessfulJob` + `insertAllRejectsBatchAndLeavesAllVersionsAtZeroIfAnySerializationFails` + `insertAllPreservesIdOrderInReturnedList` + `insertAllUnderContentionDoesNotDeadlockBeyondRetry` + `insertAllAcceptsConcurrencyKeyedJobsWithoutFallback` |
| Dispatcher waits out poll interval on bursty load | `ProcessingNodeTest.idleWorkerWakesDispatcherEarlyUnderBurstyLoad` + `WakeSignalTest.signalWakesAnAwaitingThread` + `multipleSignalsCollapseIntoOnePermit` |
| Scheduled promotion leaves a local dispatcher asleep until `pollInterval` | `SchedulingTest.scheduledPromotionWakesLocalDispatcherWhenJobBecomesClaimable` |
| Enqueue inside a Spring transaction fires before commit (default-on after-commit) | `TransactionAwareJobSchedulerTest.enqueueInsideTransactionIsNotVisibleBeforeCommit` + `enqueueInsideTransactionIsRolledBackOnRollback` + `ThreadmillAutoConfigurationTest.defaultsToTransactionAwareJobScheduler` + `immediateEnqueueModeUsesPlainJobScheduler` |
| Spring + Postgres enqueue joins the caller SQL transaction when requested | `SpringPostgresTransactionBoundaryTest.enqueueCommitsWithCallerTransactionAndWakesAfterCommit` + `enqueueRollsBackWithCallerTransaction` + `dedupRollsBackWithCallerTransaction` + `joinTransactionFailsFastWhenCallerTransactionUsesDifferentDataSource` + `TransactionJoinedJobSchedulerTest.enqueueInsideTransactionWritesImmediatelyButWakesAfterCommit` |
| Mixed-payload Spring batch bypasses handler/payload validation | `TransactionAwareJobSchedulerTest.enqueueAllRejectsMixedPayloadsBeforeWritingAnything` |
| PostgresJobStore starts against pre-PG18 server | `PostgresVersionGateTest.refusesToStartAgainstPrePostgresEighteenServers` |
| Threadmill Spring auto-config silently runs on an unsupported Spring Boot major | `SpringBootVersionGuardTest.failsFastOnSpringBootThree` + `failsFastOnSpringBootTwo` + `failsFastOnUnparseableVersion` + `failsFastOnNullVersion` + `acceptsSpringBootFourReleases` |
| Engine startup banner drops store identity, lane count, or worker totals | `ThreadmillLifecycleBannerTest.startupBannerLogsStoreCapabilitiesAndLanes` + `bannerWorksWithDefaultSingleLaneNode` |
| A store implementation returns null / blank from `JobStore.describe()` | `AbstractJobStoreContractTest.describeReturnsNonBlankString` (runs against in-memory, Postgres, Redis) |
| `JobDefinitionMigrator` rewrites only replaceable-state jobs (ENQUEUED / SCHEDULED / AWAITING), enforces the new handler type, respects `max`, and rejects nulls | `JobDefinitionMigratorTest.rewritesEnqueuedScheduledAndAwaitingJobsToNewHandlerSignature` + `leavesProcessingAndTerminalJobsUntouched` + `appliesPayloadSpecMigrationBeforeRewritingHandler` + `newHandlerTypeOverridesAnythingTheSpecMigrationReturned` + `respectsMaxBatchSizeAndReturnsZeroForNonPositiveMax` + `nullArgsAreRejected` |
| Redis `replace_job.lua` updates the `by_handler` SET when handler signature changes, leaves it alone otherwise | `RedisJobStoreRegressionTest.replaceJobMovesIdAcrossByHandlerIndexWhenHandlerSignatureChanges` + `replaceJobLeavesByHandlerIndexUnchangedWhenHandlerSignatureIsSame` |
| Redis concurrency-blocked claim corrupts queue, counts, or pending indexes | `RedisJobStoreRegressionTest.blockedConcurrencyClaimLeavesPendingJobEnqueuedAndIndexesIntact` |
| Redis active-state / count / concurrency indexes drift after reliable-fetch transitions | `RedisJobStoreRegressionTest.assertRedisIndexesConsistent` applied across claim, blocked claim, serializer failure, terminal transition, orphan reclaim, replace, retry, and delete regressions |
| Every standard Redis eviction policy (`allkeys-*`, `volatile-*`) fails Threadmill startup | `RedisJobStoreRegressionTest.startupRejectsEvictionPolicy` (parameterized over 7 policies) |
| `RedisSafetyValidation.externallyValidatedMode` bypasses the eviction-policy gate for managed Redis where `CONFIG GET` is unavailable | `RedisJobStoreRegressionTest.externallyValidatedModeAllowsStartupAgainstEvictionPolicy` |
| `Scheduler.reconcileRecurring` for one namespace deletes only that namespace's stale tasks and leaves other namespaces and unowned tasks alone | `SchedulingTest.reconcileRecurringForOneNamespaceLeavesOtherNamespaceAndManualTasksUntouched` + `reconcileRecurringWithEmptyDesiredSetDeletesAllTasksOwnedByThatNamespace` |
| Class-renamed `@Recurring` handler with default name deletes the old owned task and registers the new one | `ThreadmillAutoConfigurationTest.renamingARecurringHandlerWithoutAnExplicitNameDeletesTheOldOwnedTaskAndRegistersTheNew` |
| Remote wake publish/listen regresses or Spring stops wiring wake publishers/listeners | `PostgresRemoteWakeChannelTest.publishDeliversWakeToListener` + `RedisRemoteWakeChannelTest.publishDeliversWakeToListener` + `ThreadmillAutoConfigurationTest.localWakePublishesToConfiguredRemoteWakeChannel` + `remoteWakeDisabledDoesNotCreateChannel` + `remoteWakeLifecycleStartsListenerOnlyWhenNodeRuns` |
| OpenTelemetry tracing misses processing/store span attributes or exceptions | `ThreadmillTracingTest.processingSpanIsCurrentAndRecordsSuccessAttributes` + `processingFailureRecordsExceptionAndCause` + `storeDecoratorRecordsClaimCountAndStoreDescription` |
| `CronExpression` `*/0` infinite loop and Sunday-as-7 corruption (`5-7`, `1-7`, `*/7`) | `CronExpressionTest.parseRejectsZeroAndNegativeSteps` + `sundayAsSevenWorksInRangesListsAndSteps` |
| Job replacement strips concurrencyKey / concurrencyMode / workflowRootId | `AbstractJobStoreContractTest.replaceJobPreservesConcurrencyKeyModeAndWorkflowRoot` |
| Deserializing an ownerless checked-in job fabricates a phantom owner heartbeat | `JsonJobSerializerTest.ownerlessJobWithCheckinDoesNotFabricateOwnerHeartbeatOnRoundTrip` |
| Failure-message truncation splits surrogate pairs or exceeds maxBytes; malformed wire escapes as raw runtime exceptions | `JsonJobSerializerTest.capFailureMessageNeverSplitsSurrogatePairsAndRespectsMaxBytes` + `malformedWireYieldsSerializationExceptionNotRawRuntimeExceptions` |
| Persisted type tag initializes an arbitrary classpath class before the payload assignability check | `JsonJobSerializerTest.deserializeArgumentRejectsNonPayloadTypesWithoutRunningTheirInitializers` |
| Unbounded metadata or state-history entry count blocks the terminal FAILED/SUCCEEDED save | `JsonJobSerializerTest.hugeMetadataAndLongRetryHistoryStillFitTheTerminalSave` |
| Failed SUCCEEDED-save leaves the job stuck in PROCESSING (and its key held) forever | `ProcessingNodeTest.transientSucceededSaveFailureIsRetriedAndTheJobSucceeds` + `failedSucceededSaveRoutesThroughTheSingleFailurePathAndReleasesTheKey` + `oversizedHandlerResultIsDroppedRatherThanBlockingTheSucceededSave` |
| Per-job timeout ignored in the watchdog's initial delay; malformed timeout metadata disables enforcement | `ProcessingNodeTest.perJobTimeoutShorterThanTheGlobalDefaultFiresOnTime` + `malformedTimeoutMetadataStillEnforcesTheGlobalTimeout` |
| Retry backoff off-by-one, sub-second truncation to zero, malformed retry metadata cancelling retry, racy policy map | `RetryInterceptorTest.firstRetryDelayEqualsInitialBackoff` + `subSecondBackoffIsNotTruncatedToZero` + `malformedRetryMetadataFallsBackToTheDefaultPolicyAndStillRetries` + `concurrentPolicyRegistrationDoesNotBreakTheFailurePath` |
| Recurring instance loses the back-link to its recurring definition | `SchedulingTest.recurringInstancesCarryTheirCronTaskName` + `JsonJobSerializerTest.jobRoundTripsAllCoreFields` |
| CATCH_UP instances indistinguishable (nominal fire time discarded) | `SchedulingTest.catchUpInstancesCarryDistinctNominalFireTimes` |
| Workflow fan-out beyond one batch strands children; deep-chain abandonment recursion | `AdvancedFeatureTest.workflowFanOutBeyondOneBatchPromotesEveryChild` + `workflowFanOutBeyondOneBatchAbandonsEveryChildOnFailure` + `deepWorkflowChainIsAbandonedIterativelyWithoutRecursion` |
| Retention capped at 100 deletions per interval; FAILED/DELETED/QUARANTINED never swept; retention age hardcoded | `ProcessingNodeTest.retentionSweepDrainsBeyondOneBatchAndCoversAllTerminalStates` + `ProcessingNodeConfigTest.terminalStateRetentionAgesAreConfigurableAndValidated` |
| Dispatch failure mid-batch abandons remaining claimed jobs and can leak a worker permit | `ProcessingNodeTest.dispatchFailureMidBatchDoesNotAbandonRemainingClaimedJobs` |
| WakeSignal permit-cap check-then-act race; idle gate misses concurrent finishers | `WakeSignalTest.simultaneousSignalersCollapseIntoOnePermit` + `ProcessingNodeTest.idleWorkerWakesDispatcherEarlyUnderBurstyLoad` |
| Newly discovered queue-family queue starts at pass 0 and monopolizes claims | `ProcessingNodeTest.queueFamilyLateJoinerDoesNotMonopolizeClaims` |
| concurrencyMode without key silently dropped; queue-name validation divergent across backends | `JobTest.concurrencyModeWithoutKeyIsRejectedLoudly` + `queueNamesAreValidatedAtTheModelBoundary` + `SchedulingTest.scheduleInFiresAfterTheDelayWithTypedHandlerAgreement` |
| Typed-ID JSON shape regresses to object form (dashboard React error #31) | `TypedIdJsonTest.typedIdsSerializeAsBareStringsAndRoundTrip` + `typedIdsRoundTripAsMapValuesAndRecordComponents` |
| Dual master after a hung registry tick; heartbeat starved by master work; CATCH_UP burst unbounded; claimHeartbeat ≥ heartbeatTimeout misconfiguration | `NodeRegistryTest.mastershipSelfExpiresWhenATickHangsPastTheLeaseDuration` + `SchedulingTest.catchUpBacklogIsCappedPerTickWithCarryOver` + `ProcessingNodeConfigTest.heartbeatTimeoutMustBeAtLeastTwiceTheClaimHeartbeat` |
| upsertCron read-modify-write clobbers a concurrently-set inFlightJobId; FAILED pile-up-guard window semantics | `SchedulingTest.materializerSkipsATaskWhoseStateMutexIsHeld` + `upsertCronWaitsForTheTaskMutexAndPreservesInFlightTracking` + `failedInFlightInstanceDoesNotBlockTheNextMaterialization` |
| In-memory claim blind-put loses concurrent softDelete/replaceJob writes; lock-free writers tear claim-time concurrency decisions | `InMemoryJobStoreConcurrencyTest.claimRacingSoftDeleteNeverResurrectsADeletedJob` + `claimRacingReplaceJobNeverLosesTheReplacement` |
| Retried EXCLUSIVE workflow root deadlocks against its own AWAITING children (in-memory hold inference) | `InMemoryJobStoreConcurrencyTest.retriedExclusiveWorkflowRootCanReclaimUnderItsOwnHold` |
| In-memory priority comparator overflow, zombie execution updates, insertAll phantom visibility, mutex map growth | `InMemoryJobStoreHardeningTest.integerMinValuePrioritySortsLastLikeTheRelationalBackends` + `zombieExecutionUpdateFromAPreviousAttemptIsRejected` + `insertAllWithDuplicateIdsRejectsTheBatchWithoutPhantomVisibility` + `insertAllDuplicateOfAnExistingJobLeavesTheStoreUntouched` |
| Workflow hold double-decrements on retry resurrect (EXCLUSIVE key released while a descendant runs) | `AbstractJobStoreContractTest.workflowRootRetryAfterFailureCanReclaimUnderItsOwnHold` + `workflowMemberRetryDoesNotDoubleReleaseTheHold` |
| `emitPendingSql` executes DDL despite being the inspect-only API | `PostgresJobStoreRegressionTest.emitPendingSqlOnAFreshDatabaseIsReadOnlyAndPrependsHistoryDdl` |

### Postgres-layer improvements (engagement notes)

A standing audit against a reference Postgres job system flagged five additive improvements; all landed under the `[postgres-improvements]` commit trailer, plus a PG18-only baseline bump. These four patterns are worth recognising before working in the same areas:

- **Bulk-insert atomic-failure pattern.** `JobStore.insertAll` pre-serializes every job in the batch *before* any write — `OversizedJobException` rejects the whole batch with no in-memory `version` mutated on any input. Three concrete impls demonstrate the pattern: in-memory pre-serializes under the claim mutex, Postgres pre-serializes outside the transaction then uses `PreparedStatement.addBatch()` inside, and Redis pre-serializes plus a single `insert_all.lua` that does a two-pass EXISTS-then-write under one atomic Lua call. Concurrency-keyed jobs are NOT a fallback trigger — claim-time concurrency handles them safely.
- **Spring enqueue transaction modes.** `threadmill.spring.enqueue-mode` has three explicit choices: `after_commit` (default) defers job rows until Spring's `afterCommit`, `immediate` writes outside the caller transaction, and `join_transaction` makes normal, scheduled, bulk, and deduplicated enqueues part of the caller's JDBC transaction. `join_transaction` is intentionally Spring + Postgres only and requires the auto-configured `PostgresJobStore` to use `SpringPostgresTransactionBoundary`; Redis cannot join a SQL transaction. Wake signals for joined writes fire only after commit.
- **Truncation at snapshot time.** `JsonJobSerializer.serializeJob(snapshot, capabilities)` trims `JobLog` from the head and caps FAILED / QUARANTINED state-history messages with a sentinel suffix BEFORE the overall size check. For terminal-save states (SUCCEEDED / FAILED / DELETED / QUARANTINED) it additionally elides middle state-history entries beyond `maxStateHistoryEntries` (creation entry + most recent kept) and drops the largest non-`threadmill.` metadata entries beyond `maxMetadataBytes`, recording both under the `threadmill.truncated.*` marker keys — so a terminal save can never be blocked by user/engine-growable areas, while non-terminal saves keep the loud §6 oversize rejection. This is the only place in the codebase where the failure-detail policy lives — the single failure code path in `JobRunner.recordFailure` is unchanged. If you add a new state that needs failure detail, add it to `trimFailureMessages` in `JsonJobSerializer`.
- **Wake-signal pattern.** `WakeSignal` is a single-permit `Semaphore`. The cap and the all-busy → at-least-one-idle gate are both load-bearing: without the cap, many concurrent finishes burn CPU on `release()` calls; without the gate, steady-state high throughput busy-spins. Remote wake must feed `ProcessingNode.wake(queue)` and the existing dispatcher signal rather than inventing a second local wake primitive.
- **Per-queue pause primitive.** Threadmill claims by *explicit* queue at the store layer, so we do not inherit the reference's `DISTINCT(queue)` performance footnote. Do not add a wildcard claim selector at the store layer "for parity" — the explicit-queue shape is the win. Pause and resume are idempotent across all backends.

### Manual soak harness (engagement notes)

A standing piece of operability work: a per-backend, per-scenario stress harness under `threadmill-soak` that produces a self-contained artifact directory rich enough for an AI agent to read cold. Landed under the `[soak-harness]` commit trailer. Worth recognising:

- **Distinct from the `:soakRegression` task and `threadmill-simulation`.** The `:soakRegression` JUnit task runs fixed throughput / recovery scenarios across all three backends in one go (part of `productionCheck`). `threadmill-simulation` owns correctness simulations and worker-process churn. The load soak harness picks **one** backend and **one** scenario per run, takes `-P` knobs (`scenario`, `duration`, `jobsPerSecond`, `workerCount`, `nodes`, `outputDir`, `runId`, `force`, `postgresUrl`, `redisTopology`), and writes a self-contained directory under `build/soak/`. The three suites are complementary; don't fold them.
- **Trace format duplicated, not shared.** The harness has its own `SoakTraceWriter` and an extended event vocabulary (`enqueued`, `claimed`, `started`, `succeeded`, `failed`, `timed_out`, `quarantined`, `retried`, `lock_acquired`, `lock_released`, `queue_paused`, `queue_resumed`, `node_started`, `node_stopped`, `bulk_batch_committed`, `bulk_batch_rejected`). The simulation module's verifier stays untouched; the soak harness ships a separate `InvariantChecks` library with ten named invariants (`atLeastOnce`, `exclusivityHeld`, `strictInGroupOrder`, `workflowLockHeldContinuously`, `retryBudgetRespected`, `noLockLeaks`, `pauseObeyed`, `weightRatioWithinTolerance`, `noProgressTimeoutKills`, `bulkInsertAtomic`). Drift is acceptable because neither module reads the other's output.
- **Engine claim is invisible to the `JobInterceptor` chain.** `onStateChange` is only fired by `JobRunner` (PROCESSING→SUCCEEDED / FAILED / QUARANTINED) — never by the dispatcher for the ENQUEUED→PROCESSING claim transition. The harness's `SoakInterceptor` therefore emits both `claimed` and `started` from `onProcessingStarting` at the same instant: an AI agent grepping for either term finds it, and the lifecycle vocabulary stays complete.
- **`StalledWork` must call `checkIn()` once to test the no-progress path.** Per the `noProgressTimeout` contract, the engine only switches a job from wall-clock `jobTimeout` to `noProgressTimeout` once it has checked in at least once. The `LongRunningScenario`'s stalled handler does an initial check-in then goes silent — that's the only way the no-progress kill fires before the wall-clock timeout.
- **Weight ratio invariant is ordinal, not absolute.** The pass-based weighted-fair dispatcher in `Dispatcher.pickFamilyQueue` does deliver the absolute ratio in steady state, but short runs with shallow backlog can't accumulate enough picks for the ratio to converge. The `weightRatioWithinTolerance` invariant therefore asserts the directional ordering (heavier ≥ lighter within slack) plus the zero-weight guarantee, not the absolute ratio. The existing core-test `queueFamilyStaticWeightsPreferHighWeightQueueSmoothly` exercises the absolute ratio with `BlockingHandler` + `claimBatchSize=1` if a stricter check is ever needed.
- **`summary.json` is schema-validated.** The schema file lives at `threadmill-soak/src/main/resources/com/hemju/threadmill/soak/harness/summary.schema.json` (JSON Schema draft 2020-12). The harness validates every generated summary against it before writing the file (Networknt `json-schema-validator`). The `SummarySchemaTest` (untagged) asserts the contract end-to-end. Adding a new top-level field to `SummaryReport` is a deliberate decision — update the schema, the writer, and the test in the same commit.
- **`-PoutputDir` collision check.** Default `outputDir` is timestamped per run, so manual invocations never collide. An explicit `-PoutputDir` that already exists is rejected unless `-Pforce=true` — protecting the operator from accidentally overwriting a previous run's artifacts.
- **Mixed-workload liveness edge is pinned.** The original `soakMemory` symptom stranded a small number of jobs in `ENQUEUED` while one job sat in `PROCESSING` forever under the JUnit JVM at 8 s × 50 jobs/sec × 4 workers (401 enqueued, 389 succeeded, 13 active after drain budget). `ProcessingNodeTest.mixedWorkloadQueueFamilyDrainsAndLeavesNoConcurrencyHoldBehind` now reduces the shape into a deterministic in-memory regression: queue-family dispatch, mixed SHARED / EXCLUSIVE keyed work, drain assertion, orphan-reclaim check, and sentinel claims that prove no concurrency key remains held without a live processing owner. The untagged harness smoke still uses `rw-lock-stress` to check the output contract reliably; if the full load harness reproduces a stronger variant, reduce that trace into a named regression before changing concurrency bookkeeping.

### Reusable skills

- **Add-a-store skill** (informal): create a `threadmill-store-X` module, implement `JobStore`, and add an integration test class extending `AbstractJobStoreContractTest`. Pass every contract test (currently 62) before adding any backend-specific tests.
- **Turn a bug into a regression test**: add the named test in the closest `*RegressionTest` (per backend) or in `AbstractJobStoreContractTest` (cross-backend), then fix the code. Add a row to the matrix above.
- **Format a contribution**: `./gradlew spotlessApply` before sending. `./gradlew check` is the gate.
- **Run the full check**: `./gradlew check` runs every test + the Spotless gate.
- **Run the soak regression suite explicitly**: `./gradlew :threadmill-soak:soakRegression` — sustained throughput + recurring no-skip + induced container-pause recovery on in-memory, real PostgreSQL, and real Redis.
- **Run the load soak harness**: `./gradlew :threadmill-soak:soakMemory` / `soakPostgres` / `soakRedis` / `soakAll` — operator-driven, per-backend, per-scenario stress with `-P` knobs (`scenario`, `duration`, `jobsPerSecond`, `workerCount`, `nodes`, `outputDir`, `runId`, `force`, `postgresUrl`, `redisTopology`). Eight scenarios (`mixed-workload` default; `rw-lock-stress`, `weighted-queues`, `retry-storm`, `long-running`, `pause-resume`, `bulk-enqueue`, `crash-recover`). Each run writes a self-contained directory under `build/soak/<scenario>-<backend>-<timestamp>/` with `summary.json` + `summary.md` + `trace.jsonl` + `metrics.jsonl` + `latencies.jsonl` + `lock-events.jsonl` + `invariants.json` + `config.json` + log captures. `summary.json` validates against a JSON Schema 2020-12 file shipped at `threadmill-soak/src/main/resources/com/hemju/threadmill/soak/harness/summary.schema.json`. See `threadmill-soak/README.md` for the AI-drop-in workflow.
- **Run production readiness**: `.agents/skills/run-production-check/SKILL.md`.
- **Add a store regression**: `.agents/skills/add-store-regression/SKILL.md`.
- **Release readiness**: `.agents/skills/release-readiness/SKILL.md`.
- **Operability check**: `.agents/skills/operability-check/SKILL.md`.

### Build & module-layout decisions worth remembering

- **`buildSrc/` is kept, not inlined.** The convention plugin `threadmill.java-base` holds the Java 25 toolchain config, `-Xlint:all -Werror`, Javadoc strictness, and JUnit Platform wiring. Keeping that in one place avoids duplicating the same compile / test policy across every module and preserves a single point of truth. Convention plugins also avoid `buildSrc` re-evaluation surprises that earlier Gradle classpath plumbing had. Don't inline.
- **`MigrationRunner.SHIPPED_MIGRATIONS` is an explicit list, not a classpath scan.** The list is what makes the migration runner native-image friendly and what makes the order deterministic. New migrations go on the end of the list; do not collapse the list into a directory scan.
- **The convention plugin's `-Werror` is intentional.** A warning is a defect; promoting it to a build break catches it at the cheapest possible moment.

### Open items / follow-on work

These are deliberately additive and design-compatible with the current model — none of them needs a model migration.

- **Task 2 of the v1-readiness finishing pass — first-class Spring Boot integration.**
  - **Landed:** `ThreadmillAutoConfiguration` carries `@AutoConfigureAfter` for `DataSourceAutoConfiguration` and `RedisAutoConfiguration`. The `JobStore` bean resolves by precedence (explicit Redis config → Postgres if a `DataSource` is present and the Postgres store is on the classpath → in-memory fallback with a loud warning). `ThreadmillLifecycle` is a `SmartLifecycle` at phase `Integer.MAX_VALUE / 2` so the engine starts after infrastructure beans and stops before them on graceful shutdown.
  - **Deferred:** Actuator integration (`HealthIndicator`, `MeterBinder`, `/actuator/threadmill` endpoint) is held back from v1 because Spring Boot 4.0's actuator surface is still being reorganised — health and Micrometer integration moved out of the main `spring-boot-actuator` artifact during the milestone series and the final shape isn't pinned yet. Re-attempt after SB4 GA. Spring AOT `RuntimeHints` for native image is deferred for the same reason — it needs a stable actuator target first. A `threadmill-example/spring-boot-4/` sample app is deferred to the same follow-up. (Spring Boot 3 is intentionally not supported and there is no SB3 sample app planned.) After-commit enqueue is already default-on (postgres-improvements Phase 5).
- **Task 3 of the v1-readiness finishing pass — per-module READMEs and full docs.** Each module gets a `README.md` at its root (`threadmill-core`, the three stores, `threadmill-spring-boot`, `threadmill-test-support`, `threadmill-metrics`, `threadmill-dashboard-api`, `threadmill-dashboard-ui`, `threadmill-dashboard-spring`, `threadmill-soak`, `threadmill-simulation`, `threadmill-example`). The Postgres README carries the full schema; the Redis README carries the full key layout and Lua script inventory; the Spring README carries the SmartLifecycle phase choice and Actuator endpoints. `docs/` is restructured around the new shape: `index`, `getting-started`, `architecture`, `spring-boot`, `transactions` (deep dive — atomic boundaries per backend, handler-is-not-in-our-transaction, at-least-once + idempotency, outbox pattern), `configuration`, `concurrency`, `queues`, `operations`, `troubleshooting`, `migration`, `handlers`, and dedicated comparison pages for named reference systems. Every doc snippet is a real compiled file under `threadmill-example/snippets/`. The bar is "an AI agent can use Threadmill to replace an existing job/scheduler system without reading source code."
- **Task 4 of the v1-readiness finishing pass — `threadmill-simulation` module — landed.** New module, separate from `threadmill-soak` (load/performance) and `threadmill-example` (teaching). The short correctness simulation runs 50 projects with `Import` (EXCLUSIVE) and `Export` (SHARED) jobs, 400 jobs over the run (small enough to finish in seconds), random failure injection (5% exception, 0.5% hang), mid-run pause/resume, half-via-`insertAll` bulk-enqueue sample. Records JSON-lines traces under `build/simulation/`; `TraceVerifier` asserts at-least-once, concurrency exclusion (EXCLUSIVE-vs-anything, SHARED-vs-EXCLUSIVE), lock pairing, and pause-obeyed. Gradle entry points: `:threadmill-simulation:simulate` (all three backends), `simulateMemory`, `simulatePostgres`, `simulateRedis`. The Gradle task fails (non-zero exit) when any backend doesn't drain or produces a verifier violation. The worker-churn simulation lives under `com.hemju.threadmill.simulation.workerchurn` and runs through `simulateWorkerChurnPostgres` / `simulateWorkerChurnRedis` against shared local datastores, writing traces to `build/simulation/worker-churn-<timestamp>-<backend>.jsonl` by default.
- **Batches.** The child relationship is already in the model; a `BatchCompletionInterceptor` over multiple children is the natural shape.
- **External-trigger jobs.** The `PROCESSED` state is reserved; this needs an external-signal API plus an escape-hatch timeout.
- **Rate limiters.** A store-side token-bucket primitive; Redis can use the standard Lua bucket pattern.
- **Additional dashboard adapters** beyond Spring MVC, reusing the portable dashboard API and static UI assets.
- **Reproducible production-grade benchmarks** (separate from the soak suite) and artifact publishing automation.
