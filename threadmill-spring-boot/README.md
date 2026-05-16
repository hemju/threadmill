# threadmill-spring-boot

Spring Boot auto-configuration for Threadmill. Wires the engine into a Spring
Boot application with sensible defaults, automatic store selection, lifecycle
management via `SmartLifecycle`, and transaction-aware enqueue semantics.

## Quick start

```kotlin
dependencies {
    implementation("com.hemju:threadmill-spring-boot:VERSION")
    // Optional, pick one or none — auto-detected:
    implementation("com.hemju:threadmill-store-postgres:VERSION")
    // implementation("com.hemju:threadmill-store-redis:VERSION") // pulled in transitively
}
```

```java
@Component
@Job(queue = "email", timeout = "PT2M", maxRetries = 5)
final class SendEmailHandler implements JobHandler<SendEmail> {
    @Override public void run(SendEmail payload, JobExecutionContext ctx) { … }
}

@RestController
final class MailController {
    private final JobScheduler jobs;
    MailController(JobScheduler jobs) { this.jobs = jobs; }

    @PostMapping("/mail")
    JobId send(@RequestBody SendEmail command) {
        return jobs.enqueue(SendEmailHandler.class, command);
    }
}
```

That's the whole integration. `@Job` discovery happens at context
start; `JobScheduler` verifies the handler/payload pair at enqueue time.

## Store selection

`ThreadmillAutoConfiguration` resolves a `JobStore` bean by precedence:

1. **Explicit Redis config** (`threadmill.store.redis.*` populated) → `RedisJobStore`.
2. **DataSource present + Postgres store on classpath** → `PostgresJobStore`
   wired from the application's existing `DataSource`.
3. **Otherwise** → `InMemoryJobStore` with a loud warning that jobs won't
   survive restart.

Define your own `JobStore` bean to override everything.

## Lifecycle (`SmartLifecycle`)

`ThreadmillLifecycle` wraps `ProcessingNode` and registers at phase
`Integer.MAX_VALUE / 2`. That's deliberate:

- Spring's `DataSourceAutoConfiguration` and `RedisAutoConfiguration` register
  their lifecycle objects at the default phase (`Integer.MAX_VALUE`).
- Spring stops higher phases first.
- So at this phase: engine starts **after** the `DataSource` / `Redis` is
  fully ready, and stops **before** they're torn down on graceful shutdown.

You don't need to do anything to use this — `ProcessingNode.start()` is called
when the context completes startup; `node.close()` is called as part of
graceful shutdown.

## Transactional enqueue (`enqueue-after-commit`)

Default is **`true`** — a job enqueued inside an open Spring transaction is
held until the transaction commits. A rollback discards the enqueue.

```java
@Transactional
public void scheduleWelcome(UserCreated created) {
    userRepo.save(created.toUser());         // pending write
    jobs.enqueue(SendEmailHandler.class, new SendEmail(...)); // also pending
    // Both happen — or neither: the job insert fires on afterCommit.
}
```

`TransactionAwareJobScheduler` is the bean wired by default. It builds the
`Job` (id reserved client-side via UUIDv7) synchronously and registers the
actual `store.insert(...)` via `TransactionSynchronizationManager`. If no
synchronisation is active, the insert is immediate — identical to the
non-Spring path.

Disable with:

```yaml
threadmill:
  spring:
    enqueue-after-commit: false
```

Two parts of the API stay immediate by design:

- `enqueueIfAbsent(...)` — must return a meaningful `EnqueueResult.Created` /
  `EnqueueResult.Coalesced` synchronously.
- `enqueueRecurring(...)` — cron-task definitions are configuration, not work.

See [`docs/transactions.md`](../docs/transactions.md) for the full
deep-dive (atomic boundaries per backend, handler-is-not-in-our-transaction,
at-least-once + idempotency, outbox pattern).

## Properties reference

Bound under `threadmill.*` (see `ThreadmillProperties` for the full
list). The most common:

| Property | Default | What |
|---|---|---|
| `threadmill.enabled` | `true` | Wire the `Scheduler` bean without starting a `ProcessingNode` when `false` (submitter-only mode). |
| `threadmill.workerCount` | `10` | Virtual-thread workers per default lane. |
| `threadmill.pollInterval` | `PT0.5S` | Fallback sleep before the next claim cycle. Idle workers, same-JVM producers, and scheduled promotion wake the dispatcher early, so this is an upper bound. |
| `threadmill.maintenancePollInterval` | `PT1S` | Master-only maintenance cadence for recurring tasks, scheduled promotion, and orphan reclaim. |
| `threadmill.retentionInterval` | `PT1H` | Master-only retention cadence for succeeded jobs, dedup keys, and stale node records. |
| `threadmill.defaultMaxAttempts` | `5` | Per-job retry budget (including first attempt). |
| `threadmill.jobTimeout` | `PT5M` | Per-job timeout. |
| `threadmill.spring.enqueue-after-commit` | `true` | Defer enqueue to `afterCommit` when a Spring transaction is active. |
| `threadmill.store.redis.mode` | `standalone` | `standalone` / `sentinel` / `cluster`. |
| `threadmill.store.redis.uri` | — | `redis://host:port` for standalone mode. |

## `@Job` annotation

Applied to a `JobHandler<P>` Spring bean. Required attributes:

- `queue` — queue name. Default `"default"`.
- `priority` — within-queue priority, higher wins. Default `0`.
- `timeout` — ISO-8601 duration. Falls back to `threadmill.jobTimeout`.
- `maxRetries` — falls back to `threadmill.defaultMaxAttempts`.

`ThreadmillJobRegistry` discovers every `@Job` bean at context
start. Two handlers for the same payload type fail startup with both names
in the error message.

## Out of scope for v1

- **Spring Boot Actuator integration** (`HealthIndicator`, `MeterBinder`,
  `/actuator/threadmill` endpoint) — Spring Boot 4.0's actuator surface is
  still being reorganised; revisit after SB4 GA.
- **Spring AOT / native-image `RuntimeHints`** — needs a stable actuator
  target.
- **Per-Spring-Boot-version sample apps** — `threadmill-example/spring-boot-3/`
  and `spring-boot-4/` are planned for a follow-up release.

## Build

```
./gradlew :threadmill-spring-boot:test
```

Tests cover `SpringBootIntegrationTest`, `ThreadmillAutoConfigurationTest`,
and `TransactionAwareJobSchedulerTest`.
