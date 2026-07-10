# threadmill-spring-boot

Spring Boot auto-configuration for Threadmill. Wires the engine into a Spring
Boot application with sensible defaults, automatic store selection, lifecycle
management via `SmartLifecycle`, and transaction-aware enqueue semantics.

## Spring Boot compatibility

Requires **Spring Boot 4.0 or newer**. Earlier majors are **not supported** —
their bundled ASM cannot parse Java 25 class files (major version 69) and
crash during `@ComponentScan`. The auto-configuration fails fast with a
clear message if it detects Spring Boot 3.x or earlier on the classpath.

## Startup banner

On engine start, `ThreadmillLifecycle` logs a single multi-line banner with
the node id, store identity (e.g. `PostgreSQL 18.1 @ threadmill`,
`Redis standalone host=localhost port=6379`, or
`In-Memory (volatile, single-JVM)`), capability flags, lane / worker
breakdown, and the polling / maintenance cadences. It's the operator's
single place to confirm "which engine, which store, which lanes" at boot time.

## Quick start

```kotlin
dependencies {
    implementation("com.hemju.threadmill:threadmill-spring-boot:VERSION")
    // Optional, pick one or none — auto-detected:
    implementation("com.hemju.threadmill:threadmill-store-postgres:VERSION")
    // implementation("com.hemju.threadmill:threadmill-store-redis:VERSION") // pulled in transitively
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
   wired from the application's existing `DataSource`. The auto-configured
   store applies pending Threadmill schema migrations by default before it is
   created.
3. **Explicit development opt-in** (`threadmill.store.memory.enabled=true`) →
   `InMemoryJobStore` with a loud warning that jobs won't survive restart.
4. **Otherwise** → startup fails with an actionable error instead of silently
   selecting volatile storage.

Define your own `JobStore` bean to override everything.

## Lifecycle (`SmartLifecycle`)

`ThreadmillLifecycle` wraps `ProcessingNode` and registers at Spring's default
maximum phase (`Integer.MAX_VALUE`). That's deliberate:

- Spring starts lower phases first and stops higher phases first.
- The maximum phase therefore starts Threadmill as late as possible and stops
  it as early as possible.
- Store beans and their connections are constructed before lifecycle startup.
- Remote-wake subscriptions share the engine lifecycle: they subscribe after
  the node starts and close before the node drains.

You don't need to do anything to use this — `ProcessingNode.start()` is called
when the context completes startup; `node.close()` is called as part of
graceful shutdown.

## Transactional enqueue (`enqueue-mode`)

Default is **`after_commit`** — a job enqueued inside an open Spring
transaction is held until the transaction commits. A rollback discards the
enqueue.

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

Use immediate writes with:

```yaml
threadmill:
  spring:
    enqueue-mode: immediate
```

Use caller-transaction participation with Spring + Postgres:

```yaml
threadmill:
  spring:
    enqueue-mode: join_transaction
```

`join_transaction` wires `PostgresJobStore` with a Spring transaction-bound
connection strategy. Job inserts and dedup rows commit or roll back with the
application transaction; local worker wakeups still fire only after commit.
Redis and custom stores cannot join a SQL transaction and fail fast if this
mode is requested.

`enqueueRecurring(...)` stays immediate because cron-task definitions are
configuration, not work.

See [`docs/transactions.md`](../docs/transactions.md) for the full
deep-dive (atomic boundaries per backend, handler-is-not-in-our-transaction,
at-least-once + idempotency, outbox pattern).

## Remote wake

Durable auto-configured stores publish remote wake hints by default:

- Postgres: `LISTEN`/`NOTIFY` on `threadmill_wake`.
- Redis: Pub/Sub on `{threadmill}:wake`.

The listener calls `ProcessingNode.wake(queue)` on matching local dispatchers.
This is a latency optimization only; polling remains the fallback when a
notification is missed or the pub/sub channel is temporarily unavailable.
Set `threadmill.remote-wake.channel` to isolate multiple deployments that share
one Postgres database or Redis instance.

Postgres `LISTEN` holds one JDBC connection while remote wake is enabled. If
the listener uses the same application pool, reserve one extra pool slot for
it. For saturated pools, expose a custom `PostgresRemoteWakeChannel` bean with
a dedicated one-connection listener `DataSource`.

Disable it with:

```yaml
threadmill:
  remote-wake:
    enabled: false
```

Custom stores can participate by exposing a `RemoteWakeChannel` bean.

## Tracing

User-provided `JobInterceptor` beans are added to the auto-configured
`ProcessingNode`, so the tracing module can be wired without replacing the
node:

```java
@Bean
ThreadmillTracing threadmillTracing(OpenTelemetry openTelemetry) {
    return ThreadmillTracing.of(openTelemetry);
}

@Bean
JobInterceptor threadmillTracingInterceptor(ThreadmillTracing tracing) {
    return tracing.asInterceptor();
}
```

Applications that create their own `JobStore` bean can also wrap it with
`tracing.wrapStore(store)` to emit store-operation spans.

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
| `threadmill.remote-wake.enabled` | `true` | Publish cross-node wake hints for auto-configured Postgres / Redis stores. |
| `threadmill.remote-wake.channel` | backend default | Optional channel override for deployment isolation. |
| `threadmill.spring.enqueue-mode` | `after_commit` | `after_commit`, `join_transaction`, or `immediate`. |
| `threadmill.store.memory.enabled` | `false` | Explicitly allow volatile in-memory storage for development or tests. Without a durable store or this opt-in, startup fails. |
| `threadmill.store.postgres.schema-mode` | `migrate` | `migrate`, `validate`, `none`, or `drop-and-migrate`. |
| `threadmill.store.postgres.allow-destructive-schema-reset` | `false` | Required for `drop-and-migrate`; destroys stored Threadmill jobs. |
| `threadmill.store.redis.mode` | `standalone` | `standalone` / `sentinel` / `cluster`. |
| `threadmill.store.redis.uri` | — | `redis://host:port` for standalone mode. |
| `threadmill.store.redis.reset-on-start` | `false` | Delete Threadmill Redis keys before startup; development only. |
| `threadmill.store.redis.allow-destructive-reset` | `false` | Required for `reset-on-start`; destroys stored Threadmill jobs. |

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
- **Spring Boot 4 sample app** — `threadmill-example/spring-boot-4/` is
  planned for a follow-up release.

## Build

```
./gradlew :threadmill-spring-boot:test
```

Tests cover `SpringBootIntegrationTest`, `ThreadmillAutoConfigurationTest`,
and `TransactionAwareJobSchedulerTest`.
