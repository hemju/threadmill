# threadmill-dashboard

Data-first observability API. The mountable UI is additive — this module
ships the `EngineSnapshot` value type and the `EngineSnapshot.of(store)`
factory that builds one from any `JobStore`.

The concrete observability win is one data shape any consumer (Spring Boot
Actuator endpoint, custom internal admin UI, ops dashboard, monitoring script)
can render or scrape.

## `EngineSnapshot`

```java
public record EngineSnapshot(
    Instant takenAt,
    Map<JobState, Long> countsByState,
    Map<String, Long> queueDepths,
    Map<String, Instant> oldestEnqueuedAt,
    Instant oldestProcessingHeartbeat,
    List<NodeHeartbeat> nodeHeartbeats,
    List<CronTask> cronTasks,
    Set<String> pausedQueues,
    JobStoreCapabilities capabilities) { … }
```

Every field is sourced from a cheap store query — per-state counts come
from the incrementally-maintained counter (no full table scan); queue
depths come from per-queue cardinality (constant time on Postgres via
the partial index, O(1) on Redis via `ZCARD`); the rest are direct
look-ups.

## Use

```java
EngineSnapshot snapshot = EngineSnapshot.of(store);
// Render however you want — JSON, HTML, log, OpenTelemetry trace.
```

## Capability flags

`capabilities` carries the same `JobStoreCapabilities` the store advertises:
backend differences (`supportsRichSearch`, `supportsConcurrencyGroups`,
`maxSerializedJobBytes`, etc.) are visible to dashboards too. The UI can
gate features the backend can't honour.

## Build

```
./gradlew :threadmill-dashboard:test
```
