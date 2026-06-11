# threadmill-metrics

Micrometer integration for Threadmill. Register a `MeterRegistry`-aware
`ThreadmillMetrics` with the `ProcessingNode.Builder` to export per-state
counts, queue depths, processed/failed counters, and timers.

## Meters

| Meter | Type | Tags | What |
|---|---|---|---|
| `threadmill.jobs.count` | Gauge | `state` | Point-in-time count per state. Sourced from `JobStore.countsByState()` (cheap — incrementally maintained, never a full scan). |
| `threadmill.queue.depth` | Gauge | `queue` | Per-queue depth, ENQUEUED only. |
| `threadmill.processing.oldest.heartbeat.age` | Gauge | — | Age of the oldest PROCESSING owner heartbeat, in milliseconds. Spiking means orphan-recovery is behind. |
| `threadmill.jobs.processed` | Counter | — | Successful completions. |
| `threadmill.jobs.failed` | Counter | `cause` | Failures by `JobInterceptor.FailureCause` (`EXCEPTION`, `TIMEOUT`, `ORPHAN_RECLAIM`, `QUARANTINE`). |
| `threadmill.jobs.processing.time` | Timer | — | Handler runtime from claim to terminal transition. |
| `threadmill.claim.latency` | Timer | — | Time spent in `claimReady`. |
| `threadmill.metrics.refresh.errors` | Counter | — | Gauge-refresh failures (store unreachable etc.). |

## Wiring

```java
var registry = new SimpleMeterRegistry();
var metrics = new ThreadmillMetrics(registry, store);

var node = ProcessingNode.builder(store)
    .interceptor(metrics.asInterceptor())
    .build();
node.start();
```

The interceptor feeds the counters / timers on every lifecycle event. The
gauges are updated from the store on job completion (coalesced behind a 1s TTL) and the per-queue oldest-enqueued-age gauge is read live on scrape.

Under Spring Boot, the `MeterBinder` integration is held until SB4 GA — see
`threadmill-spring-boot/README.md`. Until then, hosts wire `ThreadmillMetrics`
directly against their `MeterRegistry` bean.

## What to alert on

- **`threadmill.jobs.count{state="QUARANTINED"}` ≥ 1** — poison job that
  couldn't be deserialized / instantiated. Inspect and either fix the
  payload class or hard-delete the row.
- **`threadmill.jobs.count{state="FAILED"}` rising** — the failure rate is
  outpacing the retry budget. Look at the `cause` label on
  `threadmill.jobs.failed` for the breakdown.
- **`threadmill.processing.oldest.heartbeat.age` > 5 × `heartbeatTimeout`** —
  orphan recovery isn't keeping up; the master node may be unhealthy.
- **`threadmill.metrics.refresh.errors` > 0** — Micrometer gauge refresh is
  failing. Usually a transient store outage; the dispatcher's circuit
  breaker will pause processing too.

## Build

```
./gradlew :threadmill-metrics:test
```
