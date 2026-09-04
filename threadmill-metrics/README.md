# threadmill-metrics

Micrometer integration for Threadmill. Register a `MeterRegistry`-aware
`ThreadmillMetrics` with the `ProcessingNode.Builder` to export per-state
counts, bounded per-queue gauges, processing lifecycle signals, claim health,
and rejected store writes.

> Threadmill provides at-least-once delivery. Metrics do not change that
> guarantee: a job may execute more than once, so handlers must be idempotent.

## Meters

| Meter | Type | Tags | What |
|---|---|---|---|
| `threadmill.jobs.count` | Gauge | `state` | Point-in-time count per state. Sourced from `JobStore.countsByState()` (cheap — incrementally maintained, never a full scan). |
| `threadmill.queue.depth` | Gauge | `queue` | Per-queue depth, ENQUEUED only. |
| `threadmill.queue.oldest.enqueued.age` | Gauge | `queue` | Age of the oldest ENQUEUED job in the queue, in milliseconds. |
| `threadmill.processing.oldest.heartbeat.age` | Gauge | — | Age of the oldest PROCESSING owner heartbeat, in milliseconds. Spiking means orphan-recovery is behind. |
| `threadmill.jobs.processed` | Counter | — | Successful completions. |
| `threadmill.jobs.failed` | Counter | `cause` | Failures by `JobInterceptor.FailureCause` (`EXCEPTION`, `TIMEOUT`, `ORPHAN_RECLAIM`, `QUARANTINE`, `SHUTDOWN`). |
| `threadmill.jobs.orphan.reclaimed` | Counter | — | Orphaned attempts whose FAILED transition committed through the engine's single failure path. |
| `threadmill.jobs.recurring.runs` | Counter | `origin` | Recurring instances by bounded origin (`schedule`, `nudge`, `manual`, `other`). |
| `threadmill.jobs.processing.time` | Timer | — | Handler runtime from claim to terminal transition. |
| `threadmill.claim.latency` | Timer | — | Time spent in every `claimReady` call, including empty polls and failed calls. |
| `threadmill.claim.failures` | Counter | — | `claimReady` calls that threw. Deliberately has no queue tag. |
| `threadmill.store.writes.rejected` | Counter | `operation` | Non-contractual store-write failures, tagged by a fixed operation-name set. Expected stale-version, oversize, invalid-argument, and duplicate-id rejections are excluded. Counts failed attempts, including retries, rather than distinct outages. |
| `threadmill.metrics.refresh.errors` | Counter | — | Gauge-refresh failures (store unreachable etc.). |
| `threadmill.metrics.queue.meter.errors` | Counter | — | Failures while registering or removing bounded per-queue meters. The store snapshot can still be current. |
| `threadmill.metrics.snapshot.stale` | Gauge | — | `1` after a failed refresh; clears to `0` after the next successful refresh. |
| `threadmill.metrics.snapshot.age` | Gauge | — | Milliseconds since the last successful store-derived snapshot (`-1` before any success). |
| `threadmill.metrics.queue.tags.omitted` | Gauge | — | Active queues omitted because the configured queue-tag cap was reached. |

## Wiring

```java
var registry = new SimpleMeterRegistry();
var backingStore = new PostgresJobStore(dataSource);
var metrics = new ThreadmillMetrics(registry, backingStore);
var store = metrics.meteredStore();

var node = ProcessingNode.builder(store)
    .interceptor(metrics.asInterceptor())
    .build();
var scheduler = new Scheduler(store, new JsonJobSerializer());
node.start();
```

Pass `metrics.meteredStore()` to every processing node and producer that uses
this metrics object. The decorator records claims and rejected writes at the
actual `JobStore` boundary; using the backing store directly bypasses those
meters. The interceptor records lifecycle signals after their persisted state
transition commits, including orphan reclaim.

Store-derived gauges use a pull-through snapshot. The first gauge read after
the one-second default interval performs one bounded refresh for all gauges,
independent of job completions. Concurrent gauge readers keep using the cached
snapshot instead of waiting behind an in-flight refresh, and the refresh
cooldown starts when the store reads finish. The reader that starts a refresh
does perform those synchronous reads, so choose the interval and queue cap as
store-load budgets as well as scrape-freshness/cardinality settings. There is
no background thread. `metrics.refresh()` waits for any in-flight pull and then
runs its own pass, so a host-requested refresh observes writes made after the
earlier pull began.

If a refresh fails, counts and queue depths retain the last successful values;
age gauges continue advancing from their last known timestamps. The stale
gauge becomes `1`, the error counter increments once per attempted refresh,
and the next successful pull replaces the snapshot and clears stale.

Per-queue meters keep stable slots for active queues and are capped at 100
distinct queue tags by default. Drained queues release their slots; newly
active queues fill available slots in lexical order. The omitted-queue gauge
shows when the cap hides active queues. Override both bounds when needed:

```java
var metrics = new ThreadmillMetrics(
    registry,
    backingStore,
    Duration.ofSeconds(5),
    250);
```

Set the last argument to `0` to disable per-queue meters. Claim meters carry
no queue tag, and rejected writes use only Threadmill's fixed operation names,
so those signals cannot introduce user-controlled tag cardinality. Unlike
gauges, accumulated counter/timer series cannot safely reuse a drained queue's
slot, so the bounded active-queue mechanism is intentionally not applied to
claim events. When queue meters are deliberately disabled, the omitted-queue
gauge remains zero.

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
- **`threadmill.claim.failures` rising or `threadmill.store.writes.rejected`
  rising** — the store is unavailable, saturated, or refusing capacity.
  Rejected writes count failed attempts, so retry loops can increase the rate
  several times during one logical outage.
- **`threadmill.metrics.snapshot.stale` = 1** — gauge values are last-known
  data. Use snapshot age to decide whether they are too old for an operational
  decision; refresh errors identify repeated failures.
- **`threadmill.metrics.queue.meter.errors` rising** — the store snapshot may
  still be current, but queue-meter registration/removal is failing. Inspect
  registry filters and duplicate meter ids.
- **`threadmill.metrics.queue.tags.omitted` > 0** — raise the configured cap or
  aggregate queues at the registry/exporter layer if per-queue visibility is
  required for all active queues.

## Build

```
./gradlew :threadmill-metrics:test
```
