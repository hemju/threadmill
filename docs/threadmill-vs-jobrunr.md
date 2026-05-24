# Threadmill vs JobRunr

A focused comparison for teams evaluating Threadmill against JobRunr OSS or
JobRunr Pro. This page uses JobRunr's public documentation as the comparison
surface: the [introduction](https://www.jobrunr.io/en/documentation/), [storage
docs](https://www.jobrunr.io/en/documentation/installation/storage/), [Pro
overview](https://www.jobrunr.io/en/documentation/pro/), [Pro
feature-comparison page](https://www.jobrunr.io/en/pro/), [transaction
plugin](https://www.jobrunr.io/en/documentation/pro/transactions/), [instant
processing](https://www.jobrunr.io/en/documentation/pro/instant-job-processing/),
[batches](https://www.jobrunr.io/en/documentation/pro/batches/), [job
chaining](https://www.jobrunr.io/en/documentation/pro/job-chaining/), [rate
limiters](https://www.jobrunr.io/en/documentation/pro/rate-limiters/), [Pro
dashboard](https://www.jobrunr.io/en/documentation/pro/jobrunr-pro-dashboard/),
and [multi-cluster
dashboard](https://www.jobrunr.io/en/documentation/pro/jobrunr-pro-multi-dashboard/).

## At a glance

| Topic | JobRunr | Threadmill |
|---|---|---|
| Java baseline | Broad JVM support | Java 25 only; virtual threads and scoped values are baseline |
| Core model | Background jobs stored through a storage provider and executed by background servers | `JobStore` SPI plus `ProcessingNode`, `Dispatcher`, and explicit state machine |
| Delivery guarantee | At-least-once; handlers must tolerate retries | At-least-once; handlers must be idempotent |
| Storage | SQL and selected NoSQL providers in public docs; Redis providers are not in the current public storage list | PostgreSQL 18+, Redis standalone/Sentinel/Cluster, in-memory |
| Transactional enqueue | Pro transaction plugin integrates job creation with application transactions | Spring + Postgres `join_transaction`; default `after_commit`; `immediate` opt-in |
| Scheduling latency | Instant processing / real-time scheduling is a Pro feature | Local wake plus Postgres/Redis remote wake hints; polling remains fallback |
| Batches | Atomic batches are Pro-only | Atomic `insertAll`, but no batch orchestration API yet |
| Chains / workflows | Job chains are Pro-only | Built-in workflow relationship and interceptor; still intentionally smaller |
| Runtime rate limiting | Pro-only | Planned store-side token bucket, not implemented yet |
| Dashboard | Dashboard in OSS; advanced dashboard/search/SSO/multi-cluster are Pro features | Data-first dashboard API only; UI intentionally out of scope for this implementation |
| Observability | Built-in dashboard and Pro observability features | Micrometer module, optional OpenTelemetry tracing module, data-first snapshot API |

## Where Threadmill is intentionally different

Threadmill optimizes for a small, explicit engine that can be reasoned about
from the store contract. The job state machine is public in the model, every
store passes one shared contract suite, and Redis is treated as a first-class
durable backend with a non-destructive reliable-fetch path.

The tradeoff is scope. Threadmill does not try to match the full Pro feature
surface immediately. Dashboard UI, advanced search, SSO, multi-cluster
dashboard views, runtime rate limiting, and polished commercial operations
features require separate design work.

## Pro-only gaps to track

- **Atomic batches.** Threadmill has atomic bulk insert (`insertAll`) but not a
  user-facing batch lifecycle with success/failure aggregation.
- **Job chains and workflows.** Threadmill has parent/child workflow steps and
  failure cleanup. It does not yet expose a fluent chain builder or batch+chain
  composition API.
- **Runtime rate limiting.** Threadmill has per-key concurrency, queue lanes,
  and queue-family weights. It does not yet have a time-window or token-bucket
  rate limiter.
- **Advanced dashboard/search/SSO/multi-cluster dashboard.** Threadmill ships a
  data API and leaves the UI for a dedicated design discussion.
- **Built-in observability.** Threadmill now has Micrometer and optional
  OpenTelemetry, but the host application still owns exporters, dashboards,
  and alert routing.
- **Transaction plugin.** Threadmill's equivalent is narrower: Spring +
  Postgres only, using `threadmill.spring.enqueue-mode=join_transaction`.
- **Instant processing / real-time scheduling.** Threadmill uses local and
  remote wake hints plus polling fallback. This is durable and simple, but it
  is not a separate instant-processing product feature.

## Practical conclusion

Threadmill is a strong fit when you want a Java-25-native engine, explicit
Postgres/Redis correctness, Spring transaction clarity, and a compact codebase
that can be audited before production. JobRunr Pro remains ahead on packaged
commercial operations features, especially dashboard breadth, rate limiting,
batches, and transaction/plugin polish.

For Threadmill's roadmap, the highest-value follow-ups are: a real batch API,
a fluent workflow/chain builder, store-side rate limiting, dashboard UI design,
and richer search on stores that can support it honestly.
