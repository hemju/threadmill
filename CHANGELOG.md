# Changelog

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
- Added Spring ergonomic API: `@ThreadmillJob`, payload-type handler discovery,
  and `JobEnqueuer`.
- Added Redis standalone, Sentinel, and Cluster configuration. Cluster uses a
  single `{threadmill}` hash slot for v1 Lua correctness.
- Added producer-side deduplication with `Created` / `Coalesced` results.
- Added long-running job check-ins, progress updates, bounded logs, and
  no-progress timeout handling.
- Removed the experimental alternate framework module and public positioning
  for now. The core remains framework-agnostic so additional integrations can
  be added later.
