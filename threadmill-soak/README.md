# threadmill-soak

Three complementary pieces:

1. The **soak regression suite** — fixed JUnit-style production checks for
   sustained throughput, recurring no-skip, and induced container-pause
   recovery. Tagged `soak`; excluded from `./gradlew check`. Task:
   `soakRegression`.
2. The **load soak harness** — operator-driven, per-backend, per-scenario
   sustained load runs that produce a self-contained artifact directory rich
   enough for an AI agent to read cold. Tasks: `soakMemory`, `soakPostgres`,
   `soakRedis`, `soakAll`.
3. The **endurance run** — the production-readiness sign-off: one harness
   JVM per backend (PostgreSQL **and** Redis in parallel) for hours, with
   node churn, live invariant verification, and a collated verdict. Task:
   `soakEndurance`. See [Endurance run](#endurance-run-production-sign-off).

---

## Soak Regression Suite

### What it tests

- **Sustained throughput.** Drives thousands of jobs end-to-end and
  asserts they all reach a terminal state.
- **Recurring no-skip.** Defines a tight-interval recurring task and
  verifies it fires every interval over a multi-second window without
  catch-up storms.
- **Induced container-pause recovery.** Pauses the Postgres or Redis
  Testcontainer mid-run via the Docker API, asserts no progress while
  paused, unpauses, and asserts the engine resumes — the store-outage
  circuit-breaker recovery path in production.

### Run

```
./gradlew :threadmill-soak:soakRegression
```

Requires Docker (Testcontainers) for the Postgres and Redis runs.
Numbers are reported in the test output:

| Backend | Throughput (5k–10k jobs) | Recurring (5 s, 100 ms interval) | Container-pause recovery |
|---|---|---|---|
| In-memory | ~950 jobs/sec | ~46 runs | n/a |
| PostgreSQL 18 | ~270 jobs/sec | ~20 runs | pause → wait → unpause → drain |
| Redis 7 (AOF) | ~250 jobs/sec | ~22 runs | pause → wait → unpause → drain |

Numbers are from a developer laptop (Apple Silicon, Docker Desktop, single
node, 8 workers, 32-batch claim). Treat them as a baseline; production
hardware and tuning will move them substantially.

---

## Load Soak Harness

The harness lets an operator run **one scenario against one backend** with
tunable duration and rate, and produces a self-contained artifact directory
under `build/soak/<scenario>-<backend>-<timestamp>/`. The directory contains
the full lifecycle trace, periodic metrics samples, per-job latencies, the
invariant check results, and machine- and human-readable summaries.

The harness is distinct from:
- **`:soakRegression` (above)** — fixed throughput / recovery tests, all
  backends in one JUnit run.
- **`threadmill-simulation`** — fixed correctness scenarios and
  worker-process churn simulations.

### Per-backend tasks

```bash
./gradlew :threadmill-soak:soakMemory [-P…]
./gradlew :threadmill-soak:soakPostgres [-P…]
./gradlew :threadmill-soak:soakRedis [-P…]
./gradlew :threadmill-soak:soakAll [-P…]   # runs Postgres then Redis
```

### `-P` properties

| Property | Default | Meaning |
|---|---|---|
| `-Pscenario=<name>` | `mixed-workload` | Which scenario to run. |
| `-Pduration=<duration>` | `120s` | Wall-clock time the load generator runs (`90ms` / `30s` / `5m` / `8h`). |
| `-PjobsPerSecond=<int>` | `100` | Target enqueue rate. Real backends typically take 2–4× this; tune via `-P`. |
| `-PworkerCount=<int>` | `8` | Workers per node. |
| `-Pnodes=<int>` | `1` | How many `ProcessingNode`s in the same JVM. |
| `-PnodeChurn=<duration>` | off | Close-and-replace one node every interval (requires `-Pnodes=2`+). |
| `-PoutputDir=<path>` | `build/soak/<runId>` | Where to write artifacts. |
| `-PrunId=<string>` | minted from scenario + backend + ISO timestamp | Explicit run id. |
| `-PfailFast=<bool>` | `true` | Abort the run on the first *definite* invariant violation (see below). |
| `-PprogressInterval=<duration>` | `30s` | How often `progress.json` is rewritten. |
| `-PpostgresUrl=<jdbc>` | unset → Testcontainers | External JDBC URL alternative. |
| `-PredisUrl=<redis://…>` | unset → Testcontainers | External Redis alternative. Only the `{threadmill}:*` namespace is reset — never `FLUSHDB`. |
| `-PredisTopology=<topology>` | `standalone` | v1 supports `standalone` only. |
| `-Pforce=<bool>` | `false` | Allow overwriting an existing `-PoutputDir`. |

### Live verification, `progress.json`, and fail-fast

Invariants are verified **live**: every trace event feeds the scenario's
streaming checks as it is written, with state bounded by in-flight work — the
same definitions verify a five-second smoke and an eight-hour endurance run.
Two violation kinds exist:

- **Definite** violations are provable the moment the offending event arrives
  (an EXCLUSIVE lock overlapping, a claim on a paused queue, an over-budget
  retry). With `-PfailFast=true` the first one aborts the run — the producer
  stops, the engine drains, and every artifact is still written, with a note
  in `summary.json`.
- **Completeness** violations only exist at end of run (a job that never
  reached a terminal event, a lock never released); they appear in the final
  results, never mid-run.

`progress.json` is atomically rewritten every `-PprogressInterval` so a
still-running soak can be inspected from outside the process: phase
(`running` / `draining` / `finished`), counts, store states, queue depths,
live p99, and per-invariant status. `summary.json` supersedes it once the
run ends. `TraceReplay` (in `…soak.harness.invariant`) re-verifies a
finished `trace.jsonl` offline, streaming, without loading it into memory.

The Postgres harness uses a fixed connection pool (`maxConnections=80`)
because the soak loop intentionally creates enough claim, completion, heartbeat,
and metric traffic to make one-connection-per-operation fixtures distort the
result.

### Scenario library

| Scenario | Description |
|---|---|
| `mixed-workload` (default) | 20 resources × EXCLUSIVE imports + SHARED exports over a queue-family lane. |
| `rw-lock-stress` | Single concurrency key, 95% SHARED + 5% EXCLUSIVE — strict in-group order check. |
| `weighted-queues` | Three queues with 10:3:1 weighting under one queue-family lane. |
| `retry-storm` | 5% baseline failures + 1% wall-clock timeouts — retry-budget bounds. |
| `long-running` | Long handlers with periodic check-ins; some stalled — no-progress-timeout kill check. |
| `pause-resume` | 10 project queues; randomly-chosen queue paused mid-run for 5 s. |
| `bulk-enqueue` | Producer uses `enqueueAll` exclusively in 50-job batches. |
| `crash-recover` | Closes one `ProcessingNode` mid-run; orphan reclaim picks up its work. |

### Output directory layout

Each run writes ten files. The brief one-liner:

- `summary.json` — verdict + invariants + performance summary; **read this first.**
- `summary.md` — human-readable mirror of `summary.json`.
- `progress.json` — live status while the run is underway (phase, counts, invariant snapshot).
- `trace.jsonl` — every lifecycle event, JSON-lines.
- `lock-events.jsonl` — derived from trace; one row per acquire/release pair with hold duration.
- `metrics.jsonl` — once-per-second snapshots of queue depths, per-state counts, in-flight.
- `latencies.jsonl` — per-job timings for the four lifecycle stages.
- `invariants.json` — invariant check results (also embedded in `summary.json`).
- `config.json` — the run's effective configuration.
- `stdout.log` / `stderr.log` — engine logs captured by `JavaExec`.

### `summary.json` shape

The schema is checked into the module at
`src/main/resources/com/hemju/threadmill/soak/harness/summary.schema.json`
(JSON Schema draft 2020-12). The harness validates every generated summary
against this schema before writing it.

Required top-level fields: `runId`, `scenario`, `backend`, `config`,
`verdict` (`passed` | `failed`), `invariants` (list of name + pass/fail +
violations + sample event chains), `performance` (totals, throughput
breakdown by queue and handler, p50/p95/p99/max latency for each lifecycle
stage, per-key lock contention stats).

### AI-drop-in workflow

A typical session:

```bash
# Suspect concurrency lock perf on Redis?
./gradlew :threadmill-soak:soakRedis -Pscenario=rw-lock-stress -Pduration=180s

# Drop the directory into a fresh AI conversation:
#   "Analyse build/soak/rw-lock-stress-redis-<timestamp>/. Verdict? Lock-wait
#    percentiles? Anything anomalous in the trace?"

# Compare to Postgres:
./gradlew :threadmill-soak:soakPostgres -Pscenario=rw-lock-stress -Pduration=180s
```

Because the directory is self-contained, the AI agent gets the same view as
a human reading the files by hand. `summary.md` is the table-of-contents;
`trace.jsonl` is the source of truth for any "what really happened" question.

### Picking a scenario

| Symptom | Scenario to reach for |
|---|---|
| Queue-depth spike with no obvious cause | `mixed-workload` |
| Lock-wait p99 looks high under contention | `rw-lock-stress` |
| One project queue dominating despite equal weighting | `weighted-queues` |
| Retries / failures blowing up | `retry-storm` |
| Long-running jobs being killed prematurely | `long-running` |
| Queue pause didn't take effect | `pause-resume` |
| Bulk-enqueue path slower than per-job | `bulk-enqueue` |
| Node crash didn't trigger orphan reclaim | `crash-recover` |

---

## Endurance run (production sign-off)

`soakEndurance` is the "run it for hours against both backends before I put
this into production" shape. It launches **one harness JVM per backend in
parallel** — each child is the unmodified per-backend harness, so every
artifact contract above holds verbatim per backend — and supervises both.

### Provision long-lived datastores

For multi-hour runs prefer external instances over Testcontainers: they
outlive the harness process, survive a restart, and stay inspectable after a
failed run. The module ships a compose file on deliberately offset ports
(54320 / 63790) so it never collides with everyday local instances:

```bash
docker compose -f threadmill-soak/docker-compose.endurance.yml up -d
```

### Run

```bash
./gradlew :threadmill-soak:soakEndurance \
  "-PpostgresUrl=jdbc:postgresql://localhost:54320/threadmill?user=threadmill&password=threadmill" \
  -PredisUrl=redis://localhost:63790
```

Without the URL knobs each child provisions its own Testcontainer — fine for
short validation runs, not recommended for hours.

Defaults are the sign-off profile: **8 hours**, `mixed-workload`,
**50 jobs/second**, **3 nodes per backend**, **node churn every 10 minutes**
(`-PnodeChurn=off` to disable), `failFast=true`. All harness `-P` knobs apply;
`-Pbackends=memory,memory` exists for cheap orchestrator exercise.

A short pre-flight before committing a night to it:

```bash
./gradlew :threadmill-soak:soakEndurance -Pduration=90s -Pnodes=2 -PnodeChurn=30s \
  "-PpostgresUrl=…" -PredisUrl=…
```

### What it produces

```
build/soak/endurance-<timestamp>/
├── endurance-summary.json   # combined verdict + per-backend comparison — read this first
├── endurance-summary.md     # human-readable mirror
├── endurance-config.json    # effective orchestrator configuration
├── postgres-console.log     # child JVM console
├── redis-console.log
├── postgres/                # full per-backend artifact directory (ten files, see above)
└── redis/
```

While the run is live the orchestrator prints one status line per backend per
minute, built from the children's `progress.json` — elapsed/target minutes,
counts, in-flight, live p99, and the running invariant-violation total. The
combined verdict is `passed` only when every child exits 0 **and** reports a
`passed` verdict; a child that dies without writing a summary fails the run.
A fail-fast abort in one backend never stops the other — its result is still
worth having.

### What it exercises

- **Correctness under parallelism** — the scenario's streaming invariants
  (at-least-once, EXCLUSIVE/SHARED exclusion, in-group order, lock pairing,
  retry budget) verified live over millions of events per backend.
- **Multiple nodes** — N nodes per backend contend through the shared store;
  node churn closes and replaces one node per cycle, repeatedly exercising
  maintenance-lease handover, node-registry cleanup, interrupted handlers
  retried on survivors, and queue-family rediscovery. Hard process kills are
  covered separately by `threadmill-simulation`'s worker-churn runs.
- **Performance** — throughput and latency percentiles per backend. Both
  stacks share one machine: the figures are comparable *to each other under
  equal contention*, not absolute capacity measurements.

### Checking the result

`endurance-summary.md` answers "did both backends pass, and how did they
compare?". For anything deeper, drop the whole run directory into an AI
conversation — every question reduces to the per-backend artifacts:

```
"Analyse build/soak/endurance-<timestamp>/. Both verdicts? Any invariant
 violations? Compare lock-wait p99 between postgres/ and redis/. Did the
 node churn cycles cause retry spikes (trace.jsonl: node_churn_stop)?"
```

---

## Why neither suite runs as part of `check`

`./gradlew check` runs in seconds. Both the soak regression suite and the
harness's end-to-end scenario tests run in minutes and need a container
runtime.
Keeping them separate lets developers iterate quickly while preserving
one-command "is the engine healthy under load?" gates for release prep:

```
./gradlew productionCheck
```

runs `check`, `soakRegression`, Javadoc, and the artifact inspection together.

The harness's *output-contract* tests — invariant-violation, summary-schema,
and a fast in-memory smoke — are untagged and run in every `check`. They
guarantee a regression that breaks the harness's output shape can't sneak
in without CI noticing.
