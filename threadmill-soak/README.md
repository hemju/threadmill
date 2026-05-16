# threadmill-soak

Two complementary suites:

1. The **soak regression suite** — fixed JUnit-style production checks for
   sustained throughput, recurring no-skip, and induced container-pause
   recovery. Tagged `soak`; excluded from `./gradlew check`. Task:
   `soakRegression`.
2. The **load soak harness** — operator-driven, per-backend, per-scenario
   sustained load runs that produce a self-contained artifact directory rich
   enough for an AI agent to read cold. Tasks: `soakMemory`, `soakPostgres`,
   `soakRedis`, `soakAll`.

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
| `-Pduration=<duration>` | `120s` | Wall-clock time the load generator runs. |
| `-PjobsPerSecond=<int>` | `100` | Target enqueue rate. Real backends typically take 2–4× this; tune via `-P`. |
| `-PworkerCount=<int>` | `8` | Workers per node. |
| `-Pnodes=<int>` | `1` | How many `ProcessingNode`s in the same JVM. |
| `-PoutputDir=<path>` | `build/soak/<runId>` | Where to write artifacts. |
| `-PrunId=<string>` | minted from scenario + backend + ISO timestamp | Explicit run id. |
| `-PfailFast=<bool>` | `true` | Reserved for future per-violation abort. |
| `-PpostgresUrl=<jdbc>` | unset → Testcontainers | External JDBC URL alternative. |
| `-PredisTopology=<topology>` | `standalone` | v1 supports `standalone` only. |
| `-Pforce=<bool>` | `false` | Allow overwriting an existing `-PoutputDir`. |

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

Each run writes nine files. The brief one-liner:

- `summary.json` — verdict + invariants + performance summary; **read this first.**
- `summary.md` — human-readable mirror of `summary.json`.
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
