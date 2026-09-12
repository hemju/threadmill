# Threadmill 1.0 soak qualification plan

These are **planned runs**, not completed endurance evidence. Run PostgreSQL
and Redis separately so resource contention between backends cannot disguise
a regression. Complete `productionCheck` first, then freeze the candidate,
configuration, and container image digests for the entire qualification.

Threadmill delivers **at least once**. Assert delivery and exclusive execution
invariants; do not demand exactly one handler invocation after a crash.
Application side effects must remain idempotent.

## Common preparation and measurements

Use dedicated disposable databases. Each harness invocation resets Threadmill
data, including when an external URL is supplied. Preserve the previous run's
datastore snapshot and artifacts before starting another invocation. The
PostgreSQL fixture resets Threadmill tables; Redis resets `{threadmill}:*`.

Allocate a fixed host with at least 4 CPUs, 8 GiB RAM, and 50 GiB free SSD space
for each backend experiment. Give the datastore and harness separate resource
budgets, record them, and keep them unchanged between baseline and candidate.
Start at 50 jobs/s, one producer, three processing nodes, and eight workers per
node. This is an initial qualification load, not a published capacity claim.
Measure a 30-minute baseline first; reduce the rate if that host cannot drain
it. Do not change the rate halfway through a comparison.

Capture the candidate revision **and working-tree patch**, JDK/Gradle versions,
OS/CPU/RAM/storage, resolved image digests, datastore settings, schema/index
versions, scenario, and all effective configuration. Use unique output paths;
do not use `-Pforce=true` for sign-off evidence. URLs in `config.json` can
contain credentials: use disposable credentials and redact them before sharing.

The harness writes live `progress.json`, `trace.jsonl`, `latencies.jsonl`,
`metrics.jsonl`, invariant results, and final JSON/Markdown summaries. Its
one-second sampler now includes:

- State counts, queue depths, ages for at most 32 queues, and state ages.
- A bounded dashboard-shaped state-page read and total monitoring duration.
- Cumulative enqueue/claim/terminal-write/retention operation counts and failures,
  with p50/p95/p99 microseconds over the most recent 4,096 calls per operation.
- Actual job and concurrency-group deletion counts and JVM heap usage.

The lock summary keeps at most 127 named keys and one `(additional keys)`
aggregate; all original per-key events remain in `lock-events.jsonl`. Final
full-run percentile calculation retains primitive duration samples and therefore
uses memory proportional to completed attempts, even though live verification
and the summary key maps are bounded. Include report generation in the resource
measurements and wait for the final summary before calling a run complete.

Operation percentiles include failed calls; use the failure counters and fault
timeline when interpreting them. These are recent windows, not full-run
percentiles. Lifecycle latencies are separately recorded per completed job.
State ages include legitimately retained final failures and protected workflow
parents; they are eligibility signals, not proof of a stuck maintenance worker.
Monitor sampling freshness externally: a stopped sampler must not look healthy
because its last successful values remain on disk.

Harness producers recover transport failures for at most two minutes, retaining
the original job IDs and checking durable records before retrying uncertain
insert, bulk-insert, and deduplication acknowledgements. Worker calls still use
the original store. `producer_outage` and `producer_recovered` trace events mark
these intervals; invalid requests and partially visible ambiguous batches fail
the run. This is harness behavior, not an automatic retry guarantee of the
public `Scheduler`. Real Redis regressions pause the server longer than its
command timeout and require both mixed and retention producers to resume.
Redis `LOADING` responses during restart also use the same bounded producer
recovery and acknowledgement reconciliation; unrelated command errors do not.
PostgreSQL recovery includes connection failures and restart states `57P01`,
`57P02`, and `57P03`, including new connections refused during shutdown or
startup. A real PostgreSQL regression holds the server in smart shutdown,
verifies `57P03`, then restarts it and requires the original job to be inserted
exactly once. Other SQL errors still fail the producer immediately.
The PostgreSQL fixture does not set a JDBC socket timeout: a paused server can
leave a producer blocked until it resumes, without throwing a transport error.
That experiment measures blocked-call recovery; the PostgreSQL restart
experiment exercises connection failure and reconnection. Trace events alone
must not be used to infer that the paused PostgreSQL producer failed to recover.

Every minute also record datastore CPU/RSS, connection counts, disk/AOF/WAL
growth, and metadata cardinality. During faults capture every second. Retain
raw measurements rather than only graphs or a final jobs/s number. For Redis,
measure independent-client `PING` latency during the high-cardinality run to
detect Lua monopolizing the server. Record enqueue, claim, and terminal-write
tails while monitoring is active.

`retention-churn` uses a repeatable sequence recipe rather than random choices:
8 KiB payloads across eight queue-family queues, a fresh concurrency key for
each root, workflows at every tenth sequence position, duplicate producer
requests at applicable multiples of four, and first-attempt failures at
multiples of seven. Retention is ten seconds, deduplication TTL is five seconds,
and maintenance polls every 100 ms. It accepts one producer. Its purpose is to
cycle through many retention windows and lifetime-distinct keys. The existing
`mixed-workload` uses randomness without an exposed seed; retain its full trace
for replay and identify the scenario implementation with the candidate patch.
Do not invent a `-Pseed` flag or claim deterministic thread scheduling.

## PostgreSQL 18 experiment

Use a dedicated PostgreSQL 18 instance with durable settings (`fsync=on`,
`synchronous_commit=on`, `full_page_writes=on`) and normal autovacuum enabled.
The local convenience service is:

```sh
docker compose -f threadmill-soak/docker-compose.endurance.yml up -d postgres
```

Run the 30-minute baseline with the command below, changing duration to `30m`,
omitting node churn, and choosing a distinct run ID/output directory. Archive
it, then run these two phases sequentially:

```sh
./gradlew :threadmill-soak:soakPostgres \
  -Pscenario=mixed-workload -Pduration=12h -PjobsPerSecond=50 \
  -Pproducers=1 -Pnodes=3 -PworkerCount=8 -PnodeChurn=10m \
  -PprogressInterval=30s -PfailFast=true \
  '-PpostgresUrl=jdbc:postgresql://localhost:54320/threadmill?user=threadmill&password=threadmill' \
  -PrunId=pg18-mixed-candidate -PoutputDir=.local-reference/qualification/pg18-mixed-candidate

./gradlew :threadmill-soak:soakPostgres \
  -Pscenario=retention-churn -Pduration=12h -PjobsPerSecond=50 \
  -Pproducers=1 -Pnodes=3 -PworkerCount=8 -PnodeChurn=10m \
  -PprogressInterval=30s -PfailFast=true \
  '-PpostgresUrl=jdbc:postgresql://localhost:54320/threadmill?user=threadmill&password=threadmill' \
  -PrunId=pg18-retention-candidate -PoutputDir=.local-reference/qualification/pg18-retention-candidate
```

The first phase deliberately accumulates history; the second must reach a
steady retained population. Add 30-minute `retry-storm`, `long-running`, and
`nudge-pump` runs using the same task and separate paths. Do not run a second
Gradle build that replaces the active harness's classpath during these runs.

At hours 2, 6, and 10 of each 12-hour phase, pause the dedicated PostgreSQL
container for 20 seconds, then unpause it. Use the exact container ID obtained
from `docker compose ... ps -q postgres`; never target a shared database.
Observe circuit-breaker recovery, retained finalizers, maintenance leadership,
and complete subsequent drain. Run one normal database restart after hour 8
with volumes retained. Log fault start/end times and recovery times. The
configured node churn closes/replaces an in-process node; the separate
process-crash simulations in `productionCheck` cover abrupt worker death.

Collect `pg_stat_activity`, lock waits, `pg_stat_database`,
`pg_stat_user_tables` live/dead tuple estimates, autovacuum times, WAL bytes,
and `pg_total_relation_size` for jobs, indexes, concurrency groups/holds,
deduplication keys, and queue counters. Count group/hold/dedup rows once per
minute. Validate sharded state/queue counters against raw job aggregates after
the final drain. During retention, a stable logical population with unchecked
dead-tuple or index growth still needs investigation.

Repeat the existing `benchmarkPostgresMonitoring` comparison at 10k/100k/1m
pending jobs on the candidate hardware. Keep warm/cold results separate; see
[the audit benchmark evidence](audit-1.0-performance.md). Its short measurements
are a query-cost baseline, not a substitute for these endurance phases.

## Redis experiments

Run the same baseline and two 12-hour phases first on standalone Redis 7.4,
then repeat qualification on the newer Redis line intended for support (the
bounded topology suite currently also tests 8.6). Pin resolved image digests.
Enable AOF, record the fsync policy, require `maxmemory-policy noeviction`, and
set an explicit memory limit with headroom. Capture AOF rewrite behavior and
RSS fragmentation as well as logical key counts.

```sh
docker compose -f threadmill-soak/docker-compose.endurance.yml up -d redis

./gradlew :threadmill-soak:soakRedis \
  -Pscenario=mixed-workload -Pduration=12h -PjobsPerSecond=50 \
  -Pproducers=1 -Pnodes=3 -PworkerCount=8 -PnodeChurn=10m \
  -PprogressInterval=30s -PfailFast=true -PredisTopology=standalone \
  -PredisUrl=redis://localhost:63790 \
  -PrunId=redis74-mixed-candidate -PoutputDir=.local-reference/qualification/redis74-mixed-candidate

./gradlew :threadmill-soak:soakRedis \
  -Pscenario=retention-churn -Pduration=12h -PjobsPerSecond=50 \
  -Pproducers=1 -Pnodes=3 -PworkerCount=8 -PnodeChurn=10m \
  -PprogressInterval=30s -PfailFast=true -PredisTopology=standalone \
  -PredisUrl=redis://localhost:63790 \
  -PrunId=redis74-retention-candidate -PoutputDir=.local-reference/qualification/redis74-retention-candidate
```

Use the same 20-second pause schedule on the dedicated standalone Redis
container. This checks outage recovery without deliberately destroying recent
acknowledged writes. Include the separate retry/check-in/nudge phases described
for PostgreSQL. Monitor `INFO memory`, `INFO persistence`, `INFO stats`,
`INFO commandstats`, `SLOWLOG`, and independent-client latency. Record
`ZCARD {threadmill}:concurrency_counters` and
`ZCARD {threadmill}:dedup_expiry`; sample actual namespace key categories with
cursor-based `SCAN` outside the hot path. After drain, check state/index counts,
pending members, workflow holds, and zero evictions.

For topology sign-off, provision three Sentinel processes with a primary and
replica on separate failure domains, and a Cluster with three primaries plus
three replicas. Run an additional eight-hour retention phase per topology with
the same workload and independent artifacts. The harness accepts external
topologies using these replacement arguments:

```sh
-PredisTopology=sentinel '-PredisUrl=redis-sentinel://sentinel1:26379,sentinel2:26379,sentinel3:26379/0#soak-primary'
-PredisTopology=cluster '-PredisUrl=redis://redis1:6379,redis://redis2:6379,redis://redis3:6379'
```

All advertised data-node addresses must be reachable from the harness. Cluster
seed URIs must share credentials/TLS settings and database zero. `rediss://`
Cluster seeds use full certificate/hostname verification. For Sentinel's URI
and authentication options use the [Lettuce connection reference](https://redis.github.io/lettuce/user-guide/connecting-redis/).
Run the authenticated TLS/certificate rejection gate against the candidate
configuration as well; local development endpoints are not certificate evidence.

At hours 2 and 6, perform a controlled primary handover. At hour 4, migrate the
`{threadmill}` slot in the Cluster experiment while load continues; in the
Sentinel experiment, pause the current primary for 20 seconds and observe
discovery and recovery. At hour 7, perform a separate abrupt
primary-failure experiment and record acknowledged-write loss and recovery time.
Keep its verdict distinct from no-loss runs: Redis asynchronous replication
can lose acknowledged writes, so such a failure is not automatically a
Threadmill state-machine defect. It is still a failed zero-loss qualification
and must be reported, never hidden by weakening the invariant checker. Compare
the observed recovery point with the application's requirement. The bounded
`RedisFailoverTest` separately proves recovery of pre-replicated queued/in-flight
work; it uses a fixture-only barrier and does not change Threadmill durability.
See [the topology and durability contract](redis-topologies.md).

## Exit criteria and retained evidence

Agree on baseline-relative performance thresholds before the candidate run.
The starting acceptance envelope is:

- Zero definite invariant violations: eventual delivery of retained accepted
  work, no exclusive execution overlap, no leaked execution brackets/holds,
  no abandoned retryable workflows, and no stuck processing after recovery.
- Every non-fault phase drains within its scenario budget; no unexpected
  terminal failure/quarantine in mixed or retention workloads. Final state and
  queue counters agree with durable records after quiescence.
- Successful throughput sustains at least 95% of the chosen offered rate
  outside documented fault/recovery intervals. Stable-window operation p95
  stays within 20% and p99 within 2x of the same-host baseline, with no downward
  throughput trend or growing queue/maintenance lag. Investigate breaches;
  do not change the threshold after seeing the result.
- In retention phases, job and metadata populations plateau after warm-up and
  repeatedly fall as deletions occur. Compare 30-minute windows after hour 1;
  unexplained monotonic heap/RSS/disk/index growth blocks sign-off. Account for
  normal PostgreSQL reusable space, Redis fragmentation, and AOF rewrites.
- Sampling stays current, no Redis eviction occurs, and every injected fault
  has a recorded recovery result. Datastore durability loss is quantified
  separately and accepted explicitly by the deployment owner.

Archive the full artifacts, system/datastore samples, fault timeline, image
digests, candidate patch, comparison tables, and final datastore snapshot under
each run ID. Review every failed invariant and outlier before signing off.
An interrupted or aborted run is incomplete evidence. Any correctness fix
requires fresh relevant qualification and a fresh `productionCheck` before 1.0.

Handler lock traces track outstanding started brackets separately from cumulative
attempt counts. A claim refunded before its handler starts emits no release,
even when an earlier attempt ran. Final lock pairing and the independent
datastore hold/counter audit remain required.
