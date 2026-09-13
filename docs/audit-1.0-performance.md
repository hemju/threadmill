# PostgreSQL monitoring measurements for audit #135

These are local diagnostic measurements, not advertised capacity or a 1.0
endurance sign-off. The fixture used PostgreSQL 18 in a disposable container
limited to two CPUs and 1,536 MiB, with 100 queues and a mixture of old busy queues
and a newer target queue. Each population was bulk-loaded, then vacuumed and
analyzed. Trigger work was excluded from fixture setup; query timings below are
`EXPLAIN (ANALYZE, BUFFERS)` execution times, excluding network and JVM overhead.

| Jobs | Queue depths before/after (ms) | Queue discovery before/after (ms) | Target queue oldest before/after (ms) |
|---:|---:|---:|---:|
| 10,000 | 1.144 / 0.048 | 1.084 / 0.066 | 0.427 / 0.014 |
| 100,000 | 11.455 / 0.041 | 11.781 / 0.060 | 4.857 / 0.016 |
| 1,000,000 | 62.236 / 0.044 | 45.006 / 0.068 | 96.450 / 0.045 |

The new queue counters make reads proportional to queue/shard cardinality.
The partial `(queue, current_state_at)` index gives the oldest-job query a direct
ordered lookup. Counters add transactional write work and index maintenance;
these read timings alone do not establish a net application throughput gain.

The concurrent benchmark uses valid serialized jobs, an eight-connection pool,
four claimers, and 10-job claims. At each of 10k/100k/1m backlog, it alternates
control/monitoring/monitoring/control phases of 1,000 claims. Monitoring executes
the metrics store-query mix and a 20-record dashboard page, at a target 10 Hz.
A first run exposed a full-population dashboard history sort; V9 now includes
`(state, current_state_at DESC, id)` for that page as well.

```sh
./gradlew :threadmill-soak:benchmarkPostgresMonitoring
```

The CSV is written to `threadmill-soak/build/soak/postgres-monitoring/claims.csv`.
Every phase checks distinct claims and the final exact queue count. In the final
local run, the warmed one-million-job pair had p95 claim-call latencies of
3.51 ms with monitoring and 3.50 ms without. The first pair was much slower
(82.60/84.80 ms), which demonstrates why cache/JVM warmup and repeated sustained
runs matter. Phases are deliberately short; their jobs/second values must not be
used as production sizing estimates. The separate endurance plans require
longer stable measurement windows, dashboard/metrics polling, retention and
concurrent producers on isolated comparable resources.
