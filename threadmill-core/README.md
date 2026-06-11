# threadmill-core

The framework-agnostic core of Threadmill: the job model, the state machine, the
`JobStore` SPI, the JSON serializer, the processing engine, the scheduling API,
and the interceptor surface. Every other module depends on this one. No storage
implementation, no UI, and no framework code (Spring, CDI, etc.) lives here.

## Quick map

- **The model.** `Job` is the live aggregate handed to engine code. `JobSnapshot`
  is the immutable view the serializer walks. `JobSpec` + `JobArgument` describe
  the work; `JobPayload` is the user-facing typed wrapper. `JobState` is the
  centralised state machine — illegal transitions throw.
- **The engine.** `ProcessingNode` is one per JVM. It runs one or more
  `Dispatcher`s (one per `QueueLane` or `QueueFamily`), the `JobRunner` that
  executes user handlers, the `MaintenanceCycle` that promotes scheduled jobs
  and reclaims orphans, and the `NodeRegistry` that fights for the master lease.
- **The SPI.** `JobStore` is the persistence boundary, expressed in operations
  and guarantees, not SQL. Every backend extends `AbstractJobStoreContractTest`
  in `threadmill-test-support` and is held to the same 76-test suite.
- **The scheduler.** `Scheduler` is the user-facing API for enqueue / schedule
  / recurring. It needs only a `JobStore` and a `JobSerializer` — no running
  engine — so submission-only nodes work.

## Lifecycle of a job

```
       enqueue                  claim                run                 save
ENQUEUED ────► claimReady ────► PROCESSING ───► handler.run ───► SUCCEEDED|FAILED|…
                  │
                  └─ heartbeat refresh ◄─── owner-heartbeat thread (every node)
                  └─ orphan reclaim ─────►  MaintenanceCycle on the master, if heartbeat expires
```

Every save funnels through `store.saveAtomic(job, expectedVersion)`. Every
failure funnels through `JobRunner.recordFailure(...)`. Both invariants
are load-bearing — see `AGENTS.md` §6.

## Key invariants

1. **Optimistic-lock `version` is persisted state.** The in-memory job's
   version is only advanced after a successful save, via
   `Job.adoptVersion(long)`. A failed save (`StaleJobException`,
   `OversizedJobException`, anything) leaves the job reusable.
2. **Snapshot-on-serialize.** `Job.snapshot()` copies user-touchable areas
   under the job's monitor before serialization. Concurrent mutation during
   serialization is impossible by construction.
3. **One state machine, centralised and tested.** `JobStateMachine.requireLegal`
   throws on illegal transitions. `PROCESSING → ENQUEUED` is explicitly illegal:
   orphan recovery routes through `FAILED`.
4. **One failure code path.** `JobRunner.recordFailure(...)` is the only place
   "a job failed" is expressed. Exceptions, per-job timeouts, orphan reclaim,
   and quarantine all flow through it.
5. **Scoped values, not `ThreadLocal`.** `EngineScopedValues.CURRENT` is bound
   around `handler.run(...)`. The binding is inherited by structured-concurrency
   forks (a `StructuredTaskScope` opened in the handler), but **not** by virtual
   threads the handler spawns directly via an executor — use
   `EngineScopedValues.capturing(...)` to carry it across that boundary.

## Virtual threads

Every worker is a virtual thread (`Executors.newVirtualThreadPerTaskExecutor`).
The `Dispatcher`'s polling loop runs on a platform thread. Capacity is enforced
by a `Semaphore` per `QueueLane` — that's the engine's defence against
starvation. A flood of jobs on one queue cannot occupy capacity reserved for
another.

## The interceptor surface

`JobInterceptor` has four hooks:

```
onProcessingStarting (before handler.run)
onProcessingSucceeded (after normal return)
onProcessingFailed (single failure path: EXCEPTION, TIMEOUT, ORPHAN_RECLAIM, QUARANTINE)
onStateChange (every persisted transition)
```

`RetryInterceptor` and `WorkflowInterceptor` are auto-attached by
`ProcessingNode`. User interceptors come via `ProcessingNode.builder().interceptor(...)`.
Retry policy precedence: per-job metadata > per-exception-type > global default.

## Serialization

JSON via Jackson is the default (`JsonJobSerializer`). The wire form is
explicit and typed; supplementary-plane Unicode round-trips losslessly. Two
`serializeJob` overloads:

- `serializeJob(snapshot, long maxBytes)` — strict size cap; no truncation.
- `serializeJob(snapshot, JobStoreCapabilities)` — applies log truncation
  (FIFO from the head) and FAILED/QUARANTINED message capping with a sentinel
  before the size check. Stores use this overload so a `PROCESSING → FAILED`
  save can never be blocked by an oversized exception trace.

## Concurrency primitives

`concurrencyKey` + `ConcurrencyMode {EXCLUSIVE, SHARED}` on `Job` plus
`workflow_root_id` for workflow inheritance. Enforcement is at **claim time**,
not enqueue time — `JobStore.claimReady` consults per-key bookkeeping (counts +
pending) before moving a job to `PROCESSING`. The lock is held continuously
from the workflow root's claim to the last descendant's terminal save.

See `docs/concurrency.md` for worked examples.

## Public API surface

Read [`AGENTS.md`](../AGENTS.md) §5 for the canonical vocabulary table — every
type listed there is part of the public API and has Javadoc on every public
method.

## Build

```
./gradlew :threadmill-core:test
```

76 contract tests run as part of every store module's suite; module-local
tests cover the state machine, the JSON serializer, JobLog bounds, queue
families, the wake signal, and cron expressions.
