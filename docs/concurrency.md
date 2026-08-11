# Concurrency

Threadmill supports claim-time per-key concurrency for jobs that protect the
same resource. This is scheduling metadata on `Job`, not part of `JobSpec`.

```java
Job job = Job.builder()
        .spec(spec)
        .queue("project:42")
        .concurrencyKey("project:42")
        .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
        .build();
```

The lower-level `Scheduler` overload mirrors the same fields:

```java
scheduler.enqueue(payload, ImportHandler.class, "project:42", 0,
        "project:42", ConcurrencyMode.EXCLUSIVE);
```

`concurrencyKey` must be non-blank and at most 256 UTF-8 bytes.
`concurrencyMode` is required when a key is present.

## Modes

`SHARED` jobs for the same key can run together. `EXCLUSIVE` jobs run alone
for their key. Jobs without a concurrency key are unconstrained and keep the
same claim behavior as ordinary Threadmill jobs.

Within one key, Threadmill preserves enqueue order. A later `SHARED` job does
not leapfrog an earlier pending `EXCLUSIVE` job, even though it could run with
other shared jobs. This prevents writer starvation without a separate priority
setting.

## Release

The concurrency hold is persisted and moves with state transitions:

- `ENQUEUED -> PROCESSING` acquires the hold.
- Any `PROCESSING -> SUCCEEDED`, `FAILED`, `DELETED`, or `QUARANTINED`
  releases the hold when the workflow rooted at that job is done.

Timeouts, no-progress timeouts, thrown exceptions, orphan reclaim, and
quarantine all use the same engine failure path, so the release is attached to
the same persisted transition that records the failure.

Threadmill still provides at-least-once delivery. Concurrency prevents
simultaneous execution for a key; handlers must still be idempotent because a
job may run again after a crash.

For backend-specific claim and release paths, including the Postgres row-lock
shape and the Redis Lua-script shape, see
[Backend execution model](backend-execution-model.md).

## Workflows

Workflow successors inherit the parent's concurrency key and mode. The hold is
owned by the workflow root (`workflow_root_id`) and survives until every job in
that workflow is terminal. A chain such as `validate -> import -> notify` with
`EXCLUSIVE` on `project:42` keeps the project locked until `notify` finishes.

Branching workflows behave the same way: if the root has three successors, the
key is released only after all three are terminal, even when one fails and the
others succeed.

## Exclusive recurring tasks

A recurring task can declare that only one of its instances runs at a time
across the whole cluster. Every materialized instance — scheduled, caught-up
under `CATCH_UP`, or triggered by hand from the dashboard — is claimed under a
derived key in `EXCLUSIVE` mode, so the store refuses to admit a second one
while another is processing.

```java
@Job
@Recurring(interval = "PT1M", exclusive = true)
public class NightlySweep implements JobAction { … }
```

Outside Spring, use the `Scheduler.defineRecurring(…)` overload that takes the
`exclusive` flag.

The key is **derived, not user-supplied**: it is `recurring:<task name>`, in a
namespace reserved for exactly this, so it can never collide with an
application's own concurrency keys. Task names longer than the 256-UTF-8-byte
key cap are truncated on a code-point boundary with a stable hash suffix.

This replaces the advisory lock or status-claim column that a singleton sweep
usually grows. It is stronger than the recurring pile-up guard, which only
decides what to *materialize*: exclusivity is enforced at claim time by the
store, on every node, so it also covers a dashboard manual trigger racing a
scheduled instance, and the window between a failed instance and its retry.

**It does not close the reclaim window.** Orphan reclaim releases the
concurrency slot as part of the terminal failure save, so a reclaimed instance
can still overlap an original that is still running on a node which stopped
heartbeating without dying. See
[the overlap windows](transactions.md#when-exactly-can-two-instances-of-the-same-job-overlap)
for why that window is not closable and how to fence against it. Handlers stay
idempotent by contract.

## Examples

For project import/export workloads, put imports in `EXCLUSIVE` mode and
exports in `SHARED` mode with a key such as `project:<id>`. Multiple exports
for the same project can run together, while imports wait for readers to finish
and block later readers until the import completes.

For tenant event processing, use a key such as `tenant:<id>`. A replay or
schema migration can run as `EXCLUSIVE`; independent event fan-out can run as
`SHARED`.

For batch jobs, put the root job's key and mode on the root. Children inherit
the key, mode, and `workflow_root_id`, so the batch holds the resource until
the last descendant terminates.
