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
