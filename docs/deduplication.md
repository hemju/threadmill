# Producer-Side Deduplication

Deduplication prevents duplicate enqueues. It does not change Threadmill's
at-least-once execution guarantee, and it does not remove the need for
idempotent handlers.

```java
EnqueueResult result = jobEnqueuer.enqueueIfAbsent(
        new RebuildAccount(accountId),
        "account:" + accountId,
        Duration.ofHours(6));
```

The result is either:

- `Created(JobId id)` when this producer created a new job.
- `Coalesced(JobId existingId)` when an existing unexpired or still-active job
  already represents the same queue and dedup key.

## Semantics

The dedup identity is `(queue, dedupKey)`.

- `dedupKey` must be nonblank and at most 256 UTF-8 bytes.
- `dedupTtl` must be positive and is capped by `maxDedupTtl` (default `30d`).
- An expired dedup record still coalesces while its referenced job is
  non-terminal.
- Maintenance removes expired records only after the referenced job is terminal
  or gone.

Postgres stores dedup records in `threadmill_dedup_keys`. Redis stores them in
`{threadmill}:dedup:*` keys plus an expiry index; it intentionally does not use
Redis key TTL because a long-pending job must not lose dedup protection before
completion.
