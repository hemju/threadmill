# Operability Check

Use this when reviewing on-call readiness.

1. Start at least two `ProcessingNode`s against one real store.
2. Confirm only one maintenance lease holder exists.
3. Pause the datastore and verify dispatchers pause rather than crash.
4. Resume the datastore and verify work completes.
5. Check metrics for queue depth, oldest enqueued age, processing heartbeat age,
   job counts, failures, and refresh errors.
6. Kill a worker mid-job and verify orphan recovery.
