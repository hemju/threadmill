# Troubleshooting

## Queue backlog

Check `threadmill.queue.depth{queue}` and oldest enqueued age. Add workers,
increase the lane capacity for that queue, or inspect handler latency.

## Jobs stuck in processing

Check oldest processing heartbeat age and node heartbeat age. If the owner is
gone, the maintenance leader should reclaim the job after `heartbeatTimeout`.

## Store unreachable

Dispatchers pause after `maxConsecutiveDispatcherFailures` and probe at
`storeOutagePollInterval`. Restore the datastore; the cluster should resume
without restarting nodes.

## Duplicate execution

This is expected under at-least-once delivery. Add or fix idempotency in the
handler or in the external system call.

## Quarantined jobs

`QUARANTINED` means the handler or payload could not be resolved/deserialized.
Deploy the missing handler or repair the payload before retrying manually.

## Leader flapping

Ensure `maintenanceLeaseDuration` is comfortably greater than
`claimHeartbeat`, and check store latency. A flapping leader can delay recurring
materialization and orphan recovery.
