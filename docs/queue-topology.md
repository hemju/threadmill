# Queue Topology

Threadmill uses queue lanes to reserve worker capacity inside one
`ProcessingNode`. A fixed lane drains exactly one queue:

```java
ProcessingNode node = ProcessingNode.builder(store)
        .lane("default", 8)
        .lane(Scheduler.SYSTEM_QUEUE, 2)
        .build();
```

Use fixed lanes for a small number of known queues, especially the `system`
queue used by recurring and housekeeping work that should not be starved by
application jobs.

## Queue Families

A queue-family lane drains every enqueued queue that matches a simple anchored
pattern. The lane has one shared worker pool, so hundreds of project queues can
share capacity without pre-registering each name.

```java
ProcessingNode node = ProcessingNode.builder(store)
        .lane("default", 8)
        .lane("project:*", 50, QueueWeights.uniform())
        .build();
```

Patterns support only:

| Pattern part | Meaning |
|---|---|
| `*` | Zero or more characters within one queue-name segment. |
| `?` | Exactly one character. |

Patterns are anchored at both ends. `project:*` matches `project:42` and
`project:x`, but not `xproject:42`, `projectA:42`, or `project:42:sub`.
Threadmill rejects regular-expression syntax, character classes, double-star
patterns, and literal `%` / `_` pattern characters.

## Weights

`QueueWeights` controls the proportion of claims between matched queues:

```java
QueueWeights.uniform();
QueueWeights.fromMap(Map.of("project:42", 10, "project:43", 1));
QueueWeights.from(queue -> queue.endsWith(":hot") ? 5 : 1);
```

Unlisted queues in `fromMap` default to weight `1`. A weight of `0` pauses a
queue without deleting its jobs. Negative weights are rejected.

The dispatcher uses stride scheduling, so a 10:1 weight does not run ten large
batches from one queue and then one batch from another. It interleaves picks
smoothly while still converging on the requested throughput ratio.

## Discovery

Queue-family lanes discover matching queues periodically. Spring Boot binds the
cadence under:

| Setting | Default | Notes |
|---|---:|---|
| `threadmill.queue-family.discovery-interval` | `1s` | How often a pattern lane refreshes its matching queue set and weight cache. |
| `threadmill.queue-family.retention-after-empty` | `30s` | How long an empty queue stays in the lane working set before it is dropped. |

The retention window keeps bursty queues from being rediscovered on every
cadence. A queue that appears after a node starts is discovered within one
discovery interval.

## Project Queue Example

A common shape is one queue per resource plus a concurrency key for the same
resource:

```java
scheduler.enqueue(
        new ExportProject(projectId),
        ExportProjectHandler.class,
        "project:" + projectId,
        0,
        "project:" + projectId,
        ConcurrencyMode.SHARED);

scheduler.enqueue(
        new ImportProject(projectId),
        ImportProjectHandler.class,
        "project:" + projectId,
        0,
        "project:" + projectId,
        ConcurrencyMode.EXCLUSIVE);
```

The queue-family lane shares capacity across all active project queues. The
concurrency key still enforces per-project reader/writer rules at claim time,
even if two jobs for the same key are accidentally placed on different matched
queues.
