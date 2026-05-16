# Getting Started

Threadmill is a Java 25 background job-processing library with at-least-once
delivery. Handlers must be idempotent because a job may run more than once after
a crash, timeout, or orphan recovery.

## Run the checked example

```bash
./gradlew :threadmill-example:run
```

The example uses `InMemoryJobStore`, starts a `ProcessingNode`, enqueues one job,
waits for `SUCCEEDED`, and exits. Its source is the canonical getting-started
snippet: `threadmill-example/src/main/java/com/example/threadmill/GettingStartedMain.java`.

## Minimal wiring

```java
InMemoryJobStore store = new InMemoryJobStore();
Scheduler scheduler = new Scheduler(store, new JsonJobSerializer());

try (ProcessingNode node = ProcessingNode.builder(store).build()) {
    node.start();
    scheduler.enqueue(new SendEmail("ops@example.test", "hello"), SendEmailHandler.class);
}
```

For production, replace `InMemoryJobStore` with `PostgresJobStore` or
`RedisJobStore`, run the datastore with durable storage, and configure queue
lanes and timeouts explicitly.

## Real store examples

The example module also contains runnable Postgres and Redis examples:

```bash
docker compose up -d postgres redis

./gradlew :threadmill-example:runWorker --args="postgres alpha"
./gradlew :threadmill-example:runWorker --args="redis alpha"

./gradlew :threadmill-example:submit --args="postgres 100"
./gradlew :threadmill-example:submit --args="redis 100"
```

See `threadmill-example/README.md` for the multi-terminal walkthrough. Use
`threadmill-simulation` for correctness simulations and worker-churn traces.
