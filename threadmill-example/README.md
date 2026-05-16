# Threadmill Examples

This module is deliberately written like an application, not like Threadmill
internals. Its package is `com.example.threadmill` so users can copy the shape
into their own package instead of inheriting a library-author namespace.

It contains three levels of copyable examples:

1. `./gradlew :threadmill-example:run` — a tiny single-process
   `InMemoryJobStore` example that enqueues one job, starts a node, waits for
   success, and exits.
2. Manual multi-instance Postgres and Redis examples — start two or three
   workers against one real store, submit jobs from another terminal, and watch
   the workers share the queue.
3. Submitter examples that enqueue work into the shared Postgres or Redis
   stores while workers are already running.

## Shared datastores

Start Postgres and Redis:

```bash
docker compose up -d postgres redis
```

Defaults:

- Postgres: `jdbc:postgresql://localhost:55432/threadmill`
- Redis: `redis://localhost:56379/0`

Override with:

- `THREADMILL_JDBC_URL`
- `THREADMILL_DB_USER`
- `THREADMILL_DB_PASSWORD`
- `THREADMILL_REDIS_URI`

Redis is started with AOF enabled because Redis durability depends on Redis
persistence settings.

## Single-process getting started

```bash
./gradlew :threadmill-example:run
```

Read:

- `src/main/java/com/example/threadmill/GettingStartedMain.java`

That file shows the minimum wiring: create a store, create a `Scheduler`, start
a `ProcessingNode`, enqueue a payload, and wait until the job succeeds.

## Manual Postgres cluster

Terminal 1:

```bash
./gradlew :threadmill-example:runWorker --args="postgres alpha"
```

Terminal 2:

```bash
./gradlew :threadmill-example:runWorker --args="postgres bravo"
```

Terminal 3:

```bash
./gradlew :threadmill-example:submit --args="postgres 100"
```

The two workers race through the same Postgres `threadmill_jobs` table. Claims
are atomic, so one job is owned by one node at a time. Kill a worker with
`Ctrl-C` while jobs are running; the surviving node will recover abandoned work
after the heartbeat timeout.

## Manual Redis cluster

Terminal 1:

```bash
./gradlew :threadmill-example:runWorker --args="redis alpha"
```

Terminal 2:

```bash
./gradlew :threadmill-example:runWorker --args="redis bravo"
```

Terminal 3:

```bash
./gradlew :threadmill-example:submit --args="redis 100"
```

The workers share Redis queues and processing indexes. The claim path commits
the serialized body and all Redis indexes in one Lua script, so a process crash
before commit leaves the job enqueued and a crash after commit leaves it
recoverable from processing indexes.

## Cleanup

```bash
docker compose down
```

Add `-v` if you deliberately want to drop both datastore volumes.
