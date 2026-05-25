# Spring Quickstart

Threadmill's Spring Boot integration is the lowest-boilerplate path. It still
uses at-least-once delivery: handlers must be idempotent because recovery can
run the same logical job more than once.

> **Spring Boot 4.x.** This module requires Spring Boot 4.0 or newer.
> Spring Boot 3.x is not supported and will fail fast at application startup
> with a clear message.

## Dependencies

Use Java 25 and add the Spring module plus one store:

```kotlin
implementation("com.hemju:threadmill-spring-boot:0.1.0-rc.1")
implementation("com.hemju:threadmill-store-postgres:0.1.0-rc.1")
// or: implementation("com.hemju:threadmill-store-redis:0.1.0-rc.1")
```

## Handler

```java
package com.example.mail;

import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.spring.Job;
import com.hemju.threadmill.spring.JobScheduler;
import org.springframework.stereotype.Component;

public record SendEmail(String to, String subject) implements JobPayload {}

@Component
@Job(queue = "email", timeout = "PT2M", maxRetries = 5)
final class SendEmailHandler implements JobHandler<SendEmail> {
    @Override
    public void run(SendEmail payload, JobExecutionContext ctx) {
        ctx.log("sending " + payload.subject() + " to " + payload.to());
        // Make the external side effect idempotent.
    }
}
```

## Enqueue

```java
@RestController
final class MailController {
    private final JobScheduler jobs;

    MailController(JobScheduler jobs) {
        this.jobs = jobs;
    }

    @PostMapping("/mail")
    JobId send(@RequestBody SendEmail command) {
        return jobs.enqueue(SendEmailHandler.class, command);
    }
}
```

`JobScheduler` verifies the handler/payload pair at enqueue time. If two
`@Job` beans handle the same payload type, startup fails and names both handlers.

## Enqueue And Transactions

By default the auto-configured `JobScheduler` is transaction-aware: a job
enqueued inside an active Spring transaction is held until `afterCommit`. A
rollback leaves nothing in the queue, so a job can never pick up state that
the surrounding transaction did not commit.

```java
@Transactional
public void scheduleWelcome(UserCreated created) {
    userRepo.save(created.toUser());        // pending write
    jobs.enqueue(SendEmailHandler.class, new SendEmail(created.email(), "Welcome"));
    // Both happen — or neither: the job insert fires on afterCommit.
}
```

The returned `JobId` is reserved synchronously (UUIDv7 is generated client
side), but the store row appears only after the transaction commits. If a
caller depends on `store.findById(id)` succeeding immediately after
`enqueue()` returns — e.g., a non-transactional code path that re-reads
its own write — disable the wrapper:

```yaml
threadmill:
  spring:
    enqueue-mode: immediate
```

For Spring + Postgres, `threadmill.spring.enqueue-mode=join_transaction` makes
normal enqueue, scheduled enqueue, bulk enqueue, and dedup enqueue part of the
caller's SQL transaction.

## Configure A Store

Without durable store configuration Spring creates an in-memory store and logs
one warning. That is useful locally only.

```yaml
threadmill:
  store:
    redis:
      mode: standalone
      uri: redis://localhost:6379
```

For Postgres, add `threadmill-store-postgres` and define a normal Spring
`DataSource`. Spring auto-configures `PostgresJobStore` from that `DataSource`
and runs pending Threadmill schema migrations by default. Use
`threadmill.store.postgres.schema-mode=validate` if your deployment pipeline
applies the DDL separately. See [postgres-schema.md](postgres-schema.md) for
manual SQL and reset guidance.
