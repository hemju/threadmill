package com.example.threadmill;

import java.time.Duration;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.schedule.Scheduler;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

/** Minimal, self-contained Threadmill example used by the public docs. */
public final class GettingStartedMain {

    private GettingStartedMain() {}

    /** Example payload. */
    public record SendEmail(String to, String subject) implements JobPayload {}

    /** Example idempotent handler. */
    public static final class SendEmailHandler implements JobHandler<SendEmail> {
        @Override
        public void run(SendEmail payload, JobExecutionContext context) {
            context.log().info("sending " + payload.subject() + " to " + payload.to());
        }
    }

    static void main(String[] args) {
        var store = new InMemoryJobStore();
        var scheduler = new Scheduler(store, new JsonJobSerializer());
        ProcessingNodeConfig config = ProcessingNodeConfig.builder()
                .pollInterval(Duration.ofMillis(25))
                .claimHeartbeat(Duration.ofMillis(100))
                .maintenanceLeaseDuration(Duration.ofMillis(500))
                .jobTimeout(Duration.ofSeconds(5))
                .build();

        var id = scheduler.enqueue(new SendEmail("ops@example.test", "hello from Threadmill"), SendEmailHandler.class);
        try (ProcessingNode node = ProcessingNode.builder(store).config(config).build()) {
            node.start();
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (System.nanoTime() < deadline) {
                Job job = store.findById(id).orElseThrow();
                if (job.currentState() == JobState.SUCCEEDED) {
                    System.out.println("processed " + id);
                    return;
                }
                try {
                    Thread.sleep(25);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while waiting for example job", e);
                }
            }
        }
        throw new IllegalStateException("example job did not complete");
    }
}
