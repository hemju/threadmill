package com.hemju.threadmill.soak.harness.scenario;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobRelationship;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.engine.QueueWeights;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.soak.harness.LoadGenerator;
import com.hemju.threadmill.soak.harness.SoakExecutionTrace;
import com.hemju.threadmill.soak.harness.invariant.InvariantChecks;
import com.hemju.threadmill.soak.harness.invariant.SoakInvariant;

/** Repeatable high-cardinality workload with ten-second retention and real retry/workflow activity. */
public final class RetentionChurnScenario implements SoakScenario {
  private static final Duration RETENTION = Duration.ofSeconds(10);
  private static final Duration DEDUP_TTL = Duration.ofSeconds(5);
  private static final String DATA = "payload-".repeat(1024);

  @Override
  public String name() {
    return "retention-churn";
  }

  @Override
  public String description() {
    return "10s retention, fresh concurrency/dedup keys, 8KiB payloads, deterministic retries and workflows";
  }

  @Override
  public boolean supportsConcurrentProducers() {
    return false;
  }

  @Override
  public List<SoakInvariant> invariants() {
    return List.of(
        InvariantChecks.atLeastOnce(),
        InvariantChecks.exclusivityHeld(),
        InvariantChecks.noLockLeaks(),
        InvariantChecks.retryBudgetRespected(5));
  }

  @Override
  public ProcessingNodeConfig.Builder tuneConfig(ProcessingNodeConfig.Builder builder) {
    return builder
        .retentionInterval(Duration.ofSeconds(1))
        .maintenancePollInterval(Duration.ofMillis(100))
        .succeededRetention(RETENTION)
        .failedRetention(RETENTION)
        .deletedRetention(RETENTION)
        .quarantinedRetention(RETENTION);
  }

  @Override
  public void configureNode(ProcessingNode.Builder builder) {
    builder.lane("retention:*", 8, QueueWeights.uniform());
  }

  @Override
  public void runWorkload(LoadGenerator gen, SoakRunContext ctx) throws InterruptedException {
    var serializer = new JsonJobSerializer();
    long sequence = 0;
    while (Instant.now().isBefore(ctx.runDeadline())) {
      gen.pace(gen.deadlineFor(ctx.runStart(), sequence));
      if (!Instant.now().isBefore(ctx.runDeadline())) break;
      String key = ctx.config().runId() + ":" + sequence;
      String queue = "retention:" + sequence % 8;
      var mode = sequence % 10 == 0 ? ConcurrencyMode.EXCLUSIVE : ConcurrencyMode.SHARED;
      var root = Job.builder()
          .queue(queue)
          .concurrencyKey(key)
          .concurrencyMode(mode)
          .spec(JobSpec.of(
              WorkHandler.class.getName(),
              serializer.serializePayload(new Work(sequence, DATA, sequence % 7 == 0))))
          .build();
      if (sequence % 10 == 0) {
        var child = Job.builder()
            .queue(queue)
            .initialState(JobState.AWAITING)
            .relationship(new JobRelationship(root.id(), JobRelationship.Kind.WORKFLOW_STEP))
            .spec(JobSpec.of(
                WorkHandler.class.getName(),
                serializer.serializePayload(new Work(sequence + 1, DATA, false))))
            .build();
        ctx.store().insertAll(List.of(root, child));
        gen.recordAccepted(root);
        gen.recordAccepted(child);
        sequence += 2;
      } else if (sequence % 4 == 0) {
        var created = ctx.store().enqueueIfAbsent(root, key, DEDUP_TTL, Instant.now());
        if (!(created instanceof EnqueueResult.Created))
          throw new IllegalStateException("fresh dedup key already exists");
        gen.recordAccepted(root);
        var duplicate = ctx.store().enqueueIfAbsent(root, key, DEDUP_TTL, Instant.now());
        if (!(duplicate instanceof EnqueueResult.Coalesced coalesced)
            || !coalesced.existingId().equals(root.id())) {
          throw new IllegalStateException("deduplication did not preserve the accepted job id");
        }
        sequence++;
      } else {
        ctx.store().insert(root);
        gen.recordAccepted(root);
        sequence++;
      }
    }
  }

  /** Fixed workload data; every seventh root fails its first attempt. */
  public record Work(long sequence, String data, boolean retryOnce) implements JobPayload {}

  /** Idempotent synthetic handler with execution brackets used by the live invariant checker. */
  public static final class WorkHandler implements JobHandler<Work> {
    @Override
    public void run(Work work, JobExecutionContext context) throws InterruptedException {
      SoakExecutionTrace.started(context);
      try {
        if (work.retryOnce() && context.attempt() == 1)
          throw new IllegalStateException("planned first-attempt failure");
        Thread.sleep(8);
      } finally {
        SoakExecutionTrace.finished(context);
      }
    }
  }
}
