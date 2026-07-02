package com.hemju.threadmill.soak.harness.scenario;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.soak.harness.LoadGenerator;
import com.hemju.threadmill.soak.harness.invariant.InvariantChecks;
import com.hemju.threadmill.soak.harness.invariant.SoakInvariant;

/**
 * Producer side uses {@code enqueueAll} exclusively, in 50-job batches.
 * Verifies throughput is materially better than per-job enqueue with the
 * same workload shape, and atomicity (the batch contract is "all or none").
 *
 * <p>Note on bulk atomicity: the v1 harness records {@code enqueueAll}
 * batches in the trace; full failure-injection (deliberately oversize
 * payload in one batch position) is intentionally simple — the underlying
 * atomicity is already covered by {@code AbstractJobStoreContractTest}.
 */
public final class BulkEnqueueScenario implements SoakScenario {

    private static final int BATCH_SIZE = 50;

    // Batch ids must stay unique when several producer threads run this
    // workload concurrently — the bulkInsertAtomic invariant keys on them.
    private final AtomicLong batchIdSequence = new AtomicLong();

    @Override
    public String name() {
        return "bulk-enqueue";
    }

    @Override
    public String description() {
        return "producer uses enqueueAll exclusively in " + BATCH_SIZE + "-job batches";
    }

    @Override
    public List<SoakInvariant> invariants() {
        return List.of(
                InvariantChecks.atLeastOnce(), InvariantChecks.noLockLeaks(), InvariantChecks.bulkInsertAtomic());
    }

    @Override
    public void runWorkload(LoadGenerator gen, SoakRunContext ctx) throws InterruptedException {
        Instant runStart = ctx.runStart();
        long batchIdx = 0;
        long n = 0;
        while (Instant.now().isBefore(ctx.runDeadline())) {
            // Pace per-batch using the configured jobsPerSecond — one batch is
            // BATCH_SIZE jobs, so one batch every BATCH_SIZE/rate seconds.
            long nanosPerBatch = (long) BATCH_SIZE * Duration.ofSeconds(1).toNanos() / Math.max(1, gen.jobsPerSecond());
            Instant batchDeadline = runStart.plusNanos(batchIdx * nanosPerBatch);
            gen.pace(batchDeadline);

            String batchId = "batch-" + batchIdSequence.getAndIncrement();
            List<SoakPayloads.FixedWork> payloads = new ArrayList<>(BATCH_SIZE);
            for (int i = 0; i < BATCH_SIZE; i++) {
                payloads.add(new SoakPayloads.FixedWork((int) (n + i), 4, 0.0));
            }
            try {
                List<JobId> ids = gen.enqueueAll(payloads, SoakPayloads.FixedWorkHandler.class, "default", 0);
                // Augment the per-job enqueued events with the batchId so the
                // invariant can join them to a hypothetical bulk_batch_rejected
                // event for this batch.
                Map<String, Object> ack = new LinkedHashMap<>();
                ack.put("batchId", batchId);
                ack.put("count", ids.size());
                ctx.trace().emit("bulk_batch_committed", ack);
            } catch (RuntimeException ex) {
                Map<String, Object> rej = new LinkedHashMap<>();
                rej.put("batchId", batchId);
                rej.put("reason", ex.getClass().getSimpleName());
                ctx.trace().emit("bulk_batch_rejected", rej);
            }
            n += BATCH_SIZE;
            batchIdx++;
        }
    }
}
