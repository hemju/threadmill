package com.hemju.threadmill.soak.harness.scenario;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.QueueWeights;
import com.hemju.threadmill.soak.harness.LoadGenerator;
import com.hemju.threadmill.soak.harness.invariant.InvariantChecks;
import com.hemju.threadmill.soak.harness.invariant.SoakInvariant;

/**
 * Mid-run, a randomly-chosen queue is paused, then resumed shortly after.
 * Verifies no {@code claimed} events appear for the paused queue during the
 * pause window.
 */
public final class PauseResumeScenario implements SoakScenario {

    private static final int QUEUE_COUNT = 10;
    private static final Duration PAUSE_DURATION = Duration.ofSeconds(5);

    @Override
    public String name() {
        return "pause-resume";
    }

    @Override
    public String description() {
        return "10 project queues, randomly-chosen queue paused mid-run for 5s";
    }

    @Override
    public List<SoakInvariant> invariants() {
        return List.of(InvariantChecks.atLeastOnce(), InvariantChecks.pauseObeyed());
    }

    @Override
    public boolean supportsConcurrentProducers() {
        // Each workload invocation runs its own pause/resume bracket; N
        // producers pausing and resuming the same queues concurrently would
        // interleave brackets and make pauseObeyed unjudgeable.
        return false;
    }

    @Override
    public void configureNode(ProcessingNode.Builder b) {
        b.lane("project:*", 8, QueueWeights.uniform());
    }

    @Override
    public void runWorkload(LoadGenerator gen, SoakRunContext ctx) throws InterruptedException {
        Instant runStart = ctx.runStart();
        Duration total = ctx.duration();
        Instant pauseAt = runStart.plus(total.dividedBy(3));
        Instant resumeAt = pauseAt.plus(PAUSE_DURATION);
        boolean paused = false;
        String pausedQueue = null;
        long n = 0;
        while (Instant.now().isBefore(ctx.runDeadline())) {
            gen.pace(gen.deadlineFor(runStart, n));
            if (!paused && Instant.now().isAfter(pauseAt)) {
                pausedQueue = "project:" + ThreadLocalRandom.current().nextInt(QUEUE_COUNT);
                ctx.pauseQueue(pausedQueue, "soak: pause-resume scenario");
                paused = true;
            }
            if (paused && Instant.now().isAfter(resumeAt)) {
                ctx.resumeQueue(pausedQueue);
                paused = false;
            }
            String queue = "project:" + ((int) (n % QUEUE_COUNT));
            gen.enqueue(new SoakPayloads.FixedWork((int) n, 6, 0.0), SoakPayloads.FixedWorkHandler.class, queue, 0);
            n++;
        }
        if (paused && pausedQueue != null) ctx.resumeQueue(pausedQueue);
    }
}
