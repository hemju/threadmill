package com.hemju.threadmill.soak.harness.scenario;

import java.util.concurrent.ThreadLocalRandom;

import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;

/**
 * Shared job payloads + handlers used by the scenarios. Each handler is
 * deliberately small and idempotent — every handler may run more than once
 * under at-least-once delivery.
 */
public final class SoakPayloads {

    private SoakPayloads() {}

    /** Fixed-cost work — sleeps the configured duration. */
    public static final class FixedWork implements JobPayload {
        public int seq;
        public long durationMillis;
        public double failureRate;

        public FixedWork() {}

        public FixedWork(int seq, long durationMillis, double failureRate) {
            this.seq = seq;
            this.durationMillis = durationMillis;
            this.failureRate = failureRate;
        }
    }

    public static final class FixedWorkHandler implements JobHandler<FixedWork> {

        @Override
        public void run(FixedWork p, JobExecutionContext ctx) throws InterruptedException {
            if (p.failureRate > 0 && ThreadLocalRandom.current().nextDouble() < p.failureRate) {
                throw new RuntimeException("soak: simulated failure for seq " + p.seq);
            }
            if (p.durationMillis > 0) Thread.sleep(p.durationMillis);
        }
    }

    /** Sometimes-hangs — fails or runs over jobTimeout with a configured probability. */
    public static final class FlakyWork implements JobPayload {
        public int seq;
        public long durationMillis;
        public double failureRate;
        public double timeoutRate;
        public long timeoutSleepMillis;

        public FlakyWork() {}

        public FlakyWork(
                int seq, long durationMillis, double failureRate, double timeoutRate, long timeoutSleepMillis) {
            this.seq = seq;
            this.durationMillis = durationMillis;
            this.failureRate = failureRate;
            this.timeoutRate = timeoutRate;
            this.timeoutSleepMillis = timeoutSleepMillis;
        }
    }

    public static final class FlakyWorkHandler implements JobHandler<FlakyWork> {

        @Override
        public void run(FlakyWork p, JobExecutionContext ctx) throws InterruptedException {
            double dice = ThreadLocalRandom.current().nextDouble();
            if (dice < p.failureRate) {
                throw new RuntimeException("soak: flaky failure for seq " + p.seq);
            }
            if (dice < p.failureRate + p.timeoutRate) {
                Thread.sleep(p.timeoutSleepMillis);
                return;
            }
            if (p.durationMillis > 0) Thread.sleep(p.durationMillis);
        }
    }

    /** Long-running with periodic check-ins — survives noProgressTimeout. */
    public static final class CheckingInWork implements JobPayload {
        public int seq;
        public long totalDurationMillis;
        public long checkInIntervalMillis;

        public CheckingInWork() {}

        public CheckingInWork(int seq, long totalDurationMillis, long checkInIntervalMillis) {
            this.seq = seq;
            this.totalDurationMillis = totalDurationMillis;
            this.checkInIntervalMillis = checkInIntervalMillis;
        }
    }

    public static final class CheckingInWorkHandler implements JobHandler<CheckingInWork> {

        @Override
        public void run(CheckingInWork p, JobExecutionContext ctx) throws InterruptedException {
            long deadline = System.currentTimeMillis() + p.totalDurationMillis;
            long interval = Math.max(50L, p.checkInIntervalMillis);
            while (System.currentTimeMillis() < deadline) {
                Thread.sleep(Math.min(interval, Math.max(1L, deadline - System.currentTimeMillis())));
                ctx.checkIn();
            }
        }
    }

    /** Stalled long-running — never calls checkIn, so it must be killed. */
    public static final class StalledWork implements JobPayload {
        public int seq;
        public long stallMillis;

        public StalledWork() {}

        public StalledWork(int seq, long stallMillis) {
            this.seq = seq;
            this.stallMillis = stallMillis;
        }
    }

    public static final class StalledWorkHandler implements JobHandler<StalledWork> {

        @Override
        public void run(StalledWork p, JobExecutionContext ctx) throws InterruptedException {
            // One initial check-in flips the engine from wall-clock jobTimeout
            // to noProgressTimeout (per AGENTS.md). Then we go silent so the
            // engine reclaims us — that's the path this scenario tests.
            ctx.checkIn();
            Thread.sleep(p.stallMillis);
        }
    }
}
