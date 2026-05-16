package com.hemju.threadmill.store.memory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;

/** Handler implementations used by the engine tests. */
public final class EngineTestHandlers {

    private EngineTestHandlers() {}

    /** Records every invocation by job id; returns immediately. */
    public static final class CountingHandler implements JobHandler<HelloPayload> {
        public static final ConcurrentHashMap<String, AtomicInteger> COUNT = new ConcurrentHashMap<>();

        @Override
        public void run(HelloPayload payload, JobExecutionContext ctx) {
            COUNT.computeIfAbsent(ctx.jobId().toString(), k -> new AtomicInteger())
                    .incrementAndGet();
        }
    }

    /** Always throws; used to exercise the failure + retry path. */
    public static final class FailingHandler implements JobHandler<HelloPayload> {
        public static final AtomicInteger ATTEMPTS = new AtomicInteger();

        @Override
        public void run(HelloPayload payload, JobExecutionContext ctx) {
            ATTEMPTS.incrementAndGet();
            throw new RuntimeException("boom");
        }
    }

    /** Sleeps longer than the configured job timeout. */
    public static final class HangingHandler implements JobHandler<HelloPayload> {
        public static final AtomicInteger INTERRUPTS = new AtomicInteger();

        @Override
        public void run(HelloPayload payload, JobExecutionContext ctx) throws Exception {
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                INTERRUPTS.incrementAndGet();
                throw e;
            }
        }
    }

    /** Blocks until the node is closed; useful for inspecting claimed work. */
    public static final class BlockingHandler implements JobHandler<HelloPayload> {
        public static final AtomicInteger INTERRUPTS = new AtomicInteger();

        @Override
        public void run(HelloPayload payload, JobExecutionContext ctx) throws Exception {
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                INTERRUPTS.incrementAndGet();
                throw e;
            }
        }
    }

    /** Runs briefly so shutdown can prove it waits for in-flight work. */
    public static final class SlowHandler implements JobHandler<HelloPayload> {
        public static final AtomicInteger COMPLETIONS = new AtomicInteger();

        @Override
        public void run(HelloPayload payload, JobExecutionContext ctx) throws Exception {
            Thread.sleep(250);
            COMPLETIONS.incrementAndGet();
        }
    }

    /** Checks in regularly so a long job can outlive the wall-clock timeout. */
    public static final class CheckInHandler implements JobHandler<HelloPayload> {
        public static final AtomicInteger COMPLETIONS = new AtomicInteger();

        @Override
        public void run(HelloPayload payload, JobExecutionContext ctx) throws Exception {
            for (int i = 0; i < 5; i++) {
                ctx.checkIn("step " + i);
                ctx.updateProgress((i + 1) / 5.0);
                Thread.sleep(120);
            }
            COMPLETIONS.incrementAndGet();
        }
    }

    /** Throws an exception whose {@code getMessage()} is large enough to blow past
     * a job's serialized-size cap when concatenated into the FAILED state-history
     * entry. Used to pre-cover the audit §4.3 truncation invariant. */
    public static final class BigErrorMessageHandler implements JobHandler<HelloPayload> {
        public static final AtomicInteger ATTEMPTS = new AtomicInteger();
        public static final int MESSAGE_BYTES = 200 * 1024;

        @Override
        public void run(HelloPayload payload, JobExecutionContext ctx) {
            ATTEMPTS.incrementAndGet();
            throw new RuntimeException("big-error: " + "x".repeat(MESSAGE_BYTES));
        }
    }

    /** Checks in once, then hangs so the no-progress timeout can interrupt it. */
    public static final class StalledAfterCheckInHandler implements JobHandler<HelloPayload> {
        public static final AtomicInteger INTERRUPTS = new AtomicInteger();

        @Override
        public void run(HelloPayload payload, JobExecutionContext ctx) throws Exception {
            ctx.checkIn("started");
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                INTERRUPTS.incrementAndGet();
                throw e;
            }
        }
    }

    /** Simple payload used by every test handler. */
    public static final class HelloPayload implements JobPayload {
        public String name;

        public HelloPayload() {}

        public HelloPayload(String name) {
            this.name = name;
        }
    }

    public static void reset() {
        CountingHandler.COUNT.clear();
        FailingHandler.ATTEMPTS.set(0);
        HangingHandler.INTERRUPTS.set(0);
        BlockingHandler.INTERRUPTS.set(0);
        SlowHandler.COMPLETIONS.set(0);
        CheckInHandler.COMPLETIONS.set(0);
        StalledAfterCheckInHandler.INTERRUPTS.set(0);
        BigErrorMessageHandler.ATTEMPTS.set(0);
    }
}
