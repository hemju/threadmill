package com.hemju.threadmill.simulation;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;

/**
 * The simulation's two job shapes:
 *
 * <ul>
 *   <li>{@link Import} — {@code EXCLUSIVE} on {@code project:N}, slow.</li>
 *   <li>{@link Export} — {@code SHARED} on {@code project:N}, fast.</li>
 * </ul>
 *
 * Both fail / hang based on the configured rates so retry, timeout, and the
 * concurrency-release path get exercised.
 */
public final class SimulationPayloads {

    private SimulationPayloads() {}

    public static final class Import implements JobPayload {
        public int projectId;
        public long importDurationMillis;
        public double failureRate;
        public double hangRate;
        public long hangDurationMillis;

        public Import() {}

        public Import(
                int projectId,
                long importDurationMillis,
                double failureRate,
                double hangRate,
                long hangDurationMillis) {
            this.projectId = projectId;
            this.importDurationMillis = importDurationMillis;
            this.failureRate = failureRate;
            this.hangRate = hangRate;
            this.hangDurationMillis = hangDurationMillis;
        }
    }

    public static final class Export implements JobPayload {
        public int projectId;
        public long exportDurationMillis;
        public double failureRate;
        public double hangRate;
        public long hangDurationMillis;

        public Export() {}

        public Export(
                int projectId,
                long exportDurationMillis,
                double failureRate,
                double hangRate,
                long hangDurationMillis) {
            this.projectId = projectId;
            this.exportDurationMillis = exportDurationMillis;
            this.failureRate = failureRate;
            this.hangRate = hangRate;
            this.hangDurationMillis = hangDurationMillis;
        }
    }

    public static final class ImportHandler implements JobHandler<Import> {
        public static final AtomicInteger ATTEMPTS = new AtomicInteger();

        @Override
        public void run(Import payload, JobExecutionContext ctx) throws InterruptedException {
            ATTEMPTS.incrementAndGet();
            double dice = ThreadLocalRandom.current().nextDouble();
            if (dice < payload.failureRate) {
                throw new RuntimeException("simulated import failure for project " + payload.projectId);
            }
            if (dice < payload.failureRate + payload.hangRate) {
                Thread.sleep(payload.hangDurationMillis);
                return;
            }
            Thread.sleep(payload.importDurationMillis);
        }
    }

    public static final class ExportHandler implements JobHandler<Export> {
        public static final AtomicInteger ATTEMPTS = new AtomicInteger();

        @Override
        public void run(Export payload, JobExecutionContext ctx) throws InterruptedException {
            ATTEMPTS.incrementAndGet();
            double dice = ThreadLocalRandom.current().nextDouble();
            if (dice < payload.failureRate) {
                throw new RuntimeException("simulated export failure for project " + payload.projectId);
            }
            if (dice < payload.failureRate + payload.hangRate) {
                Thread.sleep(payload.hangDurationMillis);
                return;
            }
            Thread.sleep(payload.exportDurationMillis);
        }
    }
}
