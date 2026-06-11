package com.hemju.threadmill.core.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobLog;
import com.hemju.threadmill.core.JobMetadata;
import com.hemju.threadmill.core.JobProgress;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.handler.JobExecutionContext;

/**
 * Pins the documented scoped-value contract: plain virtual-thread executors
 * do NOT inherit the binding (only {@code StructuredTaskScope} forks do), and
 * {@link EngineScopedValues#capturing(Runnable)} is the supported way to
 * carry the context across an executor boundary.
 */
class EngineScopedValuesTest {

    @Test
    @Timeout(10)
    void plainVirtualThreadExecutorDoesNotInheritTheBinding() throws Exception {
        var observed = new AtomicReference<Boolean>();
        JobExecutionContext context = fakeContext();

        ScopedValue.where(EngineScopedValues.CURRENT, context).run(() -> {
            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                executor.submit(() -> observed.set(EngineScopedValues.CURRENT.isBound()))
                        .get();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });

        assertThat(observed.get()).isFalse();
    }

    @Test
    @Timeout(10)
    void capturingRebindsTheContextAcrossAnExecutorBoundary() throws Exception {
        var observed = new AtomicReference<JobExecutionContext>();
        JobExecutionContext context = fakeContext();

        ScopedValue.where(EngineScopedValues.CURRENT, context).run(() -> {
            Runnable task = EngineScopedValues.capturing(() -> observed.set(EngineScopedValues.CURRENT.get()));
            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                executor.submit(task).get();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });

        assertThat(observed.get()).isSameAs(context);
    }

    @Test
    void capturingOutsideAnyBindingReturnsTheTaskUnchanged() {
        Runnable task = () -> {};
        assertThat(EngineScopedValues.capturing(task)).isSameAs(task);
    }

    private static JobExecutionContext fakeContext() {
        return new JobExecutionContext() {
            @Override
            public JobId jobId() {
                return JobId.newId();
            }

            @Override
            public NodeId nodeId() {
                return NodeId.newId();
            }

            @Override
            public int attempt() {
                return 1;
            }

            @Override
            public Instant claimedAt() {
                return Instant.now();
            }

            @Override
            public JobLog log() {
                return null;
            }

            @Override
            public JobProgress progress() {
                return null;
            }

            @Override
            public JobMetadata metadata() {
                return null;
            }
        };
    }
}
