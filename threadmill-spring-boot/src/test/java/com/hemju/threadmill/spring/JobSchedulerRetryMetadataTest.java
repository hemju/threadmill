package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.engine.RetryInterceptor;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

/**
 * Enqueue-time retry-metadata stamping (github issue #104): only an explicit
 * {@code @Job(maxAttempts)} may stamp per-job retry metadata. Per-job metadata
 * outranks per-exception-type retry policies, so stamping the resolved default
 * on every job would permanently shadow those policies for every
 * Spring-enqueued job.
 */
class JobSchedulerRetryMetadataTest {

    private InMemoryJobStore store;
    private JobScheduler scheduler;

    public static final class CappedPayload implements JobPayload {
        public String tag = "capped";
    }

    public static final class CappedHandler implements JobHandler<CappedPayload> {
        @Override
        public void run(CappedPayload p, JobExecutionContext c) {}
    }

    public static final class UncappedPayload implements JobPayload {
        public String tag = "uncapped";
    }

    public static final class UncappedHandler implements JobHandler<UncappedPayload> {
        @Override
        public void run(UncappedPayload p, JobExecutionContext c) {}
    }

    private static final class TestRegistry extends ThreadmillJobRegistry {
        TestRegistry() {
            super(
                    new ThreadmillJobRegistry.Registration(
                            CappedPayload.class, CappedHandler.class, "default", 0, 4, Duration.ofMinutes(5), null),
                    new ThreadmillJobRegistry.Registration(
                            UncappedPayload.class,
                            UncappedHandler.class,
                            "default",
                            0,
                            null,
                            Duration.ofMinutes(5),
                            null));
        }
    }

    @BeforeEach
    void setUp() {
        store = new InMemoryJobStore();
        scheduler = new JobScheduler(
                store,
                new JsonJobSerializer(),
                new TestRegistry(),
                ProcessingNodeConfig.builder().build());
    }

    @Test
    void explicitMaxAttemptsIsStampedAsPerJobRetryMetadata() {
        JobId id = scheduler.enqueue(CappedHandler.class, new CappedPayload());
        var job = store.findById(id).orElseThrow();
        assertThat(job.metadata().get(RetryInterceptor.META_MAX_ATTEMPTS)).contains("4");
    }

    @Test
    void defaultRetryBudgetLeavesRetryMetadataUnstamped() {
        // With no explicit @Job(maxAttempts) the job carries no per-job retry
        // metadata: the RetryInterceptor's per-exception-type policies and
        // global default stay reachable for it.
        JobId id = scheduler.enqueue(UncappedHandler.class, new UncappedPayload());
        var job = store.findById(id).orElseThrow();
        assertThat(job.metadata().get(RetryInterceptor.META_MAX_ATTEMPTS)).isEmpty();
    }
}
