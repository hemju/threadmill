package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

/**
 * Regression for the 2a4e9d6 registry rewrite, whose runtime
 * {@code isInstance} check silently accepted payload subtypes that have
 * their own registered handler — weakening the previous startup-grade
 * {@code IllegalStateException} to nothing.
 */
class JobSchedulerSubtypeRoutingTest {

    public static class BasePayload implements JobPayload {
        public String tag;

        public BasePayload() {}

        public BasePayload(String tag) {
            this.tag = tag;
        }
    }

    public static final class SpecialPayload extends BasePayload {
        public SpecialPayload() {}

        public SpecialPayload(String tag) {
            super(tag);
        }
    }

    public static final class BaseHandler implements JobHandler<BasePayload> {
        @Override
        public void run(BasePayload p, JobExecutionContext c) {}
    }

    public static final class SpecialHandler implements JobHandler<SpecialPayload> {
        @Override
        public void run(SpecialPayload p, JobExecutionContext c) {}
    }

    private InMemoryJobStore store;
    private JobScheduler enqueuer;

    @BeforeEach
    void setUp() {
        store = new InMemoryJobStore();
        enqueuer = new JobScheduler(
                store,
                new JsonJobSerializer(),
                new TwoLevelRegistry(),
                ProcessingNodeConfig.builder().build());
    }

    @Test
    void payloadSubtypeWithItsOwnHandlerIsRejectedWithBothHandlerNames() {
        assertThatThrownBy(() -> enqueuer.enqueue(BaseHandler.class, new SpecialPayload("q")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(SpecialHandler.class.getName())
                .hasMessageContaining(BaseHandler.class.getName());
    }

    @Test
    void theSubtypeRoutesToItsOwnHandler() {
        JobId id = enqueuer.enqueue(SpecialHandler.class, new SpecialPayload("q"));
        assertThat(store.findById(id).orElseThrow().spec().handlerType()).isEqualTo(SpecialHandler.class.getName());
    }

    @Test
    void theExactBasePayloadStillRoutesToTheBaseHandler() {
        JobId id = enqueuer.enqueue(BaseHandler.class, new BasePayload("p"));
        assertThat(store.findById(id).orElseThrow().spec().handlerType()).isEqualTo(BaseHandler.class.getName());
    }

    /** Test seam: base handler for the supertype, special handler for the subtype. */
    private static final class TwoLevelRegistry extends ThreadmillJobRegistry {
        TwoLevelRegistry() {
            super(
                    new ThreadmillJobRegistry.Registration(
                            BasePayload.class, BaseHandler.class, "default", 0, 5, Duration.ofMinutes(5), null),
                    new ThreadmillJobRegistry.Registration(
                            SpecialPayload.class, SpecialHandler.class, "default", 0, 5, Duration.ofMinutes(5), null));
        }
    }
}
