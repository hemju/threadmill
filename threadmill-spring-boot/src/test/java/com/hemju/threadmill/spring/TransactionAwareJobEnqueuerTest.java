package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

/**
 * Drives the {@link TransactionAwareJobEnqueuer} contract directly by
 * managing {@link TransactionSynchronizationManager} from the test. Boots no
 * Spring context — the wrapper only checks
 * {@code isSynchronizationActive()} so the manager is sufficient.
 */
class TransactionAwareJobEnqueuerTest {

    private InMemoryJobStore store;
    private TransactionAwareJobEnqueuer enqueuer;

    public static final class GreetPayload implements JobPayload {
        public String tag;

        public GreetPayload() {}

        public GreetPayload(String tag) {
            this.tag = tag;
        }
    }

    public static final class GreetHandler implements JobHandler<GreetPayload> {
        @Override
        public void run(GreetPayload p, JobExecutionContext c) {}
    }

    @BeforeEach
    void setUp() {
        store = new InMemoryJobStore();
        var serializer = new JsonJobSerializer();
        var registry = new TestRegistry();
        enqueuer = new TransactionAwareJobEnqueuer(
                store, serializer, registry, ProcessingNodeConfig.builder().build());
    }

    @AfterEach
    void tearDown() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clear();
        }
    }

    @Test
    void enqueueOutsideTransactionIsImmediate() {
        JobId id = enqueuer.enqueue(new GreetPayload("immediate"));
        assertThat(store.findById(id)).isPresent();
    }

    @Test
    void enqueueInsideTransactionIsNotVisibleBeforeCommit() {
        TransactionSynchronizationManager.initSynchronization();
        try {
            JobId id = enqueuer.enqueue(new GreetPayload("deferred"));
            assertThat(store.findById(id))
                    .as("row must not exist until afterCommit fires")
                    .isEmpty();
            triggerAfterCommit();
            assertThat(store.findById(id)).isPresent();
        } finally {
            TransactionSynchronizationManager.clear();
        }
    }

    @Test
    void enqueueInsideTransactionIsRolledBackOnRollback() {
        TransactionSynchronizationManager.initSynchronization();
        try {
            JobId id = enqueuer.enqueue(new GreetPayload("rolled-back"));
            assertThat(store.findById(id)).isEmpty();
            // No afterCommit triggered — simulating rollback.
            // The synchronization is dropped on clear() without firing afterCommit.
            TransactionSynchronizationManager.clear();
            assertThat(store.findById(id)).isEmpty();
        } finally {
            if (TransactionSynchronizationManager.isSynchronizationActive()) {
                TransactionSynchronizationManager.clear();
            }
        }
    }

    @Test
    void enqueueAllDefersTheBatchAndCommitsAtomically() {
        TransactionSynchronizationManager.initSynchronization();
        try {
            var ids = enqueuer.enqueueAll(List.of(new GreetPayload("a"), new GreetPayload("b"), new GreetPayload("c")));
            for (JobId id : ids) {
                assertThat(store.findById(id)).isEmpty();
            }
            triggerAfterCommit();
            for (JobId id : ids) {
                assertThat(store.findById(id)).isPresent();
            }
        } finally {
            TransactionSynchronizationManager.clear();
        }
    }

    @Test
    void scheduledEnqueueIsAlsoDeferred() {
        TransactionSynchronizationManager.initSynchronization();
        try {
            JobId id = enqueuer.enqueueIn(new GreetPayload("later"), Duration.ofMinutes(5));
            assertThat(store.findById(id)).isEmpty();
            triggerAfterCommit();
            assertThat(store.findById(id)).isPresent();
        } finally {
            TransactionSynchronizationManager.clear();
        }
    }

    private static void triggerAfterCommit() {
        for (TransactionSynchronization s : TransactionSynchronizationManager.getSynchronizations()) {
            s.afterCommit();
        }
    }

    /** Tiny stand-in for ThreadmillJobRegistry that exposes one handler binding. */
    private static final class TestRegistry extends ThreadmillJobRegistry {
        TestRegistry() {
            super(new ThreadmillJobRegistry.Registration(
                    GreetPayload.class, GreetHandler.class, "default", 0, 5, Duration.ofMinutes(5)));
        }
    }
}
