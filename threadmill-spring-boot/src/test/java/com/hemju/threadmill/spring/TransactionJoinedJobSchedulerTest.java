package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

class TransactionJoinedJobSchedulerTest {

    private InMemoryJobStore store;
    private TransactionJoinedJobScheduler scheduler;
    private CopyOnWriteArrayList<String> wakeCalls;

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
        var wakeBus = new LocalWakeBus();
        wakeCalls = new CopyOnWriteArrayList<>();
        wakeBus.register(wakeCalls::add);
        scheduler = new TransactionJoinedJobScheduler(
                store,
                new JsonJobSerializer(),
                new TestRegistry(),
                ProcessingNodeConfig.builder().build(),
                wakeBus);
    }

    @AfterEach
    void tearDown() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clear();
        }
        TransactionSynchronizationManager.setActualTransactionActive(false);
    }

    @Test
    void enqueueInsideTransactionWritesImmediatelyButWakesAfterCommit() {
        beginTransaction();
        try {
            JobId id = scheduler.enqueue(GreetHandler.class, new GreetPayload("joined"));

            assertThat(store.findById(id)).isPresent();
            assertThat(wakeCalls).isEmpty();

            triggerAfterCommit();
            assertThat(wakeCalls).containsExactly("default");
        } finally {
            TransactionSynchronizationManager.clear();
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    void enqueueAllInsideTransactionWakesAfterCommit() {
        beginTransaction();
        try {
            List<JobId> ids =
                    scheduler.enqueueAll(GreetHandler.class, List.of(new GreetPayload("a"), new GreetPayload("b")));

            assertThat(ids).allSatisfy(id -> assertThat(store.findById(id)).isPresent());
            assertThat(wakeCalls).isEmpty();

            triggerAfterCommit();
            assertThat(wakeCalls).containsExactly("default");
        } finally {
            TransactionSynchronizationManager.clear();
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    void dedupInsideTransactionReturnsCreatedSynchronouslyAndWakesAfterCommit() {
        beginTransaction();
        try {
            EnqueueResult result = scheduler.enqueueIfAbsent(
                    GreetHandler.class, new GreetPayload("dedup"), "tenant-1:greet", Duration.ofMinutes(5));

            assertThat(result).isInstanceOf(EnqueueResult.Created.class);
            assertThat(wakeCalls).isEmpty();

            triggerAfterCommit();
            assertThat(wakeCalls).containsExactly("default");
        } finally {
            TransactionSynchronizationManager.clear();
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    void scheduledEnqueueInsideTransactionDoesNotWake() {
        beginTransaction();
        try {
            scheduler.enqueueIn(GreetHandler.class, new GreetPayload("later"), Duration.ofMinutes(5));
            triggerAfterCommit();

            assertThat(wakeCalls).isEmpty();
        } finally {
            TransactionSynchronizationManager.clear();
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    private static void beginTransaction() {
        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(true);
    }

    private static void triggerAfterCommit() {
        for (TransactionSynchronization s : TransactionSynchronizationManager.getSynchronizations()) {
            s.afterCommit();
        }
    }

    private static final class TestRegistry extends ThreadmillJobRegistry {
        TestRegistry() {
            super(new ThreadmillJobRegistry.Registration(
                    GreetPayload.class, GreetHandler.class, "default", 0, 5, Duration.ofMinutes(5), null));
        }
    }
}
