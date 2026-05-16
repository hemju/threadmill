package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobRelationship;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.engine.Dispatcher;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.engine.RetryInterceptor;
import com.hemju.threadmill.core.engine.RetryPolicy;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.schedule.Scheduler;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;

class AdvancedFeatureTest {

    private InMemoryJobStore store;
    private Scheduler scheduler;
    private ProcessingNode node;
    private final JsonJobSerializer serializer = new JsonJobSerializer();

    public static final class HelloPayload implements JobPayload {
        public String tag;

        public HelloPayload() {}

        public HelloPayload(String tag) {
            this.tag = tag;
        }
    }

    public static final class ResultProducingHandler implements JobHandler<HelloPayload> {
        public static final AtomicReference<String> LAST_RESULT = new AtomicReference<>();

        @Override
        public void run(HelloPayload p, JobExecutionContext c) {
            c.setResult("answer:" + p.tag);
            LAST_RESULT.set("answer:" + p.tag);
        }
    }

    public static final class SuccessorHandler implements JobHandler<HelloPayload> {
        public static final ConcurrentLinkedQueue<String> RAN_FOR = new ConcurrentLinkedQueue<>();

        @Override
        public void run(HelloPayload p, JobExecutionContext c) {
            RAN_FOR.add(p.tag);
        }
    }

    public static final class AlwaysFailHandler implements JobHandler<HelloPayload> {
        public static final AtomicInteger ATTEMPTS = new AtomicInteger();

        @Override
        public void run(HelloPayload p, JobExecutionContext c) {
            ATTEMPTS.incrementAndGet();
            throw new IllegalStateException("boom");
        }
    }

    public static final class MutexHandler implements JobHandler<HelloPayload> {
        public static final AtomicInteger ACTIVE = new AtomicInteger();
        public static final AtomicInteger MAX_OBSERVED = new AtomicInteger();

        @Override
        public void run(HelloPayload p, JobExecutionContext c) throws InterruptedException {
            int a = ACTIVE.incrementAndGet();
            MAX_OBSERVED.accumulateAndGet(a, Math::max);
            try {
                Thread.sleep(50);
            } finally {
                ACTIVE.decrementAndGet();
            }
        }
    }

    public static final class NoopHandler implements JobHandler<HelloPayload> {
        public static final AtomicInteger COUNT = new AtomicInteger();

        @Override
        public void run(HelloPayload p, JobExecutionContext c) {
            COUNT.incrementAndGet();
        }
    }

    @BeforeEach
    void setUp() {
        store = new InMemoryJobStore();
        scheduler = new Scheduler(store, serializer);
        ResultProducingHandler.LAST_RESULT.set(null);
        SuccessorHandler.RAN_FOR.clear();
        AlwaysFailHandler.ATTEMPTS.set(0);
        MutexHandler.ACTIVE.set(0);
        MutexHandler.MAX_OBSERVED.set(0);
        NoopHandler.COUNT.set(0);
    }

    @AfterEach
    void tearDown() {
        if (node != null) node.close();
    }

    private ProcessingNodeConfig fast() {
        return ProcessingNodeConfig.builder()
                .workerCount(4)
                .pollInterval(Duration.ofMillis(30))
                .claimHeartbeat(Duration.ofMillis(60))
                .heartbeatTimeout(Duration.ofSeconds(2))
                .jobTimeout(Duration.ofSeconds(2))
                .defaultMaxAttempts(2)
                .retryInitialBackoff(Duration.ofMillis(50))
                .storeOutagePollInterval(Duration.ofMillis(100))
                .build();
    }

    // ---------------- Results

    @Test
    void aHandlerCanSetAResultAndItPersistsOnSuccess() {
        JobId id = scheduler.enqueue(new HelloPayload("q"), ResultProducingHandler.class);
        node = ProcessingNode.builder(store).config(fast()).build();
        node.start();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Job loaded = store.findById(id).orElseThrow();
            assertThat(loaded.currentState()).isEqualTo(JobState.SUCCEEDED);
            assertThat(loaded.result()).isPresent();
            assertThat(loaded.result().get().serialized()).isEqualTo("\"answer:q\"");
        });
    }

    // ---------------- Workflow / chaining

    @Test
    void aWorkflowSuccessorIsPromotedWhenItsPredecessorSucceeds() {
        JobId parentId = scheduler.enqueue(new HelloPayload("p"), ResultProducingHandler.class);

        // Build the successor in AWAITING with a relationship pointing to the parent.
        JobArgument arg = serializer.serializePayload(new HelloPayload("s"));
        Job successor = Job.builder()
                .spec(new JobSpec(SuccessorHandler.class.getName(), List.of(arg)))
                .initialState(JobState.AWAITING)
                .relationship(new JobRelationship(parentId, JobRelationship.Kind.WORKFLOW_STEP))
                .build();
        store.insert(successor);

        node = ProcessingNode.builder(store).config(fast()).build();
        node.start();

        await().atMost(Duration.ofSeconds(5)).until(() -> SuccessorHandler.RAN_FOR.contains("s"));
    }

    // ---------------- Custom retry policies (precedence matrix)

    @Test
    void perJobOverrideBeatsPerExceptionTypePolicyBeatsGlobalDefault() {
        // 1. Global default = 2 attempts.
        // 2. Per-exception-type for IllegalStateException = 5.
        // 3. Per-job override via metadata = 1.
        // Expect: 1 attempt for the override job; 5 for the exception-policy job;
        // 2 for a plain failing job (default).

        JobArgument arg = serializer.serializePayload(new HelloPayload("a"));
        Job override = Job.builder()
                .spec(new JobSpec(AlwaysFailHandler.class.getName(), List.of(arg)))
                .metadata(RetryInterceptor.META_MAX_ATTEMPTS, "1")
                .build();
        Job exceptionPolicy = Job.builder()
                .spec(new JobSpec(AlwaysFailHandler.class.getName(), List.of(arg)))
                .build();
        Job plain = Job.builder()
                .spec(new JobSpec(AlwaysFailHandler.class.getName(), List.of(arg)))
                .build();

        ProcessingNodeConfig fast = fast().toBuilder().defaultMaxAttempts(2).build();
        node = ProcessingNode.builder(store)
                .config(fast)
                .retryPolicyFor(IllegalStateException.class, RetryPolicy.of(5, Duration.ofMillis(20)))
                .build();
        // For the override-meta and per-exception jobs, also tag them with no metadata.
        // We will only insert/start one at a time to isolate counts.

        // Run override:
        AlwaysFailHandler.ATTEMPTS.set(0);
        store.insert(override);
        node.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            Job loaded = store.findById(override.id()).orElseThrow();
            return loaded.currentState() == JobState.FAILED && loaded.attempts() >= 1;
        });
        assertThat(AlwaysFailHandler.ATTEMPTS.get()).isEqualTo(1);
        node.close();
        node = null;

        // Run exceptionPolicy:
        AlwaysFailHandler.ATTEMPTS.set(0);
        store.insert(exceptionPolicy);
        node = ProcessingNode.builder(store)
                .config(fast)
                .retryPolicyFor(IllegalStateException.class, RetryPolicy.of(5, Duration.ofMillis(20)))
                .build();
        node.start();
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            Job loaded = store.findById(exceptionPolicy.id()).orElseThrow();
            return loaded.currentState() == JobState.FAILED && loaded.attempts() >= 5;
        });
        assertThat(AlwaysFailHandler.ATTEMPTS.get()).isEqualTo(5);
        node.close();
        node = null;

        // Run plain (default = 2):
        AlwaysFailHandler.ATTEMPTS.set(0);
        store.insert(plain);
        // No exception policy this time so default applies.
        node = ProcessingNode.builder(store).config(fast).build();
        node.start();
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            Job loaded = store.findById(plain.id()).orElseThrow();
            return loaded.currentState() == JobState.FAILED && loaded.attempts() >= 2;
        });
        assertThat(AlwaysFailHandler.ATTEMPTS.get()).isEqualTo(2);
    }

    // ---------------- Mutex

    @Test
    void mutexBlocksConcurrentAcquireFromAnotherHolder() {
        assertThat(store.tryAcquireMutex("m", "node-A", Duration.ofSeconds(5))).isTrue();
        assertThat(store.tryAcquireMutex("m", "node-B", Duration.ofSeconds(5))).isFalse();
        store.releaseMutex("m", "node-A");
        assertThat(store.tryAcquireMutex("m", "node-B", Duration.ofSeconds(5))).isTrue();
    }

    @Test
    void mutexAllowsReentrantAcquireBySameHolder() {
        assertThat(store.tryAcquireMutex("m", "node-A", Duration.ofSeconds(5))).isTrue();
        assertThat(store.tryAcquireMutex("m", "node-A", Duration.ofSeconds(5))).isTrue();
    }

    @Test
    void mutexExpiresSoADeadHolderDoesNotBlockForever() throws InterruptedException {
        assertThat(store.tryAcquireMutex("m", "dead", Duration.ofMillis(50))).isTrue();
        Thread.sleep(200); // 4× the lease — generous slack for loaded CI
        assertThat(store.tryAcquireMutex("m", "alive", Duration.ofSeconds(5))).isTrue();
    }

    // ---------------- Node tags

    @Test
    void untaggedJobRunsOnUntaggedNode() {
        JobArgument arg = serializer.serializePayload(new HelloPayload("ok"));
        Job untagged = Job.builder()
                .spec(new JobSpec(NoopHandler.class.getName(), List.of(arg)))
                .build();
        store.insert(untagged);
        node = ProcessingNode.builder(store).config(fast()).build();
        node.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> NoopHandler.COUNT.get() == 1);
    }

    @Test
    void taggedJobIsNotRunByUntaggedNodeAndIsLaterRunByTaggedNode() {
        JobArgument arg = serializer.serializePayload(new HelloPayload("ok"));
        Job tagged = Job.builder()
                .spec(new JobSpec(NoopHandler.class.getName(), List.of(arg)))
                .metadata(Dispatcher.REQUIRED_TAGS_META, "gpu")
                .build();
        store.insert(tagged);

        // Untagged node: must NOT run it.
        node = ProcessingNode.builder(store).config(fast()).build();
        node.start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        assertThat(NoopHandler.COUNT.get()).isZero();
        node.close();
        node = null;

        // Bring up a gpu-tagged node — the tagged job should now run.
        // First nudge the job back to ENQUEUED so the test isn't waiting 2s for the
        // tag-mismatch backoff to expire on its own.
        Job park = store.findById(tagged.id()).orElseThrow();
        long v = park.version();
        if (park.currentState() == JobState.SCHEDULED) {
            park.transitionTo(JobState.ENQUEUED, java.time.Instant.now(), "test.nudge", null);
            park.clearScheduledFor();
            store.saveAtomic(park, v);
        }
        node = ProcessingNode.builder(store).config(fast()).tag("gpu").build();
        node.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> NoopHandler.COUNT.get() == 1);
    }
}
