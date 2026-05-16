package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobReplacement;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.schedule.Scheduler;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;

/**
 * End-to-end tests for atomic job replacement (transformation).
 *
 * <p>Covers: spec swap end-to-end (new handler runs); state guard
 * (PROCESSING / terminal states refuse); version guard (stale version
 * throws {@link StaleJobException}); queue/priority/scheduledFor swap;
 * vanished id returns {@code false}.
 */
class JobReplacementTest {

    private InMemoryJobStore store;
    private Scheduler scheduler;
    private ProcessingNode node;
    private final JsonJobSerializer serializer = new JsonJobSerializer();

    public static final class P implements JobPayload {
        public String tag;

        public P() {}

        public P(String tag) {
            this.tag = tag;
        }
    }

    public static final class OriginalHandler implements JobHandler<P> {
        public static final ConcurrentLinkedQueue<String> RUNS = new ConcurrentLinkedQueue<>();

        @Override
        public void run(P p, JobExecutionContext c) {
            RUNS.add("ORIG:" + p.tag);
        }
    }

    public static final class ReplacementHandler implements JobHandler<P> {
        public static final ConcurrentLinkedQueue<String> RUNS = new ConcurrentLinkedQueue<>();

        @Override
        public void run(P p, JobExecutionContext c) {
            RUNS.add("NEW:" + p.tag);
        }
    }

    @BeforeEach
    void setUp() {
        store = new InMemoryJobStore();
        scheduler = new Scheduler(store, serializer);
        OriginalHandler.RUNS.clear();
        ReplacementHandler.RUNS.clear();
    }

    @AfterEach
    void tearDown() {
        if (node != null) node.close();
    }

    private ProcessingNodeConfig fast() {
        return ProcessingNodeConfig.builder()
                .workerCount(2)
                .pollInterval(Duration.ofMillis(40))
                .claimHeartbeat(Duration.ofMillis(80))
                .heartbeatTimeout(Duration.ofSeconds(2))
                .jobTimeout(Duration.ofSeconds(2))
                .defaultMaxAttempts(1)
                .retryInitialBackoff(Duration.ofMillis(50))
                .storeOutagePollInterval(Duration.ofMillis(100))
                .build();
    }

    @Test
    void replacingAnEnqueuedJobsSpecCausesTheNewHandlerToRun() {
        JobId id = scheduler.enqueue(new P("hello"), OriginalHandler.class);
        Job loaded = store.findById(id).orElseThrow();

        JobArgument newPayload = serializer.serializePayload(new P("after-replace"));
        var newSpec = new JobSpec(ReplacementHandler.class.getName(), List.of(newPayload));
        boolean replaced = scheduler.replaceSpec(id, loaded.version(), newSpec);
        assertThat(replaced).isTrue();

        // Verify persisted state.
        Job afterReplace = store.findById(id).orElseThrow();
        assertThat(afterReplace.spec().handlerType()).isEqualTo(ReplacementHandler.class.getName());
        assertThat(afterReplace.version()).isEqualTo(loaded.version() + 1);
        assertThat(afterReplace.currentState()).isEqualTo(JobState.ENQUEUED);

        // Now run the engine: only the replacement handler should fire.
        node = ProcessingNode.builder(store).config(fast()).build();
        node.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> ReplacementHandler.RUNS.contains("NEW:after-replace"));
        assertThat(OriginalHandler.RUNS).isEmpty();
    }

    @Test
    void replacingAScheduledJobsTimeMovesItsFireMoment() {
        JobId id = scheduler.scheduleAt(Instant.now().plus(Duration.ofHours(1)), new P("later"), OriginalHandler.class);
        Job loaded = store.findById(id).orElseThrow();

        boolean replaced = scheduler.replace(
                id,
                loaded.version(),
                JobReplacement.builder()
                        .scheduledFor(Instant.now().plusMillis(200))
                        .build());
        assertThat(replaced).isTrue();

        node = ProcessingNode.builder(store).config(fast()).build();
        node.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> OriginalHandler.RUNS.contains("ORIG:later"));
    }

    @Test
    void replacementWithStaleVersionThrowsStaleJobException() {
        JobId id = scheduler.enqueue(new P("a"), OriginalHandler.class);
        Job loaded = store.findById(id).orElseThrow();

        // First replacement succeeds (version bumps).
        assertThat(scheduler.replaceSpec(
                        id,
                        loaded.version(),
                        new JobSpec(
                                ReplacementHandler.class.getName(), List.of(serializer.serializePayload(new P("v2"))))))
                .isTrue();

        // Same expected version now stale.
        assertThatThrownBy(() -> scheduler.replaceSpec(
                        id,
                        loaded.version(),
                        new JobSpec(
                                OriginalHandler.class.getName(), List.of(serializer.serializePayload(new P("v3"))))))
                .isInstanceOf(StaleJobException.class);
    }

    @Test
    void replacementOnAVanishedJobIsANoOpReturningFalse() {
        boolean replaced = scheduler.replaceSpec(
                JobId.newId(),
                1L,
                JobSpec.of(ReplacementHandler.class.getName(), serializer.serializePayload(new P("x"))));
        assertThat(replaced).isFalse();
    }

    @Test
    void replacementOnAProcessingJobIsRejectedReturningFalse() {
        // Enqueue and claim, leaving the job in PROCESSING.
        JobId id = scheduler.enqueue(new P("p"), OriginalHandler.class);
        store.claimReady(NodeId.newId(), "default", 1, Instant.now());
        Job claimed = store.findById(id).orElseThrow();
        assertThat(claimed.currentState()).isEqualTo(JobState.PROCESSING);

        boolean replaced = scheduler.replaceSpec(
                id,
                claimed.version(),
                new JobSpec(ReplacementHandler.class.getName(), List.of(serializer.serializePayload(new P("nope")))));
        assertThat(replaced).isFalse();

        // The job's spec is unchanged.
        Job after = store.findById(id).orElseThrow();
        assertThat(after.spec().handlerType()).isEqualTo(OriginalHandler.class.getName());
        assertThat(after.currentState()).isEqualTo(JobState.PROCESSING);
    }

    @Test
    void replacementCanChangeQueueAndPriority() {
        JobId id = scheduler.enqueue(new P("re"), OriginalHandler.class, "default", 0);
        Job loaded = store.findById(id).orElseThrow();
        boolean replaced = scheduler.replace(
                id,
                loaded.version(),
                JobReplacement.builder().queue("high").priority(9).build());
        assertThat(replaced).isTrue();
        Job after = store.findById(id).orElseThrow();
        assertThat(after.queue()).isEqualTo("high");
        assertThat(after.priority()).isEqualTo(9);
    }
}
