package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.JobStateEntry;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.Dispatcher;
import com.hemju.threadmill.core.engine.JobInterceptor;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.engine.QueueLane;
import com.hemju.threadmill.core.engine.QueueWeights;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.serialization.SerializationException;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.test.ForwardingJobStore;

/**
 * End-to-end engine tests: enqueue a job, the dispatcher claims and runs
 * it, the success / failure / timeout / orphan / poison paths all funnel
 * through the single {@code onProcessingFailed} interceptor hook.
 */
class ProcessingNodeTest {

    private InMemoryJobStore store;
    private ProcessingNode node;
    private ProcessingNode secondNode;
    private ProcessingNodeConfig fastConfig;
    private final JsonJobSerializer serializer = new JsonJobSerializer();

    @Test
    void closeBeforeStartMakesLaterStartASilentNoOp() {
        node = ProcessingNode.builder(store).config(fastConfig).build();

        node.close();
        var heartbeatsAfterClose = store.listNodeHeartbeats();
        node.start();

        assertThat(node.isStopped()).isTrue();
        assertThat(store.listNodeHeartbeats()).isEqualTo(heartbeatsAfterClose);
    }

    @BeforeEach
    void setUp() {
        EngineTestHandlers.reset();
        store = new InMemoryJobStore();
        fastConfig = ProcessingNodeConfig.builder()
                .workerCount(4)
                .pollInterval(Duration.ofMillis(50))
                .claimHeartbeat(Duration.ofMillis(100))
                .heartbeatTimeout(Duration.ofMillis(500))
                .maintenanceLeaseDuration(Duration.ofMillis(500))
                .jobTimeout(Duration.ofSeconds(2))
                .defaultMaxAttempts(3)
                .retryInitialBackoff(Duration.ofMillis(50))
                .storeOutagePollInterval(Duration.ofMillis(100))
                .build();
    }

    @AfterEach
    void tearDown() {
        if (node != null) node.close();
        if (secondNode != null) secondNode.close();
    }

    private Job enqueueHello(Class<?> handlerClass, String queue) {
        return enqueueHello(handlerClass, queue, null, null);
    }

    private Job enqueueHello(Class<?> handlerClass, String queue, String concurrencyKey, ConcurrencyMode mode) {
        JobArgument arg = serializer.serializePayload(new EngineTestHandlers.HelloPayload("test"));
        Job j = Job.builder()
                .spec(new JobSpec(handlerClass.getName(), List.of(arg)))
                .queue(queue)
                .concurrencyKey(concurrencyKey)
                .concurrencyMode(mode)
                .build();
        store.insert(j);
        return j;
    }

    @Test
    void runsAJobToCompletionAndMovesItToSucceeded() {
        Job job = enqueueHello(EngineTestHandlers.CountingHandler.class, fastConfig.defaultQueue());
        node = ProcessingNode.builder(store).config(fastConfig).build();
        node.start();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Job loaded = store.findById(job.id()).orElseThrow();
            assertThat(loaded.currentState()).isEqualTo(JobState.SUCCEEDED);
        });
        assertThat(EngineTestHandlers.CountingHandler.COUNT)
                .containsKey(job.id().toString());
    }

    @Test
    void retriesAFailingJobUpToTheConfiguredAttempts() {
        Job job = enqueueHello(EngineTestHandlers.FailingHandler.class, fastConfig.defaultQueue());
        node = ProcessingNode.builder(store).config(fastConfig).build();
        node.start();

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(EngineTestHandlers.FailingHandler.ATTEMPTS.get())
                        .isGreaterThanOrEqualTo(fastConfig.defaultMaxAttempts()));
        // Final state is FAILED after the budget is exhausted.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Job loaded = store.findById(job.id()).orElseThrow();
            assertThat(loaded.currentState()).isIn(JobState.FAILED, JobState.SCHEDULED);
            assertThat(loaded.attempts()).isGreaterThanOrEqualTo(fastConfig.defaultMaxAttempts());
        });
    }

    @Test
    void interruptsAHungJobAndRoutesItThroughTheSameFailurePath() {
        var failureKinds = new CopyOnWriteArrayList<JobInterceptor.FailureCause>();
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder()
                        .jobTimeout(Duration.ofMillis(300))
                        .defaultMaxAttempts(1) // no retries — go straight to FAILED
                        .build())
                .interceptor(new JobInterceptor() {
                    @Override
                    public void onProcessingFailed(Job j, JobExecutionContext c, Throwable cause, FailureCause kind) {
                        failureKinds.add(kind);
                    }
                })
                .build();
        Job job = enqueueHello(EngineTestHandlers.HangingHandler.class, fastConfig.defaultQueue());
        node.start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(EngineTestHandlers.HangingHandler.INTERRUPTS.get()).isGreaterThan(0);
            assertThat(failureKinds).contains(JobInterceptor.FailureCause.TIMEOUT);
            Job loaded = store.findById(job.id()).orElseThrow();
            assertThat(loaded.currentState()).isEqualTo(JobState.FAILED);
        });
    }

    @Test
    void successfulConcurrencyJobReleasesKeyForNextJob() {
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder().defaultMaxAttempts(1).build())
                .build();
        Job first = enqueueHello(
                EngineTestHandlers.CountingHandler.class,
                fastConfig.defaultQueue(),
                "project:release-success",
                ConcurrencyMode.EXCLUSIVE);
        pauseForOrdering();
        Job second = enqueueHello(
                EngineTestHandlers.CountingHandler.class,
                fastConfig.defaultQueue(),
                "project:release-success",
                ConcurrencyMode.EXCLUSIVE);
        node.start();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(store.findById(first.id()).orElseThrow().currentState()).isEqualTo(JobState.SUCCEEDED);
            assertThat(store.findById(second.id()).orElseThrow().currentState()).isEqualTo(JobState.SUCCEEDED);
        });
    }

    @Test
    void exceptionFailureReleasesConcurrencyKeyForNextJob() {
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder().defaultMaxAttempts(1).build())
                .build();
        Job first = enqueueHello(
                EngineTestHandlers.FailingHandler.class,
                fastConfig.defaultQueue(),
                "project:release-exception",
                ConcurrencyMode.EXCLUSIVE);
        pauseForOrdering();
        Job second = enqueueHello(
                EngineTestHandlers.CountingHandler.class,
                fastConfig.defaultQueue(),
                "project:release-exception",
                ConcurrencyMode.EXCLUSIVE);
        node.start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(store.findById(first.id()).orElseThrow().currentState()).isEqualTo(JobState.FAILED);
            assertThat(store.findById(second.id()).orElseThrow().currentState()).isEqualTo(JobState.SUCCEEDED);
        });
    }

    @Test
    void timeoutFailureReleasesConcurrencyKeyForNextJob() {
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder()
                        .jobTimeout(Duration.ofMillis(250))
                        .defaultMaxAttempts(1)
                        .build())
                .build();
        Job first = enqueueHello(
                EngineTestHandlers.HangingHandler.class,
                fastConfig.defaultQueue(),
                "project:release-timeout",
                ConcurrencyMode.EXCLUSIVE);
        pauseForOrdering();
        Job second = enqueueHello(
                EngineTestHandlers.CountingHandler.class,
                fastConfig.defaultQueue(),
                "project:release-timeout",
                ConcurrencyMode.EXCLUSIVE);
        node.start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(store.findById(first.id()).orElseThrow().currentState()).isEqualTo(JobState.FAILED);
            assertThat(store.findById(second.id()).orElseThrow().currentState()).isEqualTo(JobState.SUCCEEDED);
        });
    }

    @Test
    void noProgressTimeoutReleasesConcurrencyKeyForNextJob() {
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder()
                        .jobTimeout(Duration.ofMillis(200))
                        .noProgressTimeout(Duration.ofMillis(300))
                        .checkInMinInterval(Duration.ofMillis(25))
                        .defaultMaxAttempts(1)
                        .build())
                .build();
        Job first = enqueueHello(
                EngineTestHandlers.StalledAfterCheckInHandler.class,
                fastConfig.defaultQueue(),
                "project:release-no-progress",
                ConcurrencyMode.EXCLUSIVE);
        pauseForOrdering();
        Job second = enqueueHello(
                EngineTestHandlers.CountingHandler.class,
                fastConfig.defaultQueue(),
                "project:release-no-progress",
                ConcurrencyMode.EXCLUSIVE);
        node.start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(store.findById(first.id()).orElseThrow().currentState()).isEqualTo(JobState.FAILED);
            assertThat(store.findById(second.id()).orElseThrow().currentState()).isEqualTo(JobState.SUCCEEDED);
        });
    }

    @Test
    void quarantineReleasesConcurrencyKeyForNextJob() {
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder().defaultMaxAttempts(1).build())
                .build();
        Job poison = Job.builder()
                .spec(JobSpec.of("com.example.DoesNotExist", new JobArgument("com.example.DoesNotExist", "{}")))
                .concurrencyKey("project:release-quarantine")
                .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
                .build();
        store.insert(poison);
        pauseForOrdering();
        Job second = enqueueHello(
                EngineTestHandlers.CountingHandler.class,
                fastConfig.defaultQueue(),
                "project:release-quarantine",
                ConcurrencyMode.EXCLUSIVE);
        node.start();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(store.findById(poison.id()).orElseThrow().currentState()).isEqualTo(JobState.QUARANTINED);
            assertThat(store.findById(second.id()).orElseThrow().currentState()).isEqualTo(JobState.SUCCEEDED);
        });
    }

    @Test
    void regularCheckInsLetLongRunningJobsOutliveWallClockTimeout() {
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder()
                        .jobTimeout(Duration.ofMillis(200))
                        .noProgressTimeout(Duration.ofSeconds(2))
                        .checkInMinInterval(Duration.ofMillis(50))
                        .defaultMaxAttempts(1)
                        .build())
                .build();
        Job job = enqueueHello(EngineTestHandlers.CheckInHandler.class, fastConfig.defaultQueue());
        node.start();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Job loaded = store.findById(job.id()).orElseThrow();
            assertThat(loaded.currentState()).isEqualTo(JobState.SUCCEEDED);
            assertThat(loaded.lastCheckinAt()).isPresent();
            assertThat(loaded.progress().snapshot()).hasValueSatisfying(progress -> {
                assertThat(progress.fraction()).isEqualTo(1.0);
            });
            assertThat(EngineTestHandlers.CheckInHandler.COMPLETIONS.get()).isEqualTo(1);
        });
    }

    @Test
    void stalledCheckInJobFailsAfterNoProgressTimeout() {
        var failureKinds = new CopyOnWriteArrayList<JobInterceptor.FailureCause>();
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder()
                        .jobTimeout(Duration.ofMillis(200))
                        .noProgressTimeout(Duration.ofMillis(350))
                        .checkInMinInterval(Duration.ofMillis(25))
                        .defaultMaxAttempts(1)
                        .build())
                .interceptor(new JobInterceptor() {
                    @Override
                    public void onProcessingFailed(Job j, JobExecutionContext c, Throwable cause, FailureCause kind) {
                        failureKinds.add(kind);
                    }
                })
                .build();
        Job job = enqueueHello(EngineTestHandlers.StalledAfterCheckInHandler.class, fastConfig.defaultQueue());
        node.start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(EngineTestHandlers.StalledAfterCheckInHandler.INTERRUPTS.get())
                    .isGreaterThan(0);
            assertThat(failureKinds).contains(JobInterceptor.FailureCause.TIMEOUT);
            Job loaded = store.findById(job.id()).orElseThrow();
            assertThat(loaded.currentState()).isEqualTo(JobState.FAILED);
            assertThat(loaded.lastCheckinAt()).isPresent();
        });
    }

    @Test
    void poisonJobIsQuarantinedWithoutCrashingTheLoop() {
        var quarantineHits = new AtomicInteger();
        node = ProcessingNode.builder(store)
                .config(fastConfig)
                .interceptor(new JobInterceptor() {
                    @Override
                    public void onProcessingFailed(Job j, JobExecutionContext c, Throwable cause, FailureCause kind) {
                        if (kind == FailureCause.QUARANTINE) quarantineHits.incrementAndGet();
                    }
                })
                .build();

        // Build a job whose handler type does not exist.
        Job poison = Job.builder()
                .spec(JobSpec.of("com.example.DoesNotExist", new JobArgument("com.example.DoesNotExist", "{}")))
                .build();
        store.insert(poison);
        // And a job that runs successfully — the loop must keep working.
        Job good = enqueueHello(EngineTestHandlers.CountingHandler.class, fastConfig.defaultQueue());
        node.start();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Job loadedPoison = store.findById(poison.id()).orElseThrow();
            Job loadedGood = store.findById(good.id()).orElseThrow();
            assertThat(loadedPoison.currentState()).isEqualTo(JobState.QUARANTINED);
            assertThat(loadedGood.currentState()).isEqualTo(JobState.SUCCEEDED);
            assertThat(quarantineHits.get()).isPositive();
        });
    }

    @Test
    void orphanReclaimRoutesThroughTheSameFailurePathAndFiresOnProcessingFailed() {
        // Pre-populate a PROCESSING job with a stale heartbeat (i.e., its owner died long ago).
        var failureKinds = new CopyOnWriteArrayList<JobInterceptor.FailureCause>();
        Job j = Job.builder()
                .spec(new JobSpec(
                        EngineTestHandlers.CountingHandler.class.getName(),
                        List.of(serializer.serializePayload(new EngineTestHandlers.HelloPayload("x")))))
                .build();
        store.insert(j);
        // Claim it from another node, with an expired heartbeat in the past.
        store.claimReady(
                NodeId.newId(), fastConfig.defaultQueue(), 1, Instant.now().minus(Duration.ofHours(2)));

        node = ProcessingNode.builder(store)
                .config(fastConfig)
                .interceptor(new JobInterceptor() {
                    @Override
                    public void onProcessingFailed(Job job, JobExecutionContext c, Throwable cause, FailureCause kind) {
                        failureKinds.add(kind);
                    }
                })
                .build();
        node.start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(failureKinds).contains(JobInterceptor.FailureCause.ORPHAN_RECLAIM);
            Job loaded = store.findById(j.id()).orElseThrow();
            // Either back in SCHEDULED (retried) or terminally FAILED.
            assertThat(loaded.currentState())
                    .isIn(
                            JobState.SCHEDULED,
                            JobState.FAILED,
                            JobState.ENQUEUED,
                            JobState.PROCESSING,
                            JobState.SUCCEEDED);
            assertThat(loaded.stateHistory())
                    .anySatisfy(e -> assertThat(e.reason()).isEqualTo("engine.orphan-reclaim"));
        });
    }

    @Test
    void orphanReclaimReleasesConcurrencyKeyForNextJob() {
        Job first = Job.builder()
                .spec(new JobSpec(
                        EngineTestHandlers.CountingHandler.class.getName(),
                        List.of(serializer.serializePayload(new EngineTestHandlers.HelloPayload("x")))))
                .concurrencyKey("project:release-orphan")
                .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
                .build();
        store.insert(first);
        store.claimReady(
                NodeId.newId(), fastConfig.defaultQueue(), 1, Instant.now().minus(Duration.ofHours(2)));
        pauseForOrdering();
        Job second = enqueueHello(
                EngineTestHandlers.CountingHandler.class,
                fastConfig.defaultQueue(),
                "project:release-orphan",
                ConcurrencyMode.EXCLUSIVE);

        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder().defaultMaxAttempts(1).build())
                .build();
        node.start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(store.findById(first.id()).orElseThrow().currentState()).isEqualTo(JobState.FAILED);
            assertThat(store.findById(second.id()).orElseThrow().currentState()).isEqualTo(JobState.SUCCEEDED);
        });
    }

    @Test
    void scheduledJobsArePromotedAndRun() {
        Job job = Job.builder()
                .spec(new JobSpec(
                        EngineTestHandlers.CountingHandler.class.getName(),
                        List.of(serializer.serializePayload(new EngineTestHandlers.HelloPayload("x")))))
                .initialState(JobState.SCHEDULED)
                .scheduledFor(Instant.now().plusMillis(200))
                .createdAt(Instant.now())
                .build();
        store.insert(job);

        node = ProcessingNode.builder(store).config(fastConfig).build();
        node.start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job loaded = store.findById(job.id()).orElseThrow();
            assertThat(loaded.currentState()).isEqualTo(JobState.SUCCEEDED);
        });
    }

    @Test
    void onlyOneNodeHoldsTheMaintenanceLeaseAndLeadershipTransfersOnClose() {
        ProcessingNodeConfig config = fastConfig.toBuilder()
                .claimHeartbeat(Duration.ofMillis(80))
                .maintenanceLeaseDuration(Duration.ofMillis(300))
                .build();
        node = ProcessingNode.builder(store).config(config).build();
        secondNode = ProcessingNode.builder(store).config(config).build();
        node.start();
        secondNode.start();

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(
                        () -> assertThat(store.readMaintenanceLeaseOwner()).isPresent());
        NodeId firstOwner = store.readMaintenanceLeaseOwner().orElseThrow();
        assertThat(firstOwner).isIn(node.nodeId(), secondNode.nodeId());

        if (firstOwner.equals(node.nodeId())) {
            node.close();
            node = null;
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(
                            () -> assertThat(store.readMaintenanceLeaseOwner()).contains(secondNode.nodeId()));
        } else {
            secondNode.close();
            secondNode = null;
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(
                            () -> assertThat(store.readMaintenanceLeaseOwner()).contains(node.nodeId()));
        }
    }

    @Test
    void maintenanceRemovesStaleNodeHeartbeats() {
        NodeId stale = NodeId.newId();
        store.recordNodeHeartbeat(stale, Instant.now().minus(Duration.ofSeconds(5)));
        ProcessingNodeConfig config = fastConfig.toBuilder()
                .claimHeartbeat(Duration.ofMillis(50))
                .heartbeatTimeout(Duration.ofMillis(200))
                .maintenanceLeaseDuration(Duration.ofMillis(300))
                .nodeHeartbeatRetention(Duration.ofMillis(400))
                .build();
        node = ProcessingNode.builder(store).config(config).build();
        node.start();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(store.readNodeHeartbeat(stale)).isEmpty();
            assertThat(store.listNodeHeartbeats()).extracting(h -> h.nodeId()).doesNotContain(stale);
        });
    }

    @Test
    void queueFamilyLaneDrainsMatchingQueuesOnly() {
        Job project42 = enqueueHello(EngineTestHandlers.CountingHandler.class, "project:42");
        Job projectX = enqueueHello(EngineTestHandlers.CountingHandler.class, "project:x");
        Job other = enqueueHello(EngineTestHandlers.CountingHandler.class, "default");
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder()
                        .queueFamilyDiscoveryInterval(Duration.ofMillis(50))
                        .queueFamilyRetentionAfterEmpty(Duration.ofMillis(200))
                        .build())
                .lane("project:*", 2, QueueWeights.uniform())
                .build();
        node.start();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(store.findById(project42.id()).orElseThrow().currentState())
                    .isEqualTo(JobState.SUCCEEDED);
            assertThat(store.findById(projectX.id()).orElseThrow().currentState())
                    .isEqualTo(JobState.SUCCEEDED);
        });
        assertThat(store.findById(other.id()).orElseThrow().currentState()).isEqualTo(JobState.ENQUEUED);
    }

    @Test
    void queueFamilyZeroWeightQueueIsNotDrained() {
        Job paused = enqueueHello(EngineTestHandlers.CountingHandler.class, "project:42");
        Job active = enqueueHello(EngineTestHandlers.CountingHandler.class, "project:43");
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder()
                        .queueFamilyDiscoveryInterval(Duration.ofMillis(50))
                        .queueFamilyRetentionAfterEmpty(Duration.ofMillis(200))
                        .build())
                .lane("project:*", 2, QueueWeights.fromMap(Map.of("project:42", 0, "project:43", 1)))
                .build();
        node.start();

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(
                                store.findById(active.id()).orElseThrow().currentState())
                        .isEqualTo(JobState.SUCCEEDED));
        assertThat(store.findById(paused.id()).orElseThrow().currentState()).isEqualTo(JobState.ENQUEUED);
    }

    @Test
    void queueFamilyUniformWeightsSpreadFirstClaimsAcrossMatchingQueues() {
        Map<String, List<Job>> jobsByQueue = new LinkedHashMap<>();
        for (int queue = 0; queue < 10; queue++) {
            String queueName = "project:" + queue;
            var jobs = new ArrayList<Job>();
            for (int job = 0; job < 3; job++) {
                jobs.add(enqueueHello(EngineTestHandlers.BlockingHandler.class, queueName));
            }
            jobsByQueue.put(queueName, jobs);
        }
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder()
                        .claimBatchSize(1)
                        .jobTimeout(Duration.ofSeconds(30))
                        .shutdownGracePeriod(Duration.ofMillis(50))
                        .queueFamilyDiscoveryInterval(Duration.ofMillis(50))
                        .queueFamilyRetentionAfterEmpty(Duration.ofMillis(500))
                        .build())
                .lane("project:*", 10, QueueWeights.uniform())
                .build();
        node.start();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(jobsByQueue.values().stream()
                            .mapToLong(jobs -> jobs.stream()
                                    .filter(job -> store.findById(job.id())
                                                    .orElseThrow()
                                                    .currentState()
                                            == JobState.PROCESSING)
                                    .count())
                            .sum())
                    .isEqualTo(10);
            assertThat(jobsByQueue)
                    .allSatisfy((queue, jobs) -> assertThat(jobs.stream()
                                    .filter(job -> store.findById(job.id())
                                                    .orElseThrow()
                                                    .currentState()
                                            == JobState.PROCESSING)
                                    .count())
                            .as(queue)
                            .isEqualTo(1));
        });
    }

    @Test
    void queueFamilyStaticWeightsPreferHighWeightQueueSmoothly() {
        var heavy = new ArrayList<Job>();
        var light = new ArrayList<Job>();
        for (int i = 0; i < 20; i++) {
            heavy.add(enqueueHello(EngineTestHandlers.BlockingHandler.class, "project:42"));
            light.add(enqueueHello(EngineTestHandlers.BlockingHandler.class, "project:43"));
        }
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder()
                        .claimBatchSize(1)
                        .jobTimeout(Duration.ofSeconds(30))
                        .shutdownGracePeriod(Duration.ofMillis(50))
                        .queueFamilyDiscoveryInterval(Duration.ofMillis(50))
                        .queueFamilyRetentionAfterEmpty(Duration.ofMillis(500))
                        .build())
                .lane("project:*", 11, QueueWeights.fromMap(Map.of("project:42", 10, "project:43", 1)))
                .build();
        node.start();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            long heavyProcessing = heavy.stream()
                    .filter(job -> store.findById(job.id()).orElseThrow().currentState() == JobState.PROCESSING)
                    .count();
            long lightProcessing = light.stream()
                    .filter(job -> store.findById(job.id()).orElseThrow().currentState() == JobState.PROCESSING)
                    .count();
            assertThat(heavyProcessing + lightProcessing).isEqualTo(11);
            assertThat(heavyProcessing).isEqualTo(10);
            assertThat(lightProcessing).isEqualTo(1);
        });
    }

    @Test
    void queueFamilyLateJoinerDoesNotMonopolizeClaims() {
        // Phase 1: two queues run long enough to accumulate stride passes.
        var warmup = new ArrayList<Job>();
        for (int i = 0; i < 20; i++) {
            warmup.add(enqueueHello(EngineTestHandlers.CountingHandler.class, "project:a"));
            warmup.add(enqueueHello(EngineTestHandlers.CountingHandler.class, "project:b"));
        }
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder()
                        .workerCount(3)
                        .claimBatchSize(1)
                        .jobTimeout(Duration.ofSeconds(30))
                        .shutdownGracePeriod(Duration.ofMillis(50))
                        .queueFamilyDiscoveryInterval(Duration.ofMillis(50))
                        .queueFamilyRetentionAfterEmpty(Duration.ofSeconds(30))
                        .build())
                .lane("project:*", 3, QueueWeights.uniform())
                .build();
        node.start();
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(warmup.stream()
                                .allMatch(j ->
                                        store.findById(j.id()).orElseThrow().currentState() == JobState.SUCCEEDED))
                        .isTrue());

        // Phase 2: introduce a third queue with backlog alongside fresh work
        // on the established queues. A newcomer joining at pass 0 would soak
        // up all three worker slots before project:a / project:b see a claim.
        var newcomer = new ArrayList<Job>();
        var established = new ArrayList<Job>();
        for (int i = 0; i < 4; i++) {
            newcomer.add(enqueueHello(EngineTestHandlers.BlockingHandler.class, "project:late"));
        }
        for (int i = 0; i < 2; i++) {
            established.add(enqueueHello(EngineTestHandlers.BlockingHandler.class, "project:a"));
            established.add(enqueueHello(EngineTestHandlers.BlockingHandler.class, "project:b"));
        }

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            long newcomerProcessing = newcomer.stream()
                    .filter(j -> store.findById(j.id()).orElseThrow().currentState() == JobState.PROCESSING)
                    .count();
            long establishedProcessing = established.stream()
                    .filter(j -> store.findById(j.id()).orElseThrow().currentState() == JobState.PROCESSING)
                    .count();
            assertThat(newcomerProcessing + establishedProcessing).isEqualTo(3);
            // The newcomer competes fairly from its first pick: it cannot
            // hold all three worker slots while established queues starve.
            assertThat(newcomerProcessing).isLessThanOrEqualTo(2);
            assertThat(establishedProcessing).isGreaterThanOrEqualTo(1);
        });
    }

    @Test
    void queueFamilyLaneDiscoversNewQueuesAfterStart() {
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder()
                        .queueFamilyDiscoveryInterval(Duration.ofMillis(50))
                        .queueFamilyRetentionAfterEmpty(Duration.ofMillis(200))
                        .build())
                .lane("project:*", 2, QueueWeights.uniform())
                .build();
        node.start();

        Job late = enqueueHello(EngineTestHandlers.CountingHandler.class, "project:late");
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(
                        () -> assertThat(store.findById(late.id()).orElseThrow().currentState())
                                .isEqualTo(JobState.SUCCEEDED));
    }

    @Test
    void queueFamilyEmptyQueueIsDroppedAfterRetentionWindow() throws Exception {
        var calls = new AtomicInteger();
        Job first = enqueueHello(EngineTestHandlers.CountingHandler.class, "project:burst");
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder()
                        .pollInterval(Duration.ofMillis(25))
                        .queueFamilyDiscoveryInterval(Duration.ofMillis(500))
                        .queueFamilyRetentionAfterEmpty(Duration.ofMillis(100))
                        .build())
                .lane("project:*", 1, QueueWeights.from(queue -> {
                    calls.incrementAndGet();
                    return 1;
                }))
                .build();
        node.start();

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(
                                store.findById(first.id()).orElseThrow().currentState())
                        .isEqualTo(JobState.SUCCEEDED));
        assertThat(calls).hasValue(1);

        Thread.sleep(250);
        Job second = enqueueHello(EngineTestHandlers.CountingHandler.class, "project:burst");
        Thread.sleep(100);
        assertThat(store.findById(second.id()).orElseThrow().currentState()).isEqualTo(JobState.ENQUEUED);
        assertThat(calls).hasValue(1);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(store.findById(second.id()).orElseThrow().currentState()).isEqualTo(JobState.SUCCEEDED);
            assertThat(calls).hasValue(2);
        });
    }

    @Test
    void queueFamilyLaneSkipsConcurrencyBlockedQueueAndDrainsAnother() {
        Job blocker = enqueueHello(
                EngineTestHandlers.CountingHandler.class, "project:block", "project:block", ConcurrencyMode.EXCLUSIVE);
        store.claimReady(NodeId.newId(), "project:block", 1, Instant.now());
        Job blocked = enqueueHello(
                EngineTestHandlers.CountingHandler.class, "project:block", "project:block", ConcurrencyMode.EXCLUSIVE);
        Job free = enqueueHello(EngineTestHandlers.CountingHandler.class, "project:free");

        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder()
                        .queueFamilyDiscoveryInterval(Duration.ofMillis(50))
                        .queueFamilyRetentionAfterEmpty(Duration.ofMillis(200))
                        .build())
                .lane("project:*", 2, QueueWeights.uniform())
                .build();
        node.start();

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(
                        () -> assertThat(store.findById(free.id()).orElseThrow().currentState())
                                .isEqualTo(JobState.SUCCEEDED));
        assertThat(store.findById(blocker.id()).orElseThrow().currentState()).isEqualTo(JobState.PROCESSING);
        assertThat(store.findById(blocked.id()).orElseThrow().currentState()).isEqualTo(JobState.ENQUEUED);
    }

    @Test
    void queueFamilyDynamicWeightsAreResolvedOncePerDiscoveryCadence() {
        var calls = new AtomicInteger();
        Job first = enqueueHello(EngineTestHandlers.CountingHandler.class, "project:a");
        Job second = enqueueHello(EngineTestHandlers.CountingHandler.class, "project:b");
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder()
                        .queueFamilyDiscoveryInterval(Duration.ofSeconds(5))
                        .queueFamilyRetentionAfterEmpty(Duration.ofSeconds(10))
                        .build())
                .lane("project:*", 2, QueueWeights.from(queue -> {
                    calls.incrementAndGet();
                    return 1;
                }))
                .build();
        node.start();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(store.findById(first.id()).orElseThrow().currentState()).isEqualTo(JobState.SUCCEEDED);
            assertThat(store.findById(second.id()).orElseThrow().currentState()).isEqualTo(JobState.SUCCEEDED);
        });
        assertThat(calls.get()).isEqualTo(2);
    }

    @Test
    void closeWaitsForInFlightJobWithinGracePeriod() {
        Job job = enqueueHello(EngineTestHandlers.SlowHandler.class, fastConfig.defaultQueue());
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder()
                        .jobTimeout(Duration.ofSeconds(5))
                        .shutdownGracePeriod(Duration.ofSeconds(2))
                        .build())
                .build();
        node.start();

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(
                        () -> assertThat(store.findById(job.id()).orElseThrow().currentState())
                                .isEqualTo(JobState.PROCESSING));
        node.close();
        node = null;

        assertThat(EngineTestHandlers.SlowHandler.COMPLETIONS.get()).isEqualTo(1);
        assertThat(store.findById(job.id()).orElseThrow().currentState()).isEqualTo(JobState.SUCCEEDED);
    }

    @Test
    void failPathRoutesThroughRetryInterceptorForAllCauses() {
        // Audit §6.2 — verify a counting interceptor sees onProcessingFailed
        // exactly once per cause for (orphan reclaim, per-job timeout, thrown
        // exception). RetryInterceptor itself is always registered by
        // ProcessingNode, so this confirms the single-failure-code-path
        // invariant: every cause routes through the same hook.
        for (var cause : new JobInterceptor.FailureCause[] {
            JobInterceptor.FailureCause.EXCEPTION,
            JobInterceptor.FailureCause.TIMEOUT,
            JobInterceptor.FailureCause.ORPHAN_RECLAIM
        }) {
            EngineTestHandlers.reset();
            store = new InMemoryJobStore();
            if (node != null) {
                node.close();
                node = null;
            }
            runFailureCauseAndAssertSingleHook(cause);
        }
    }

    private void runFailureCauseAndAssertSingleHook(JobInterceptor.FailureCause expected) {
        var seen = new CopyOnWriteArrayList<JobInterceptor.FailureCause>();
        var subject = expected; // for inner-class capture
        var interceptor = new JobInterceptor() {
            @Override
            public void onProcessingFailed(Job j, JobExecutionContext c, Throwable cause, FailureCause kind) {
                if (kind == subject) seen.add(kind);
            }
        };

        switch (expected) {
            case EXCEPTION -> {
                Job j = enqueueHello(EngineTestHandlers.FailingHandler.class, fastConfig.defaultQueue());
                node = ProcessingNode.builder(store)
                        .config(fastConfig.toBuilder().defaultMaxAttempts(1).build())
                        .interceptor(interceptor)
                        .build();
                node.start();
                await().atMost(Duration.ofSeconds(10))
                        .untilAsserted(() -> assertThat(
                                        store.findById(j.id()).orElseThrow().currentState())
                                .isEqualTo(JobState.FAILED));
            }
            case TIMEOUT -> {
                Job j = enqueueHello(EngineTestHandlers.HangingHandler.class, fastConfig.defaultQueue());
                node = ProcessingNode.builder(store)
                        .config(fastConfig.toBuilder()
                                .jobTimeout(Duration.ofMillis(300))
                                .defaultMaxAttempts(1)
                                .build())
                        .interceptor(interceptor)
                        .build();
                node.start();
                await().atMost(Duration.ofSeconds(10))
                        .untilAsserted(() -> assertThat(
                                        store.findById(j.id()).orElseThrow().currentState())
                                .isEqualTo(JobState.FAILED));
            }
            case ORPHAN_RECLAIM -> {
                Job j = Job.builder()
                        .spec(new JobSpec(
                                EngineTestHandlers.CountingHandler.class.getName(),
                                List.of(serializer.serializePayload(new EngineTestHandlers.HelloPayload("x")))))
                        .build();
                store.insert(j);
                // Claim from a different node with a stale heartbeat so the maintenance loop reclaims it.
                store.claimReady(
                        NodeId.newId(),
                        fastConfig.defaultQueue(),
                        1,
                        Instant.now().minus(Duration.ofHours(2)));
                node = ProcessingNode.builder(store)
                        .config(fastConfig)
                        .interceptor(interceptor)
                        .build();
                node.start();
                await().atMost(Duration.ofSeconds(10))
                        .untilAsserted(() -> assertThat(seen).contains(JobInterceptor.FailureCause.ORPHAN_RECLAIM));
            }
            default -> throw new IllegalStateException("unhandled: " + expected);
        }

        // The expected cause was observed exactly once.
        assertThat(seen).as("cause=" + expected).containsExactly(expected);
    }

    @Test
    void pausedQueueDoesNotStarveUnpausedQueues() {
        // Audit §6.5 — paused queues stop draining while unpaused queues
        // continue at full throughput. Enabled in the phase-1 commit that
        // landed per-queue pause.
        Job alpha = enqueueHello(EngineTestHandlers.CountingHandler.class, "alpha");
        Job beta = enqueueHello(EngineTestHandlers.CountingHandler.class, "beta");
        Job alpha2 = enqueueHello(EngineTestHandlers.CountingHandler.class, "alpha");

        store.pauseQueue("beta", "test");

        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder().workerCount(2).build())
                .lane(new QueueLane("alpha", 2))
                .lane(new QueueLane("beta", 2))
                .build();
        node.start();

        // alpha drains promptly; beta stays ENQUEUED while paused.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(store.findById(alpha.id()).orElseThrow().currentState()).isEqualTo(JobState.SUCCEEDED);
            assertThat(store.findById(alpha2.id()).orElseThrow().currentState()).isEqualTo(JobState.SUCCEEDED);
        });
        assertThat(store.findById(beta.id()).orElseThrow().currentState()).isEqualTo(JobState.ENQUEUED);

        // Resume beta — the dispatcher picks the job up on the next claim cycle.
        store.resumeQueue("beta");
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(
                        () -> assertThat(store.findById(beta.id()).orElseThrow().currentState())
                                .isEqualTo(JobState.SUCCEEDED));
    }

    @Test
    void idleWorkerWakesDispatcherEarlyUnderBurstyLoad() {
        // Audit §4.4 — a long pollInterval must not pin the queue when workers
        // finish quickly. The WakeSignal lets the dispatcher break out of its
        // poll sleep as soon as a worker becomes idle.
        var burstyConfig = fastConfig.toBuilder()
                .workerCount(1)
                .pollInterval(Duration.ofSeconds(2))
                .build();
        var jobs = new ArrayList<Job>();
        for (int i = 0; i < 6; i++) {
            jobs.add(enqueueHello(EngineTestHandlers.CountingHandler.class, fastConfig.defaultQueue()));
        }
        node = ProcessingNode.builder(store).config(burstyConfig).build();

        long started = System.nanoTime();
        node.start();
        await().atMost(Duration.ofSeconds(4)).untilAsserted(() -> {
            for (Job j : jobs) {
                assertThat(store.findById(j.id()).orElseThrow().currentState()).isEqualTo(JobState.SUCCEEDED);
            }
        });
        long elapsed = (System.nanoTime() - started) / 1_000_000;
        // Without the wake-up, 6 jobs at 1 worker × 2s poll would take ≥10s.
        // With wakeups, this is well under one pollInterval.
        assertThat(elapsed).as("elapsed ms").isLessThan(2_500L);
    }

    @Test
    void mixedWorkloadQueueFamilyDrainsAndLeavesNoConcurrencyHoldBehind() {
        var mixedConfig = fastConfig.toBuilder()
                .workerCount(8)
                .claimBatchSize(8)
                .pollInterval(Duration.ofMillis(10))
                .jobTimeout(Duration.ofSeconds(5))
                .defaultMaxAttempts(1)
                .queueFamilyDiscoveryInterval(Duration.ofMillis(20))
                .build();
        int resourceCount = 20;
        int total = 220;
        for (int i = 0; i < total; i++) {
            int resource = i % resourceCount;
            String queue = "project:" + resource;
            String key = "project:" + resource;
            boolean exclusive = i % 10 == 0;
            enqueueHello(
                    exclusive ? EngineTestHandlers.SlowHandler.class : EngineTestHandlers.CountingHandler.class,
                    queue,
                    key,
                    exclusive ? ConcurrencyMode.EXCLUSIVE : ConcurrencyMode.SHARED);
        }

        node = ProcessingNode.builder(store)
                .config(mixedConfig)
                .lane("project:*", 8, QueueWeights.uniform())
                .build();
        node.start();

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(activeJobs()).as("active jobs").isZero());
        assertThat(store.findOrphaned(Instant.now().plus(Duration.ofSeconds(1)), 10))
                .isEmpty();

        node.close();
        node = null;

        for (int resource = 0; resource < resourceCount; resource++) {
            String queue = "project:" + resource;
            String key = "project:" + resource;
            Job sentinel =
                    enqueueHello(EngineTestHandlers.CountingHandler.class, queue, key, ConcurrencyMode.EXCLUSIVE);
            assertThat(store.claimReady(NodeId.newId(), queue, 1, Instant.now()))
                    .as("concurrency key released for " + key)
                    .extracting(Job::id)
                    .containsExactly(sentinel.id());
        }
    }

    @Test
    void oversizedExceptionTraceDoesNotBlockFailedTransition() {
        // Audit §6.3 — a handler that throws an exception with a 200KB
        // message must still produce a FAILED save without throwing
        // OversizedJobException. The truncation policy lives in the
        // serializer; this test asserts the engine integration succeeds.
        Job job = enqueueHello(EngineTestHandlers.BigErrorMessageHandler.class, fastConfig.defaultQueue());
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder().defaultMaxAttempts(1).build())
                .build();
        node.start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job loaded = store.findById(job.id()).orElseThrow();
            assertThat(loaded.currentState()).isEqualTo(JobState.FAILED);
            // The exception's leading message fragment survives in the state-history entry.
            assertThat(loaded.stateHistory())
                    .anySatisfy(e -> assertThat(e.message()).startsWith("big-error:"));
        });
    }

    @Test
    void dispatchFailureMidBatchDoesNotAbandonRemainingClaimedJobs() {
        // Three jobs claimed in one batch. The middle one requires a tag this
        // node lacks, and its tag-mismatch release is made to fail with a
        // non-stale store error — historically that aborted the dispatch loop
        // and abandoned the rest of the batch in PROCESSING (heartbeat-shielded).
        Job first = enqueueHello(EngineTestHandlers.CountingHandler.class, fastConfig.defaultQueue());
        JobArgument arg = serializer.serializePayload(new EngineTestHandlers.HelloPayload("tagged"));
        Job tagged = Job.builder()
                .spec(new JobSpec(EngineTestHandlers.CountingHandler.class.getName(), List.of(arg)))
                .queue(fastConfig.defaultQueue())
                .metadata(Dispatcher.REQUIRED_TAGS_META, "gpu")
                .build();
        store.insert(tagged);
        Job third = enqueueHello(EngineTestHandlers.CountingHandler.class, fastConfig.defaultQueue());

        var failing = new ForwardingJobStore(store) {
            @Override
            public void saveAtomic(Job job, long expectedVersion) {
                if (job.id().equals(tagged.id()) && job.currentState() == JobState.SCHEDULED) {
                    throw new RuntimeException("release rejected");
                }
                super.saveAtomic(job, expectedVersion);
            }
        };
        node = ProcessingNode.builder(failing)
                .config(fastConfig.toBuilder().claimBatchSize(3).build())
                .build();
        node.start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(store.findById(first.id()).orElseThrow().currentState()).isEqualTo(JobState.SUCCEEDED);
            assertThat(store.findById(third.id()).orElseThrow().currentState()).isEqualTo(JobState.SUCCEEDED);
        });

        // Worker capacity is intact: a later job still gets processed.
        Job fourth = enqueueHello(EngineTestHandlers.CountingHandler.class, fastConfig.defaultQueue());
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(
                                store.findById(fourth.id()).orElseThrow().currentState())
                        .isEqualTo(JobState.SUCCEEDED));
    }

    @Test
    void retentionSweepDrainsBeyondOneBatchAndCoversAllTerminalStates() {
        Instant old = Instant.now().minus(Duration.ofDays(40));
        insertTerminal(JobState.SUCCEEDED, old, 250);
        insertTerminal(JobState.FAILED, old, 150);
        insertTerminal(JobState.DELETED, old, 120);
        insertTerminal(JobState.QUARANTINED, old, 110);
        insertTerminal(JobState.SUCCEEDED, Instant.now(), 1);

        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder()
                        .maintenancePollInterval(Duration.ofMillis(50))
                        .retentionInterval(Duration.ofMillis(100))
                        .build())
                .build();
        node.start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Map<JobState, Long> counts = store.countsByState();
            // The drain loop must clear all 250 old SUCCEEDED jobs (beyond one
            // 100-batch) and sweep the other terminal states too.
            assertThat(counts.getOrDefault(JobState.SUCCEEDED, 0L)).isEqualTo(1L);
            assertThat(counts.getOrDefault(JobState.FAILED, 0L)).isZero();
            assertThat(counts.getOrDefault(JobState.DELETED, 0L)).isZero();
            assertThat(counts.getOrDefault(JobState.QUARANTINED, 0L)).isZero();
        });
    }

    private void insertTerminal(JobState terminal, Instant at, int n) {
        JobArgument arg = serializer.serializePayload(new EngineTestHandlers.HelloPayload("x"));
        for (int i = 0; i < n; i++) {
            Job j = Job.builder()
                    .spec(new JobSpec("com.example.Done", List.of(arg)))
                    .createdAt(at)
                    .withStateHistory(List.of(
                            JobStateEntry.of(JobState.ENQUEUED, at),
                            new JobStateEntry(JobState.PROCESSING, at, "test", null),
                            new JobStateEntry(terminal, at, "test", null)))
                    .build();
            store.insert(j);
        }
    }

    @Test
    void perJobTimeoutShorterThanTheGlobalDefaultFiresOnTime() {
        var failureKinds = new CopyOnWriteArrayList<JobInterceptor.FailureCause>();
        JobArgument arg = serializer.serializePayload(new EngineTestHandlers.HelloPayload("test"));
        Job job = Job.builder()
                .spec(new JobSpec(EngineTestHandlers.HangingHandler.class.getName(), List.of(arg)))
                .queue(fastConfig.defaultQueue())
                .metadata("threadmill.job.timeoutSeconds", "1")
                .build();
        store.insert(job);
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder()
                        .jobTimeout(Duration.ofSeconds(60))
                        .noProgressTimeout(Duration.ofSeconds(60))
                        .defaultMaxAttempts(1)
                        .build())
                .interceptor(new JobInterceptor() {
                    @Override
                    public void onProcessingFailed(Job j, JobExecutionContext c, Throwable cause, FailureCause kind) {
                        failureKinds.add(kind);
                    }
                })
                .build();
        node.start();

        // The 1-second per-job override must drive the watchdog's initial
        // delay; with the 60-second global timeout this would otherwise not
        // be checked within the test window at all.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(failureKinds).contains(JobInterceptor.FailureCause.TIMEOUT);
            Job loaded = store.findById(job.id()).orElseThrow();
            assertThat(loaded.currentState()).isEqualTo(JobState.FAILED);
        });
    }

    @Test
    void malformedTimeoutMetadataStillEnforcesTheGlobalTimeout() {
        var failureKinds = new CopyOnWriteArrayList<JobInterceptor.FailureCause>();
        JobArgument arg = serializer.serializePayload(new EngineTestHandlers.HelloPayload("test"));
        Job job = Job.builder()
                .spec(new JobSpec(EngineTestHandlers.HangingHandler.class.getName(), List.of(arg)))
                .queue(fastConfig.defaultQueue())
                .metadata("threadmill.job.timeoutSeconds", "abc")
                .build();
        store.insert(job);
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder()
                        .jobTimeout(Duration.ofMillis(300))
                        .defaultMaxAttempts(1)
                        .build())
                .interceptor(new JobInterceptor() {
                    @Override
                    public void onProcessingFailed(Job j, JobExecutionContext c, Throwable cause, FailureCause kind) {
                        failureKinds.add(kind);
                    }
                })
                .build();
        node.start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(failureKinds).contains(JobInterceptor.FailureCause.TIMEOUT);
            Job loaded = store.findById(job.id()).orElseThrow();
            assertThat(loaded.currentState()).isEqualTo(JobState.FAILED);
        });
    }

    @Test
    void transientSucceededSaveFailureIsRetriedAndTheJobSucceeds() {
        var remainingFailures = new AtomicInteger(2);
        var failing = new ForwardingJobStore(store) {
            @Override
            public void saveAtomic(Job job, long expectedVersion) {
                if (job.currentState() == JobState.SUCCEEDED && remainingFailures.getAndDecrement() > 0) {
                    throw new RuntimeException("transient store blip");
                }
                super.saveAtomic(job, expectedVersion);
            }
        };
        Job job = enqueueHello(EngineTestHandlers.CountingHandler.class, fastConfig.defaultQueue());
        node = ProcessingNode.builder(failing).config(fastConfig).build();
        node.start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job loaded = store.findById(job.id()).orElseThrow();
            assertThat(loaded.currentState()).isEqualTo(JobState.SUCCEEDED);
        });
        // The retry happened at the save level, not by re-running the handler.
        assertThat(EngineTestHandlers.CountingHandler.COUNT
                        .get(job.id().toString())
                        .get())
                .isEqualTo(1);
    }

    @Test
    void deterministicSucceededSaveFailureRoutesThroughTheSingleFailurePathAndReleasesTheKey() {
        var failureKinds = new CopyOnWriteArrayList<JobInterceptor.FailureCause>();
        Job poisoned = enqueueHello(
                EngineTestHandlers.CountingHandler.class, fastConfig.defaultQueue(), "k", ConcurrencyMode.EXCLUSIVE);
        pauseForOrdering();
        Job follower = enqueueHello(
                EngineTestHandlers.CountingHandler.class, fastConfig.defaultQueue(), "k", ConcurrencyMode.EXCLUSIVE);
        var failing = new ForwardingJobStore(store) {
            @Override
            public void saveAtomic(Job job, long expectedVersion) {
                if (job.currentState() == JobState.SUCCEEDED && job.id().equals(poisoned.id())) {
                    throw new SerializationException("store rejects this SUCCEEDED snapshot");
                }
                super.saveAtomic(job, expectedVersion);
            }
        };
        node = ProcessingNode.builder(failing)
                .config(fastConfig.toBuilder().defaultMaxAttempts(1).build())
                .interceptor(new JobInterceptor() {
                    @Override
                    public void onProcessingFailed(Job j, JobExecutionContext c, Throwable cause, FailureCause kind) {
                        if (j.id().equals(poisoned.id())) failureKinds.add(kind);
                    }
                })
                .build();
        node.start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            // The job must not stay PROCESSING: the failed SUCCEEDED save is
            // routed through the single failure path...
            Job loadedPoisoned = store.findById(poisoned.id()).orElseThrow();
            assertThat(loadedPoisoned.currentState()).isEqualTo(JobState.FAILED);
            // ...and the EXCLUSIVE key is released for the next job.
            Job loadedFollower = store.findById(follower.id()).orElseThrow();
            assertThat(loadedFollower.currentState()).isEqualTo(JobState.SUCCEEDED);
        });
        assertThat(failureKinds).containsExactly(JobInterceptor.FailureCause.EXCEPTION);
    }

    @Test
    void oversizedHandlerResultIsDroppedRatherThanBlockingTheSucceededSave() {
        Job job = enqueueHello(EngineTestHandlers.BigResultHandler.class, fastConfig.defaultQueue());
        node = ProcessingNode.builder(store).config(fastConfig).build();
        node.start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Job loaded = store.findById(job.id()).orElseThrow();
            assertThat(loaded.currentState()).isEqualTo(JobState.SUCCEEDED);
            assertThat(loaded.result()).isEmpty();
            assertThat(loaded.metadata().get(JsonJobSerializer.TRUNCATED_RESULT_KEY))
                    .isPresent();
        });
    }

    private long activeJobs() {
        Map<JobState, Long> counts = store.countsByState();
        return counts.getOrDefault(JobState.ENQUEUED, 0L)
                + counts.getOrDefault(JobState.SCHEDULED, 0L)
                + counts.getOrDefault(JobState.AWAITING, 0L)
                + counts.getOrDefault(JobState.PROCESSING, 0L);
    }

    private static void pauseForOrdering() {
        try {
            Thread.sleep(2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void persistentHeartbeatFailureSuspendsClaimingAndRecovers() throws Exception {
        var heartbeatDown = new AtomicInteger(1); // 1 = failing
        var failingStore = new ForwardingJobStore(store) {
            @Override
            public void touchOwnerHeartbeat(NodeId n, Instant now) {
                if (heartbeatDown.get() == 1) throw new RuntimeException("heartbeat write failing");
                super.touchOwnerHeartbeat(n, now);
            }
        };
        node = ProcessingNode.builder(failingStore)
                .config(fastConfig.toBuilder()
                        .claimHeartbeat(Duration.ofMillis(50))
                        .heartbeatTimeout(Duration.ofMillis(200))
                        .build())
                .build();
        node.start();

        // Heartbeats fail for ~heartbeatTimeout, so the node suspends claiming.
        await().atMost(Duration.ofSeconds(5)).until(node::isClaimingSuspended);

        // While suspended, newly-enqueued work must NOT be claimed.
        Job blocked = enqueueHello(EngineTestHandlers.CountingHandler.class, "default");
        Thread.sleep(300);
        assertThat(store.findById(blocked.id()).orElseThrow().currentState()).isEqualTo(JobState.ENQUEUED);

        // Recovery: once heartbeats succeed, claiming resumes and the job runs.
        heartbeatDown.set(0);
        await().atMost(Duration.ofSeconds(5)).until(() -> !node.isClaimingSuspended());
        await().atMost(Duration.ofSeconds(5))
                .until(() -> store.findById(blocked.id()).orElseThrow().currentState() == JobState.SUCCEEDED);
    }

    @Test
    void ownerHeartbeatsKeepFlowingWhileWorkersDrainOnShutdown() throws Exception {
        Job job = enqueueHello(EngineTestHandlers.BlockingHandler.class, "default");
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder()
                        .claimHeartbeat(Duration.ofMillis(100))
                        .jobTimeout(Duration.ofSeconds(30))
                        .shutdownGracePeriod(Duration.ofMillis(700))
                        .build())
                .build();
        node.start();

        await().atMost(Duration.ofSeconds(5)).until(() -> {
            Job j = store.findById(job.id()).orElseThrow();
            return j.currentState() == JobState.PROCESSING
                    && j.ownerHeartbeatAt().isPresent();
        });
        Instant before =
                store.findById(job.id()).orElseThrow().ownerHeartbeatAt().orElseThrow();

        // close() drains the blocking handler for shutdownGracePeriod before
        // interrupting it; the owner-heartbeat thread must stay alive through the
        // drain so another node does not orphan-reclaim the still-running job.
        Thread closeThread = Thread.ofPlatform().start(() -> node.close());
        Thread.sleep(350);
        Instant during =
                store.findById(job.id()).orElseThrow().ownerHeartbeatAt().orElseThrow();
        assertThat(during).isAfter(before);

        closeThread.join(Duration.ofSeconds(5));
    }

    @Test
    void shutdownInterruptedJobIsRescheduledWithoutBurningAnAttempt() throws Exception {
        Job job = enqueueHello(EngineTestHandlers.BlockingHandler.class, "default");
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder()
                        .jobTimeout(Duration.ofSeconds(30))
                        .shutdownGracePeriod(Duration.ofMillis(200))
                        .build())
                .build();
        node.start();
        await().atMost(Duration.ofSeconds(5))
                .until(() -> store.findById(job.id()).orElseThrow().currentState() == JobState.PROCESSING);

        // Grace expires while the handler blocks; shutdownNow() interrupts it.
        // The failure must be classified SHUTDOWN, not EXCEPTION: rescheduled
        // immediately with the claim-time attempt increment reverted, so a
        // rolling deploy never consumes retry budget.
        node.close();

        // close() returns as soon as the pool is torn down; the interrupted
        // worker finishes its failure path concurrently.
        await().atMost(Duration.ofSeconds(5))
                .until(() -> store.findById(job.id()).orElseThrow().currentState() == JobState.SCHEDULED);
        Job after = store.findById(job.id()).orElseThrow();
        assertThat(after.attempts()).as("shutdown must not burn an attempt").isZero();
    }

    @Test
    void tagMismatchReleaseRoutesThroughTheFailurePathWithoutBurningAnAttempt() {
        // A claimed job whose required tags this node lacks used to be
        // "released" via an illegal PROCESSING -> SCHEDULED transition that
        // always threw, leaving the job PROCESSING (heartbeat-shielded) until
        // orphan reclaim burned a retry attempt on it. The release must route
        // through the single failure path with SHUTDOWN semantics: prompt
        // reschedule, no attempt consumed, concurrency slot freed.
        JobArgument arg = serializer.serializePayload(new EngineTestHandlers.HelloPayload("tagged"));
        Job job = Job.builder()
                .spec(new JobSpec(EngineTestHandlers.CountingHandler.class.getName(), List.of(arg)))
                .queue(fastConfig.defaultQueue())
                .metadata(Dispatcher.REQUIRED_TAGS_META, "gpu")
                .concurrencyKey("tag-release")
                .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
                .build();
        store.insert(job);
        // Heartbeats far outlive the assertion window, so orphan reclaim
        // cannot be the mechanism that moves the job out of PROCESSING here.
        node = ProcessingNode.builder(store)
                .config(fastConfig.toBuilder()
                        .heartbeatTimeout(Duration.ofSeconds(60))
                        .build())
                .build();
        node.start();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Job loaded = store.findById(job.id()).orElseThrow();
            assertThat(loaded.stateHistory())
                    .anySatisfy(e -> assertThat(e.reason()).isEqualTo("engine.retry-after-shutdown"));
            assertThat(loaded.attempts()).as("release must not burn an attempt").isZero();
        });

        node.close();
        node = null;

        // A properly-tagged node runs it. The EXCLUSIVE key it cycled through
        // must not be left held by the releases, or this claim never happens.
        secondNode = ProcessingNode.builder(store).config(fastConfig).tag("gpu").build();
        secondNode.start();
        await().atMost(Duration.ofSeconds(10))
                .until(() -> store.findById(job.id()).orElseThrow().currentState() == JobState.SUCCEEDED);
        assertThat(store.findById(job.id()).orElseThrow().attempts()).isEqualTo(1);
    }

    @Test
    void interruptedMidBatchDispatchReleasesRemainingClaimedJobsForRedelivery() {
        // A dispatcher interrupted between the batch claim and worker
        // submission (node shutdown mid-batch, e.g. rolling-deploy churn) must
        // release every remaining claimed job for prompt redelivery — not
        // leave them PROCESSING until orphan reclaim, and not burn their
        // claim-time attempt increments.
        Job first = enqueueHello(EngineTestHandlers.CountingHandler.class, fastConfig.defaultQueue());
        Job second = enqueueHello(EngineTestHandlers.CountingHandler.class, fastConfig.defaultQueue());
        Job third = enqueueHello(EngineTestHandlers.CountingHandler.class, fastConfig.defaultQueue());

        var interrupting = new ForwardingJobStore(store) {
            final AtomicBoolean fired = new AtomicBoolean(false);

            @Override
            public List<Job> claimReady(NodeId nodeId, String queue, int max, Instant now) {
                List<Job> claimed = super.claimReady(nodeId, queue, max, now);
                if (!claimed.isEmpty() && fired.compareAndSet(false, true)) {
                    // Simulates stop() interrupting the loop mid-batch: the
                    // next workerCapacity.acquire() throws InterruptedException.
                    Thread.currentThread().interrupt();
                }
                return claimed;
            }
        };
        node = ProcessingNode.builder(interrupting)
                .config(fastConfig.toBuilder()
                        .claimBatchSize(3)
                        .heartbeatTimeout(Duration.ofSeconds(60))
                        .build())
                .build();
        node.start();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            for (Job job : List.of(first, second, third)) {
                Job loaded = store.findById(job.id()).orElseThrow();
                assertThat(loaded.currentState()).isIn(JobState.SCHEDULED, JobState.ENQUEUED);
                assertThat(loaded.attempts())
                        .as("release must not burn an attempt")
                        .isZero();
                assertThat(loaded.stateHistory())
                        .anySatisfy(e -> assertThat(e.reason()).isEqualTo("engine.retry-after-shutdown"));
            }
        });
    }
}
