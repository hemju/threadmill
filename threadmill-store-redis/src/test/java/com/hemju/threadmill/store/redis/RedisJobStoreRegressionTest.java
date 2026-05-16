package com.hemju.threadmill.store.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobSnapshot;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.core.store.JobStoreCapabilities;

/**
 * Redis-specific regression tests.
 *
 * <p>The contract suite ({@link RedisJobStoreContractTest}) already covers
 * the SPI semantics on real Redis. These tests pin down:
 * <ul>
 *   <li><strong>Reliable claim:</strong> a worker crash after claim must
 *       leave the job recoverable (it stays in {@code processing:{node}}
 *       and shows up in {@code findOrphaned} when its heartbeat expires).</li>
 *   <li><strong>Lua atomicity:</strong> after a state change, the indexes
 *       and the hash never disagree (no destructive-pop loss).</li>
 *   <li><strong>Concurrency:</strong> N concurrent virtual-thread workers
 *       never double-claim.</li>
 * </ul>
 */
class RedisJobStoreRegressionTest {

    @SuppressWarnings("resource")
    private static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379)
            .withCommand("redis-server", "--appendonly", "yes")
            .waitingFor(Wait.forListeningPort());

    private static RedisURI uri;
    private static RedisClient adminClient;
    private static StatefulRedisConnection<String, String> adminConnection;

    @BeforeAll
    static void start() {
        REDIS.start();
        uri = RedisURI.create("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
        adminClient = RedisClient.create(uri);
        adminConnection = adminClient.connect();
    }

    @AfterAll
    static void stop() {
        if (adminConnection != null) adminConnection.close();
        if (adminClient != null) adminClient.shutdown();
        if (REDIS.isRunning()) REDIS.stop();
    }

    @BeforeEach
    void flush() {
        adminConnection.sync().flushdb();
    }

    private JobStore store() {
        return new RedisJobStore(uri);
    }

    private static Job sample() {
        return Job.builder()
                .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
                .build();
    }

    @Test
    void crashMidProcessingLeavesJobInProcessingIndexForOrphanRecovery() {
        JobStore store = store();
        Job j = sample();
        store.insert(j);
        NodeId node = NodeId.newId();
        var longAgo = Instant.now().minus(2, ChronoUnit.HOURS);
        List<Job> claimed = store.claimReady(node, "default", 1, longAgo);
        assertThat(claimed).hasSize(1);

        // Simulate worker crash: no completion call, just check the orphan reaper finds the job.
        var cutoff = Instant.now().minus(30, ChronoUnit.MINUTES);
        List<Job> orphans = store.findOrphaned(cutoff, 10);
        assertThat(orphans)
                .extracting(o -> o.id().toString())
                .containsExactly(claimed.get(0).id().toString());
    }

    @Test
    void claimReadyDoesNotLoseJobsWhenManyConcurrentNodesContend() throws Exception {
        JobStore store = store();
        int total = 200;
        for (int i = 0; i < total; i++) store.insert(sample());

        int workers = 12;
        var start = new CountDownLatch(1);
        Set<UUID> seen = ConcurrentHashMap.newKeySet();
        var collisions = new ConcurrentHashMap<UUID, Integer>();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<java.util.concurrent.Future<List<Job>>> futures = new ArrayList<>();
            for (int w = 0; w < workers; w++) {
                NodeId node = NodeId.newId();
                futures.add(executor.submit(() -> {
                    start.await();
                    List<Job> mine = new ArrayList<>();
                    while (true) {
                        List<Job> got = store.claimReady(node, "default", 7, Instant.now());
                        if (got.isEmpty()) break;
                        mine.addAll(got);
                    }
                    return mine;
                }));
            }
            start.countDown();
            for (var f : futures) {
                for (Job j : f.get(60, TimeUnit.SECONDS)) {
                    if (!seen.add(j.id().asUuid())) {
                        collisions.merge(j.id().asUuid(), 1, Integer::sum);
                    }
                }
            }
            executor.shutdown();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(collisions)
                .as("no double-claim across concurrent virtual-thread workers")
                .isEmpty();
        assertThat(seen).hasSize(total);
    }

    @Test
    void afterClaimIndexesAndHashAreInSync() {
        JobStore store = store();
        Job j = sample();
        store.insert(j);
        NodeId node = NodeId.newId();
        store.claimReady(node, "default", 1, Instant.now());

        RedisCommands<String, String> r = adminConnection.sync();

        // Job must NOT be in the per-queue ZSET any more.
        assertThat(r.zscore(RedisKeys.queue("default"), j.id().toString())).isNull();
        // Job MUST be in both processing structures.
        assertThat(r.zscore(RedisKeys.PROCESSING_ALL, j.id().toString())).isNotNull();
        assertThat(r.zscore(RedisKeys.processingFor(node), j.id().toString())).isNotNull();
        // Job's hash state must match.
        assertThat(r.hget(RedisKeys.job(j.id()), "state")).isEqualTo("PROCESSING");
        // Counts must reflect the move.
        assertThat(r.hget(RedisKeys.COUNTS, "ENQUEUED")).isEqualTo("0");
        assertThat(r.hget(RedisKeys.COUNTS, "PROCESSING")).isEqualTo("1");
        // Body has the updated state too (claim_commit writes it atomically).
        Job loaded = store.findById(j.id()).orElseThrow();
        assertThat(loaded.currentState()).isEqualTo(JobState.PROCESSING);
    }

    @Test
    void serializerFailureDuringClaimLeavesJobEnqueuedAndConsistent() {
        var client = RedisClient.create(uri);
        try {
            JobStore store =
                    new RedisJobStore(client, new FailingOnProcessingSerializer(), JobStoreCapabilities.defaults());
            Job job = sample();
            store.insert(job);

            org.assertj.core.api.Assertions.assertThatThrownBy(
                            () -> store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("prepared claim serialization failed");

            RedisCommands<String, String> r = adminConnection.sync();
            assertThat(r.hget(RedisKeys.job(job.id()), "state")).isEqualTo("ENQUEUED");
            assertThat(r.zscore(RedisKeys.queue("default"), job.id().toString()))
                    .isNotNull();
            assertThat(r.zscore(RedisKeys.PROCESSING_ALL, job.id().toString())).isNull();
            assertThat(r.hget(RedisKeys.COUNTS, "ENQUEUED")).isEqualTo("1");
            assertThat(r.hget(RedisKeys.COUNTS, "PROCESSING")).isNull();

            List<Job> claimed = new RedisJobStore(uri).claimReady(NodeId.newId(), "default", 1, Instant.now());
            assertThat(claimed).hasSize(1);
        } finally {
            client.shutdown();
        }
    }

    @Test
    void capabilitiesAdvertiseLimitedSearch() {
        JobStore store = store();
        assertThat(store.capabilities().supportsRichSearch()).isFalse();
        assertThat(store.capabilities().supportsExactCounts()).isTrue();
    }

    @Test
    void everyMultiKeyTransitionLeavesNoOrphanIndexEntries() {
        JobStore store = store();
        Job a = sample();
        store.insert(a);
        // Insert → ENQUEUED structures must exist.
        RedisCommands<String, String> r = adminConnection.sync();
        assertThat(r.zscore(RedisKeys.queue("default"), a.id().toString())).isNotNull();
        assertThat(r.zscore(RedisKeys.byStateTime(JobState.ENQUEUED), a.id().toString()))
                .isNotNull();

        // Claim → moves the job out of the queue and into processing structures.
        store.claimReady(NodeId.newId(), "default", 1, Instant.now());
        assertThat(r.zscore(RedisKeys.queue("default"), a.id().toString())).isNull();
        assertThat(r.zscore(RedisKeys.byStateTime(JobState.ENQUEUED), a.id().toString()))
                .isNull();
        assertThat(r.zscore(RedisKeys.byStateTime(JobState.PROCESSING), a.id().toString()))
                .isNotNull();

        // Mark SUCCEEDED via saveAtomic → leaves the active processing structures cleanly.
        Job claimed = store.findById(a.id()).orElseThrow();
        long version = claimed.version();
        claimed.transitionTo(JobState.SUCCEEDED, Instant.now());
        store.saveAtomic(claimed, version);
        assertThat(r.zscore(RedisKeys.PROCESSING_ALL, a.id().toString())).isNull();
        assertThat(r.zscore(RedisKeys.byStateTime(JobState.PROCESSING), a.id().toString()))
                .isNull();
        assertThat(r.zscore(RedisKeys.byStateTime(JobState.SUCCEEDED), a.id().toString()))
                .isNotNull();
        // Counts reflect the moves all the way through.
        assertThat(r.hget(RedisKeys.COUNTS, "PROCESSING")).isEqualTo("0");
        assertThat(r.hget(RedisKeys.COUNTS, "SUCCEEDED")).isEqualTo("1");
    }

    @Test
    void softDeletingAProcessingJobCleansProcessingIndexesAndCounts() {
        JobStore store = store();
        Job job = sample();
        store.insert(job);
        NodeId owner = NodeId.newId();
        store.claimReady(owner, "default", 1, Instant.now());

        assertThat(store.softDelete(job.id())).isTrue();

        RedisCommands<String, String> r = adminConnection.sync();
        assertThat(r.zscore(RedisKeys.PROCESSING_ALL, job.id().toString())).isNull();
        assertThat(r.zscore(RedisKeys.processingFor(owner), job.id().toString()))
                .isNull();
        assertThat(r.zscore(RedisKeys.byStateTime(JobState.PROCESSING), job.id().toString()))
                .isNull();
        assertThat(r.zscore(RedisKeys.byStateTime(JobState.DELETED), job.id().toString()))
                .isNotNull();
        assertThat(r.hget(RedisKeys.COUNTS, "PROCESSING")).isEqualTo("0");
        assertThat(r.hget(RedisKeys.COUNTS, "DELETED")).isEqualTo("1");
        assertThat(store.findById(job.id()).orElseThrow().currentState()).isEqualTo(JobState.DELETED);
    }

    @Test
    void cronTaskDefinitionAndScheduleStateRoundTrip() {
        JobStore store = store();
        CronTask task = sampleCronTask("nightly-cleanup");
        var next = Instant.parse("2026-05-16T09:00:00Z");
        var last = Instant.parse("2026-05-15T09:00:00Z");
        UUID lastJob = UUID.randomUUID();
        UUID inFlight = UUID.randomUUID();

        store.upsertCronTask(task);
        store.upsertCronTaskState(new CronTaskScheduleState(task.name(), last, lastJob, next, inFlight));

        assertThat(store.findCronTask(task.name())).contains(task);
        assertThat(store.listCronTasks()).containsExactly(task);
        assertThat(store.findCronTaskState(task.name()))
                .contains(new CronTaskScheduleState(task.name(), last, lastJob, next, inFlight));

        store.upsertCronTaskState(new CronTaskScheduleState(task.name(), null, null, next, null));
        assertThat(store.findCronTaskState(task.name()))
                .contains(new CronTaskScheduleState(task.name(), null, null, next, null));

        store.deleteCronTask(task.name());
        assertThat(store.findCronTask(task.name())).isEmpty();
        assertThat(store.findCronTaskState(task.name())).isEmpty();
    }

    @Test
    void mutexLeaseIsExclusiveReentrantAndExpires() throws InterruptedException {
        JobStore store = store();
        assertThat(store.tryAcquireMutex("billing-close", "node-a", Duration.ofMillis(75)))
                .isTrue();
        assertThat(store.tryAcquireMutex("billing-close", "node-b", Duration.ofSeconds(5)))
                .isFalse();
        assertThat(store.tryAcquireMutex("billing-close", "node-a", Duration.ofSeconds(5)))
                .isTrue();
        store.releaseMutex("billing-close", "wrong-holder");
        assertThat(store.tryAcquireMutex("billing-close", "node-b", Duration.ofMillis(75)))
                .isFalse();
        store.releaseMutex("billing-close", "node-a");
        assertThat(store.tryAcquireMutex("billing-close", "node-b", Duration.ofMillis(75)))
                .isTrue();
        Thread.sleep(220);
        assertThat(store.tryAcquireMutex("billing-close", "node-c", Duration.ofSeconds(5)))
                .isTrue();
    }

    private static CronTask sampleCronTask(String name) {
        return new CronTask(
                name,
                new CronTask.Trigger.Interval(Duration.ofMinutes(5)),
                "com.example.Cleanup",
                new JobArgument("java.lang.String", "\"payload\""),
                "system",
                7,
                CronTask.MissedRunPolicy.CATCH_UP,
                ZoneId.of("UTC"),
                true);
    }

    private static final class FailingOnProcessingSerializer implements JobSerializer {
        private final JsonJobSerializer delegate = new JsonJobSerializer();

        @Override
        public String serializeJob(JobSnapshot snapshot, long maxBytes) {
            if (snapshot.currentState() == JobState.PROCESSING) {
                throw new IllegalStateException("prepared claim serialization failed");
            }
            return delegate.serializeJob(snapshot, maxBytes);
        }

        @Override
        public String serializeJob(JobSnapshot snapshot, JobStoreCapabilities capabilities) {
            if (snapshot.currentState() == JobState.PROCESSING) {
                throw new IllegalStateException("prepared claim serialization failed");
            }
            return delegate.serializeJob(snapshot, capabilities);
        }

        @Override
        public Job deserializeJob(String wire) {
            return delegate.deserializeJob(wire);
        }

        @Override
        public JobArgument serializeArgument(Object value) {
            return delegate.serializeArgument(value);
        }

        @Override
        public Object deserializeArgument(JobArgument argument) {
            return delegate.deserializeArgument(argument);
        }

        @Override
        public JobArgument serializePayload(JobPayload payload) {
            return delegate.serializePayload(payload);
        }

        @Override
        public <P extends JobPayload> P deserializePayload(JobArgument argument, Class<P> type) {
            return delegate.deserializePayload(argument, type);
        }
    }
}
