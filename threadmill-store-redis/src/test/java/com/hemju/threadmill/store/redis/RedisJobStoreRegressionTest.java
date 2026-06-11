package com.hemju.threadmill.store.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobEngineFatalException;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobRelationship;
import com.hemju.threadmill.core.JobReplacement;
import com.hemju.threadmill.core.JobSnapshot;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.StoreCapacityExceededException;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.core.store.JobStoreCapabilities;
import com.hemju.threadmill.store.redis.RedisStoreConfig.RedisSafetyValidation;
import com.hemju.threadmill.store.redis.RedisStoreConfig.Standalone;

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
        adminConnection.sync().configSet("maxmemory", "0");
        adminConnection.sync().configSet("maxmemory-policy", "noeviction");
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

    @ParameterizedTest
    @ValueSource(
            strings = {
                "allkeys-lru",
                "allkeys-lfu",
                "allkeys-random",
                "volatile-lru",
                "volatile-lfu",
                "volatile-random",
                "volatile-ttl"
            })
    void startupRejectsEvictionPolicy(String policy) {
        adminConnection.sync().configSet("maxmemory-policy", policy);
        assertThatThrownBy(() -> new RedisJobStore(uri))
                .isInstanceOf(JobEngineFatalException.class)
                .hasMessageContaining("noeviction");
    }

    @Test
    void failedSafetyValidationDoesNotLeakLettuceResources() throws Exception {
        // A wrong eviction policy is the EXPECTED failure mode on
        // misconfigured Redis (and apps retry startup): the owned client and
        // its Netty event loops must be torn down, not abandoned.
        adminConnection.sync().configSet("maxmemory-policy", "allkeys-lru");
        try {
            long before = lettuceThreadCount();
            for (int i = 0; i < 3; i++) {
                assertThatThrownBy(() -> new RedisJobStore(uri)).isInstanceOf(JobEngineFatalException.class);
            }
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (lettuceThreadCount() > before && System.nanoTime() < deadline) {
                Thread.sleep(100);
            }
            assertThat(lettuceThreadCount()).isLessThanOrEqualTo(before);
        } finally {
            adminConnection.sync().configSet("maxmemory-policy", "noeviction");
        }
    }

    private static long lettuceThreadCount() {
        return Thread.getAllStackTraces().keySet().stream()
                .map(Thread::getName)
                .filter(name -> name.startsWith("lettuce-"))
                .count();
    }

    @Test
    void externallyValidatedModeAllowsStartupAgainstEvictionPolicy() {
        adminConnection.sync().configSet("maxmemory-policy", "allkeys-lru");
        var config = new Standalone(uri, RedisSafetyValidation.externallyValidatedMode());

        RedisJobStore opened = new RedisJobStore(config);
        try {
            assertThat(opened.capabilities()).isNotNull();
        } finally {
            opened.close();
        }
    }

    @Test
    void writableProbeTranslatesRedisOom() {
        JobStore store = store();
        adminConnection.sync().configSet("maxmemory", "1");

        assertThatThrownBy(store::verifyWritable).isInstanceOf(StoreCapacityExceededException.class);
    }

    @Test
    void replaceJobMovesIdAcrossByHandlerIndexWhenHandlerSignatureChanges() {
        JobStore store = store();
        Job j = Job.builder()
                .spec(JobSpec.of("com.example.OldHandler", new JobArgument("java.lang.String", "\"x\"")))
                .build();
        store.insert(j);

        var sync = adminConnection.sync();
        assertThat(sync.smembers(RedisKeys.byHandler("com.example.OldHandler"))).containsExactly(j.id().toString());
        assertThat(sync.smembers(RedisKeys.byHandler("com.example.NewHandler"))).doesNotContain(j.id().toString());

        var newSpec = JobSpec.of("com.example.NewHandler", new JobArgument("java.lang.String", "\"x\""));
        assertThat(store.replaceJob(j.id(), j.version(), JobReplacement.ofSpec(newSpec)))
                .isTrue();

        assertThat(sync.smembers(RedisKeys.byHandler("com.example.OldHandler"))).doesNotContain(j.id().toString());
        assertThat(sync.smembers(RedisKeys.byHandler("com.example.NewHandler"))).containsExactly(j.id().toString());
        assertThat(store.findByHandlerSignature("com.example.OldHandler", 10)).isEmpty();
        assertThat(store.findByHandlerSignature("com.example.NewHandler", 10))
                .extracting(loaded -> loaded.id().toString())
                .containsExactly(j.id().toString());
    }

    @Test
    void replaceJobLeavesByHandlerIndexUnchangedWhenHandlerSignatureIsSame() {
        JobStore store = store();
        Job j = Job.builder()
                .spec(JobSpec.of("com.example.SameHandler", new JobArgument("java.lang.String", "\"a\"")))
                .build();
        store.insert(j);

        var sameHandlerNewPayload = JobSpec.of("com.example.SameHandler", new JobArgument("java.lang.String", "\"b\""));
        assertThat(store.replaceJob(j.id(), j.version(), JobReplacement.ofSpec(sameHandlerNewPayload)))
                .isTrue();

        assertThat(adminConnection.sync().smembers(RedisKeys.byHandler("com.example.SameHandler")))
                .containsExactly(j.id().toString());
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
        assertRedisIndexesConsistent();
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
    void keyedInsertWaitsForConcurrencyClaimLockAndPreservesWorkflowHold() throws Exception {
        JobStore store = store();
        Job root = Job.builder()
                .spec(JobSpec.of("com.example.Root", new JobArgument("java.lang.String", "\"x\"")))
                .concurrencyKey("project:locked")
                .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
                .build();
        store.insert(root);
        Job claimedRoot =
                store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);

        String lockKey = RedisKeys.concurrencyClaimLock("project:locked");
        String token = UUID.randomUUID().toString();
        assertThat(adminConnection
                        .sync()
                        .set(lockKey, token, SetArgs.Builder.nx().px(30_000)))
                .isEqualTo("OK");

        Job child = workflowChild("com.example.Child", claimedRoot.id());
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var insert = executor.submit(() -> {
                store.insert(child);
                return null;
            });
            Thread.sleep(150);
            assertThat(insert)
                    .as("keyed insert waits while a same-key claim is preparing")
                    .isNotDone();

            adminConnection.sync().del(lockKey);
            insert.get(10, TimeUnit.SECONDS);
        } finally {
            adminConnection.sync().del(lockKey);
        }

        finish(store, claimedRoot, JobState.SUCCEEDED);
        promote(store, child.id());
        Job outsider = Job.builder()
                .spec(JobSpec.of("com.example.Other", new JobArgument("java.lang.String", "\"x\"")))
                .concurrencyKey("project:locked")
                .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
                .build();
        store.insert(outsider);

        List<Job> claimedChild = store.claimReady(NodeId.newId(), "default", 2, Instant.now());
        assertThat(claimedChild).extracting(Job::id).containsExactly(child.id());
        assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                .isEmpty();

        finish(store, claimedChild.get(0), JobState.SUCCEEDED);
        assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                .extracting(Job::id)
                .containsExactly(outsider.id());
        assertRedisIndexesConsistent();
    }

    @Test
    void workflowCountsTrackChildrenBeforeFirstClaimAndReleaseAfterLastTerminalJob() {
        JobStore store = store();
        Job root = keyedJob("com.example.Root", "project:counts", ConcurrencyMode.EXCLUSIVE);
        store.insert(root);
        Job childA = workflowChild("com.example.ChildA", root.id());
        Job childB = workflowChild("com.example.ChildB", root.id());
        store.insertAll(List.of(childA, childB));

        assertWorkflowCount("project:counts", root.id(), 3);
        assertActiveWorkflowHold("project:counts", root.id(), null);

        Job claimedRoot =
                store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        assertActiveWorkflowHold("project:counts", root.id(), 3);

        finish(store, claimedRoot, JobState.SUCCEEDED);
        assertWorkflowCount("project:counts", root.id(), 2);
        assertActiveWorkflowHold("project:counts", root.id(), 2);

        promote(store, childA.id());
        promote(store, childB.id());
        List<Job> claimedChildren = store.claimReady(NodeId.newId(), "default", 2, Instant.now());
        assertThat(claimedChildren).extracting(Job::id).containsExactlyInAnyOrder(childA.id(), childB.id());
        finish(store, claimedChildren.get(0), JobState.SUCCEEDED);
        assertWorkflowCount("project:counts", root.id(), 1);
        assertActiveWorkflowHold("project:counts", root.id(), 1);
        finish(store, claimedChildren.get(1), JobState.SUCCEEDED);
        assertWorkflowCount("project:counts", root.id(), null);
        assertActiveWorkflowHold("project:counts", root.id(), null);
        assertRedisIndexesConsistent();
    }

    @Test
    void workflowCountsTrackRetryBackIntoPendingState() {
        JobStore store = store();
        Job job = keyedJob("com.example.Retry", "project:retry", ConcurrencyMode.EXCLUSIVE);
        store.insert(job);
        assertWorkflowCount("project:retry", job.id(), 1);

        Job claimed =
                store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        finish(store, claimed, JobState.FAILED);
        assertWorkflowCount("project:retry", job.id(), null);
        assertActiveWorkflowHold("project:retry", job.id(), null);

        Job retry = store.findById(job.id()).orElseThrow();
        long version = retry.version();
        retry.transitionTo(JobState.SCHEDULED, Instant.now(), "test.retry", null);
        store.saveAtomic(retry, version);
        assertWorkflowCount("project:retry", job.id(), 1);

        promote(store, job.id());
        Job reclaimed =
                store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        finish(store, reclaimed, JobState.SUCCEEDED);
        assertWorkflowCount("project:retry", job.id(), null);
        assertActiveWorkflowHold("project:retry", job.id(), null);
        assertRedisIndexesConsistent();
    }

    @Test
    void workflowCountsTrackSoftDeleteOfPendingKeyedJob() {
        JobStore store = store();
        Job job = keyedJob("com.example.Delete", "project:delete", ConcurrencyMode.EXCLUSIVE);
        store.insert(job);
        assertWorkflowCount("project:delete", job.id(), 1);

        assertThat(store.softDelete(job.id())).isTrue();

        assertWorkflowCount("project:delete", job.id(), null);
        assertActiveWorkflowHold("project:delete", job.id(), null);
        assertRedisIndexesConsistent();
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
        assertRedisIndexesConsistent();
    }

    @Test
    void blockedConcurrencyClaimLeavesPendingJobEnqueuedAndIndexesIntact() {
        JobStore store = store();
        String key = "project:blocking";
        Job exclusive = keyedJob("com.example.Exclusive", key, ConcurrencyMode.EXCLUSIVE);
        Job shared = keyedJob("com.example.Shared", key, ConcurrencyMode.SHARED);
        store.insert(exclusive);
        store.insert(shared);

        Job claimedExclusive =
                store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        assertThat(claimedExclusive.id()).isEqualTo(exclusive.id());
        assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                .as("shared job must wait behind active exclusive")
                .isEmpty();

        RedisCommands<String, String> r = adminConnection.sync();
        assertThat(r.hget(RedisKeys.job(shared.id()), "state")).isEqualTo("ENQUEUED");
        assertThat(r.zscore(RedisKeys.queue("default"), shared.id().toString())).isNotNull();
        assertThat(r.zscore(
                        RedisKeys.concurrencyPending(key),
                        RedisKeys.concurrencyPendingMember(ConcurrencyMode.SHARED, shared.id())))
                .isNotNull();
        assertThat(r.hget(RedisKeys.COUNTS, "ENQUEUED")).isEqualTo("1");
        assertThat(r.hget(RedisKeys.COUNTS, "PROCESSING")).isEqualTo("1");
        assertRedisIndexesConsistent();
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
            assertRedisIndexesConsistent();
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
        assertRedisIndexesConsistent();
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
        assertRedisIndexesConsistent();
    }

    @Test
    void softDeleteRacingAClaimNeverCommitsAgainstAStaleRead() throws Exception {
        // soft_delete.lua was the only unversioned read-modify-write: a claim
        // landing between the HGETALL and the EVAL committed against stale
        // ARGVs — decrementing the wrong state count, leaving the job dangling
        // in PROCESSING indexes, and releasing live concurrency holds. The
        // script now compares the expected version and the Java side retries.
        JobStore store = store();
        var node = NodeId.newId();
        for (int round = 0; round < 40; round++) {
            Job job = Job.builder()
                    .spec(JobSpec.of("com.example.Raced", new JobArgument("java.lang.String", "\"x\"")))
                    .concurrencyKey("race:" + round)
                    .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
                    .build();
            store.insert(job);

            var start = new CountDownLatch(1);
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                Future<?> claimer = executor.submit(() -> {
                    start.await();
                    return store.claimReady(node, "default", 1, Instant.now());
                });
                Future<?> deleter = executor.submit(() -> {
                    start.await();
                    return store.softDelete(job.id());
                });
                start.countDown();
                claimer.get(30, TimeUnit.SECONDS);
                deleter.get(30, TimeUnit.SECONDS);
            }

            RedisCommands<String, String> r = adminConnection.sync();
            Job after = store.findById(job.id()).orElseThrow();
            if (after.currentState() == JobState.DELETED) {
                // The delete won (before or after the claim): nothing may
                // linger in the PROCESSING indexes.
                assertThat(r.zscore(RedisKeys.PROCESSING_ALL, job.id().toString()))
                        .isNull();
                assertThat(r.hget(RedisKeys.COUNTS, "PROCESSING")).isIn(null, "0");
            } else {
                // The claim won and the delete observed the post-claim state:
                // the job is genuinely PROCESSING with its exclusive hold held.
                assertThat(after.currentState()).isEqualTo(JobState.PROCESSING);
                assertThat(r.zscore(RedisKeys.PROCESSING_ALL, job.id().toString()))
                        .isNotNull();
                // EXCLUSIVE serialization must remain intact: a second job for
                // the same key cannot claim while the first still runs.
                Job rival = Job.builder()
                        .spec(JobSpec.of("com.example.Rival", new JobArgument("java.lang.String", "\"x\"")))
                        .concurrencyKey("race:" + round)
                        .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
                        .build();
                store.insert(rival);
                assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                        .isEmpty();
                assertThat(store.softDelete(rival.id())).isTrue();
                assertThat(store.softDelete(job.id())).isTrue();
            }
            assertRedisIndexesConsistent();
        }
    }

    @Test
    void heartbeatTouchDropsDanglingIdsInsteadOfResurrectingThem() {
        JobStore store = store();
        var node = NodeId.newId();
        Job real = sample();
        store.insert(real);
        store.claimReady(node, "default", 1, Instant.now());

        // Seed a dangling id (no job hash) in the per-node and global
        // PROCESSING ZSETs, as a partial deletion would leave behind.
        RedisCommands<String, String> r = adminConnection.sync();
        String dangling = JobId.newId().toString();
        r.zadd(RedisKeys.processingFor(node), 1.0, dangling);
        r.zadd(RedisKeys.PROCESSING_ALL, 1.0, dangling);

        store.touchOwnerHeartbeat(node, Instant.now());

        // The dangling id is removed from both indexes instead of rescored;
        // the real job keeps its fresh heartbeat.
        assertThat(r.zscore(RedisKeys.processingFor(node), dangling)).isNull();
        assertThat(r.zscore(RedisKeys.PROCESSING_ALL, dangling)).isNull();
        assertThat(r.zscore(RedisKeys.PROCESSING_ALL, real.id().toString())).isNotNull();
    }

    @Test
    void findDueForPromotionSelfHealsDanglingIdsAndStillReturnsDueJobs() {
        JobStore store = store();
        Job scheduled = Job.builder()
                .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
                .initialState(JobState.SCHEDULED)
                .scheduledFor(Instant.now().minusSeconds(1))
                .createdAt(Instant.now().minusSeconds(1))
                .build();
        store.insert(scheduled);

        // Seed a dangling id (no job hash) at the bottom of the due window, as a
        // partial deletion would leave behind.
        RedisCommands<String, String> r = adminConnection.sync();
        String dangling = JobId.newId().toString();
        r.zadd(RedisKeys.SCHEDULED, 1.0, dangling);

        List<Job> due = store.findDueForPromotion(Instant.now(), 100);

        assertThat(due)
                .extracting(j -> j.id().toString())
                .containsExactly(scheduled.id().toString());
        // The dangling id is healed out of the SCHEDULED index, not left to
        // consume a unit of the promotion budget every tick.
        assertThat(r.zscore(RedisKeys.SCHEDULED, dangling)).isNull();
    }

    @Test
    void findOrphanedSelfHealsDanglingIdsAndStillReturnsRealOrphans() {
        JobStore store = store();
        var node = NodeId.newId();
        Job orphan = sample();
        store.insert(orphan);
        store.claimReady(node, "default", 1, Instant.now());

        // Backdate the real orphan's heartbeat and seed dangling ids at the
        // very bottom of the scan window — historically they consumed the
        // orphan-scan budget on every cycle because loadJobs silently
        // skipped missing bodies.
        RedisCommands<String, String> r = adminConnection.sync();
        r.zadd(RedisKeys.PROCESSING_ALL, 1.0, JobId.newId().toString());
        r.zadd(RedisKeys.PROCESSING_ALL, 2.0, JobId.newId().toString());
        r.zadd(RedisKeys.PROCESSING_ALL, 3.0, orphan.id().toString());

        List<Job> orphans = store.findOrphaned(Instant.now().minusSeconds(1), 3);
        assertThat(orphans).extracting(Job::id).containsExactly(orphan.id());
        // The dangling ids are gone; the next scan window is clean.
        assertThat(r.zcard(RedisKeys.PROCESSING_ALL)).isEqualTo(1L);
    }

    @Test
    void dedupSweepDoesNotDeleteARecordReplacedByAConcurrentProducer() {
        // Deterministic replay of the maintenance-vs-producer interleave:
        // the sweeper reads an expired record referencing terminal job A;
        // before its delete lands, a producer's enqueue_if_absent atomically
        // replaces the record with fresh job B. The sweeper's delete carries
        // A as the expected id and must leave B's record alone.
        JobStore store = store();
        Job a = Job.builder()
                .spec(JobSpec.of("com.example.Dedup", new JobArgument("java.lang.String", "\"x\"")))
                .build();
        assertThat(store.enqueueIfAbsent(a, "report-42", Duration.ofMillis(1), Instant.now()))
                .isInstanceOf(EnqueueResult.Created.class);
        assertThat(store.softDelete(a.id())).isTrue();

        // Producer replaces the expired record (job A terminal) with job B.
        Job b = Job.builder()
                .spec(JobSpec.of("com.example.Dedup", new JobArgument("java.lang.String", "\"x\"")))
                .build();
        assertThat(store.enqueueIfAbsent(b, "report-42", Duration.ofMinutes(5), Instant.now()))
                .isInstanceOf(EnqueueResult.Created.class);

        // The racing sweeper's compare-and-delete with the stale expected id.
        RedisCommands<String, String> r = adminConnection.sync();
        String recordKey = RedisKeys.dedup("default", "report-42");
        Long deleted = r.eval(
                LuaScripts.dedupDelete(),
                ScriptOutputType.INTEGER,
                new String[] {recordKey, RedisKeys.dedupExpiry()},
                a.id().toString());
        assertThat(deleted).isEqualTo(0L);
        assertThat(r.hget(recordKey, "job_id")).isEqualTo(b.id().toString());

        // A third producer must still coalesce onto B inside its TTL window.
        Job c = Job.builder()
                .spec(JobSpec.of("com.example.Dedup", new JobArgument("java.lang.String", "\"x\"")))
                .build();
        EnqueueResult third = store.enqueueIfAbsent(c, "report-42", Duration.ofMinutes(5), Instant.now());
        assertThat(third).isInstanceOf(EnqueueResult.Coalesced.class);
        assertThat(((EnqueueResult.Coalesced) third).existingId()).isEqualTo(b.id());
    }

    @Test
    void findAwaitingByParentScalesBeyondTheGlobalAwaitingWindow() {
        // The historical implementation scanned only the first max*4 entries
        // of the single global AWAITING ZSET; with > ~400 AWAITING jobs the
        // children of newer workflows sorted last and were never found —
        // stranded forever. The per-parent SET makes the lookup exact.
        JobStore store = store();
        for (int i = 0; i < 450; i++) {
            store.insert(Job.builder()
                    .spec(JobSpec.of("com.example.Filler", new JobArgument("java.lang.String", "\"x\"")))
                    .initialState(JobState.AWAITING)
                    .build());
        }
        Job parent = sample();
        store.insert(parent);
        var children = new ArrayList<JobId>();
        for (int i = 0; i < 5; i++) {
            Job child = Job.builder()
                    .spec(JobSpec.of("com.example.LateChild", new JobArgument("java.lang.String", "\"x\"")))
                    .relationship(new JobRelationship(parent.id(), JobRelationship.Kind.WORKFLOW_STEP))
                    .initialState(JobState.AWAITING)
                    .build();
            store.insert(child);
            children.add(child.id());
        }

        List<Job> found = store.findAwaitingByParent(parent.id(), 100);
        assertThat(found).extracting(Job::id).containsExactlyInAnyOrderElementsOf(children);

        // Index hygiene: promotion and deletion both remove the entry.
        Job promoted = store.findById(children.get(0)).orElseThrow();
        long v = promoted.version();
        promoted.transitionTo(JobState.ENQUEUED, Instant.now(), "test.promote", null);
        store.saveAtomic(promoted, v);
        assertThat(store.softDelete(children.get(1))).isTrue();

        found = store.findAwaitingByParent(parent.id(), 100);
        assertThat(found).extracting(Job::id).containsExactlyInAnyOrderElementsOf(children.subList(2, children.size()));
        assertRedisIndexesConsistent();
    }

    @Test
    void queueRegistryIsMaintainedInsideTheScriptsAndPrunedWhenEmpty() {
        JobStore store = store();
        RedisCommands<String, String> r = adminConnection.sync();

        // Membership lands atomically with the insert script itself.
        Job job = Job.builder()
                .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
                .queue("burst:q")
                .build();
        store.insert(job);
        assertThat(r.smembers(RedisKeys.QUEUES)).contains("burst:q");

        // Drain the queue; the discovery path prunes the registry entry
        // (atomically against concurrent inserts) instead of growing forever.
        store.claimReady(NodeId.newId(), "burst:q", 1, Instant.now());
        assertThat(store.queueDepths()).doesNotContainKey("burst:q");
        assertThat(r.smembers(RedisKeys.QUEUES)).doesNotContain("burst:q");

        // A later insert to the same queue re-registers it from the script.
        Job again = Job.builder()
                .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
                .queue("burst:q")
                .build();
        store.insert(again);
        assertThat(r.smembers(RedisKeys.QUEUES)).contains("burst:q");
        assertThat(store.listEnqueuedQueues()).contains("burst:q");
    }

    @Test
    void retentionDeleteSkipsJobsThatLeftTheTerminalStateAndKeepsCountsExact() {
        JobStore store = store();
        RedisCommands<String, String> r = adminConnection.sync();
        Job job = sample();
        store.insert(job);
        Job claimed =
                store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        long v = claimed.version();
        claimed.transitionTo(JobState.SUCCEEDED, Instant.now(), "test", null);
        claimed.clearOwner();
        store.saveAtomic(claimed, v);

        // A racing operator delete moves the job out of SUCCEEDED after the
        // sweeper's scan observed it.
        assertThat(store.softDelete(job.id())).isTrue();

        // The sweeper's stale per-job delete must skip: state re-check fails.
        Long deleted = r.eval(
                LuaScripts.retentionDelete(),
                ScriptOutputType.INTEGER,
                new String[] {
                    RedisKeys.job(job.id()),
                    RedisKeys.byStateTime(JobState.SUCCEEDED),
                    RedisKeys.COUNTS,
                    RedisKeys.byHandler(claimed.spec().handlerType())
                },
                job.id().toString(),
                JobState.SUCCEEDED.name());
        assertThat(deleted).isEqualTo(0L);
        assertThat(store.findById(job.id())).isPresent();
        assertThat(r.hget(RedisKeys.COUNTS, "DELETED")).isEqualTo("1");
        assertThat(r.hget(RedisKeys.COUNTS, "SUCCEEDED")).isEqualTo("0");
        assertRedisIndexesConsistent();

        // The normal retention path for the job's actual state still works.
        assertThat(store.deleteFinishedOlderThan(Instant.now().plusSeconds(3600), JobState.DELETED, 10))
                .isEqualTo(1L);
        assertThat(store.findById(job.id())).isEmpty();
    }

    @Test
    void oldestEnqueuedAtReportsTheOldestJobNotTheOldestHighestPriorityJob() throws Exception {
        JobStore store = store();
        // Older low-priority job first...
        Job oldLow = Job.builder()
                .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
                .priority(-5)
                .build();
        store.insert(oldLow);
        Thread.sleep(20);
        // ...then a fresh high-priority job that sorts FIRST in the queue ZSET.
        Job freshHigh = Job.builder()
                .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
                .priority(50)
                .build();
        store.insert(freshHigh);

        Instant oldLowAt = Instant.ofEpochMilli(
                Long.parseLong(adminConnection.sync().hget(RedisKeys.job(oldLow.id()), "current_state_at")));
        // The age gauge must see the starving low-priority job, not the
        // queue head.
        assertThat(store.oldestEnqueuedAt("default")).contains(oldLowAt);
    }

    @Test
    void insertAllRejectsIntraBatchDuplicateIdsBeforeAnyWrite() {
        JobStore store = store();
        Job a = sample();
        Job duplicate = Job.builder()
                .id(a.id())
                .spec(JobSpec.of("com.example.Other", new JobArgument("java.lang.String", "\"x\"")))
                .build();

        assertThatThrownBy(() -> store.insertAll(List.of(a, duplicate)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate job id");

        // Nothing was written and counts did not double-increment.
        assertThat(store.findById(a.id())).isEmpty();
        assertThat(adminConnection.sync().hget(RedisKeys.COUNTS, "ENQUEUED")).isIn(null, "0");
        assertThat(a.version()).isZero();
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
    void dropThreadmillKeysRemovesOnlyThreadmillNamespace() {
        long removed;
        var store = new RedisJobStore(uri);
        try {
            Job job = sample();
            CronTask task = sampleCronTask("nightly-cleanup");
            store.insert(job);
            store.upsertCronTask(task);
            store.recordCronTaskOwnership("billing", task.name());
            store.pauseQueue("default", "maintenance");
            adminConnection.sync().set("other-app:keep", "yes");

            removed = store.dropThreadmillKeys();
        } finally {
            store.close();
        }

        assertThat(removed).isPositive();
        assertThat(adminConnection.sync().keys(RedisKeys.PREFIX + "*")).isEmpty();
        assertThat(adminConnection.sync().get("other-app:keep")).isEqualTo("yes");
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

    private static Job workflowChild(String handler, JobId parentId) {
        return Job.builder()
                .spec(JobSpec.of(handler, new JobArgument("java.lang.String", "\"x\"")))
                .relationship(new JobRelationship(parentId, JobRelationship.Kind.WORKFLOW_STEP))
                .initialState(JobState.AWAITING)
                .build();
    }

    private static Job keyedJob(String handler, String key, ConcurrencyMode mode) {
        return Job.builder()
                .spec(JobSpec.of(handler, new JobArgument("java.lang.String", "\"x\"")))
                .concurrencyKey(key)
                .concurrencyMode(mode)
                .build();
    }

    private static void assertWorkflowCount(String key, JobId rootId, Integer expected) {
        assertThat(adminConnection.sync().hget(RedisKeys.concurrencyWorkflowCounts(key), rootId.toString()))
                .isEqualTo(expected == null ? null : expected.toString());
    }

    private static void assertActiveWorkflowHold(String key, JobId rootId, Integer expected) {
        assertThat(adminConnection.sync().hget(RedisKeys.concurrencyWorkflows(key), rootId.toString()))
                .isEqualTo(expected == null ? null : expected.toString());
    }

    private static void assertRedisIndexesConsistent() {
        RedisCommands<String, String> r = adminConnection.sync();
        var serializer = new JsonJobSerializer();
        var expectedCounts = new EnumMap<JobState, Long>(JobState.class);
        for (JobState state : JobState.values()) expectedCounts.put(state, 0L);
        var expectedPending = new HashMap<String, Set<String>>();
        var expectedSharedInFlight = new HashMap<String, Long>();
        var expectedExclusiveInFlight = new HashMap<String, Long>();
        var expectedWorkflowCounts = new HashMap<String, Map<String, Long>>();
        var expectedWorkflowHolds = new HashMap<String, Map<String, Long>>();

        for (String jobKey : r.keys(RedisKeys.PREFIX + "job:*")) {
            Map<String, String> hash = r.hgetall(jobKey);
            var id = JobId.parse(jobKey.substring((RedisKeys.PREFIX + "job:").length()));
            JobState state = JobState.valueOf(hash.get("state"));
            Job body = serializer.deserializeJob(hash.get("body"));
            assertThat(body.currentState()).as("body state for " + id).isEqualTo(state);
            expectedCounts.merge(state, 1L, Long::sum);
            assertThat(r.zscore(RedisKeys.byStateTime(state), id.toString()))
                    .as("by-state index for " + id)
                    .isNotNull();
            for (JobState otherState : JobState.values()) {
                if (otherState == state) continue;
                assertThat(r.zscore(RedisKeys.byStateTime(otherState), id.toString()))
                        .as("by-state index leak for " + id + " in " + otherState)
                        .isNull();
            }

            String queue = hash.get("queue");
            assertActiveStateMembership(r, id, state, queue, hash.get("owner_node_id"));
            assertConcurrencyMembership(
                    r,
                    id,
                    state,
                    hash.get("concurrency_key"),
                    hash.get("concurrency_mode"),
                    hash.get("workflow_root_id"),
                    expectedPending,
                    expectedSharedInFlight,
                    expectedExclusiveInFlight,
                    expectedWorkflowCounts,
                    expectedWorkflowHolds);
        }

        for (JobState state : JobState.values()) {
            assertThat(parseLong(r.hget(RedisKeys.COUNTS, state.name())))
                    .as("count for " + state)
                    .isEqualTo(expectedCounts.get(state));
        }
        assertConcurrencyStructures(
                r,
                expectedPending,
                expectedSharedInFlight,
                expectedExclusiveInFlight,
                expectedWorkflowCounts,
                expectedWorkflowHolds);
    }

    private static void assertActiveStateMembership(
            RedisCommands<String, String> r, JobId id, JobState state, String queue, String ownerNodeId) {
        if (state == JobState.ENQUEUED) {
            assertThat(r.zscore(RedisKeys.queue(queue), id.toString()))
                    .as("queue index for " + id)
                    .isNotNull();
        } else {
            assertThat(r.zscore(RedisKeys.queue(queue), id.toString()))
                    .as("queue index leak for " + id)
                    .isNull();
        }
        if (state == JobState.SCHEDULED) {
            assertThat(r.zscore(RedisKeys.SCHEDULED, id.toString()))
                    .as("scheduled index for " + id)
                    .isNotNull();
        } else {
            assertThat(r.zscore(RedisKeys.SCHEDULED, id.toString()))
                    .as("scheduled index leak for " + id)
                    .isNull();
        }
        if (state == JobState.AWAITING) {
            assertThat(r.zscore(RedisKeys.AWAITING, id.toString()))
                    .as("awaiting index for " + id)
                    .isNotNull();
        } else {
            assertThat(r.zscore(RedisKeys.AWAITING, id.toString()))
                    .as("awaiting index leak for " + id)
                    .isNull();
        }
        if (state == JobState.PROCESSING) {
            assertThat(r.zscore(RedisKeys.PROCESSING_ALL, id.toString()))
                    .as("processing index for " + id)
                    .isNotNull();
            assertThat(r.zscore(RedisKeys.processingFor(NodeId.of(UUID.fromString(ownerNodeId))), id.toString()))
                    .as("owner processing index for " + id)
                    .isNotNull();
        } else {
            assertThat(r.zscore(RedisKeys.PROCESSING_ALL, id.toString()))
                    .as("processing index leak for " + id)
                    .isNull();
        }
    }

    private static void assertConcurrencyMembership(
            RedisCommands<String, String> r,
            JobId id,
            JobState state,
            String key,
            String mode,
            String workflowRootId,
            Map<String, Set<String>> expectedPending,
            Map<String, Long> expectedSharedInFlight,
            Map<String, Long> expectedExclusiveInFlight,
            Map<String, Map<String, Long>> expectedWorkflowCounts,
            Map<String, Map<String, Long>> expectedWorkflowHolds) {
        if (key == null || key.isBlank()) return;
        var concurrencyMode = ConcurrencyMode.valueOf(mode);
        expectedPending.computeIfAbsent(key, ignored -> new HashSet<>());
        expectedSharedInFlight.putIfAbsent(key, 0L);
        expectedExclusiveInFlight.putIfAbsent(key, 0L);
        expectedWorkflowCounts.computeIfAbsent(key, ignored -> new HashMap<>());
        expectedWorkflowHolds.computeIfAbsent(key, ignored -> new HashMap<>());
        String member = RedisKeys.concurrencyPendingMember(concurrencyMode, id);
        if (state == JobState.ENQUEUED || state == JobState.SCHEDULED || state == JobState.AWAITING) {
            assertThat(r.zscore(RedisKeys.concurrencyPending(key), member))
                    .as("concurrency pending member for " + id)
                    .isNotNull();
            expectedPending.get(key).add(member);
        } else {
            assertThat(r.zscore(RedisKeys.concurrencyPending(key), member))
                    .as("concurrency pending leak for " + id)
                    .isNull();
        }
        if (state == JobState.PROCESSING) {
            if (concurrencyMode == ConcurrencyMode.SHARED) {
                expectedSharedInFlight.merge(key, 1L, Long::sum);
            } else {
                expectedExclusiveInFlight.merge(key, 1L, Long::sum);
            }
            expectedWorkflowHolds.get(key).merge(workflowRootId, 1L, Long::sum);
        }
        if (!isTerminal(state)) {
            expectedWorkflowCounts.get(key).merge(workflowRootId, 1L, Long::sum);
        }
    }

    private static void assertConcurrencyStructures(
            RedisCommands<String, String> r,
            Map<String, Set<String>> expectedPending,
            Map<String, Long> expectedSharedInFlight,
            Map<String, Long> expectedExclusiveInFlight,
            Map<String, Map<String, Long>> expectedWorkflowCounts,
            Map<String, Map<String, Long>> expectedWorkflowHolds) {
        var keys = new HashSet<String>();
        keys.addAll(expectedPending.keySet());
        keys.addAll(expectedSharedInFlight.keySet());
        keys.addAll(expectedExclusiveInFlight.keySet());
        keys.addAll(expectedWorkflowCounts.keySet());
        keys.addAll(expectedWorkflowHolds.keySet());
        for (String key : keys) {
            assertThat(r.zrange(RedisKeys.concurrencyPending(key), 0, -1))
                    .as("pending index for " + key)
                    .containsExactlyInAnyOrderElementsOf(expectedPending.getOrDefault(key, Set.of()));
            assertThat(parseLong(r.hget(RedisKeys.concurrencyCounters(key), "shared_in_flight")))
                    .as("shared in-flight for " + key)
                    .isEqualTo(expectedSharedInFlight.getOrDefault(key, 0L));
            assertThat(parseLong(r.hget(RedisKeys.concurrencyCounters(key), "exclusive_in_flight")))
                    .as("exclusive in-flight for " + key)
                    .isEqualTo(expectedExclusiveInFlight.getOrDefault(key, 0L));
            assertLongHashEquals(
                    r.hgetall(RedisKeys.concurrencyWorkflowCounts(key)),
                    expectedWorkflowCounts.getOrDefault(key, Map.of()));
            assertLongHashEquals(
                    r.hgetall(RedisKeys.concurrencyWorkflows(key)), expectedWorkflowHolds.getOrDefault(key, Map.of()));
        }
    }

    private static void assertLongHashEquals(Map<String, String> actual, Map<String, Long> expected) {
        var parsed = new HashMap<String, Long>();
        for (var entry : actual.entrySet()) {
            long value = Long.parseLong(entry.getValue());
            if (value != 0L) parsed.put(entry.getKey(), value);
        }
        assertThat(parsed).containsExactlyInAnyOrderEntriesOf(expected);
    }

    private static long parseLong(String value) {
        return value == null ? 0L : Long.parseLong(value);
    }

    private static boolean isTerminal(JobState state) {
        return state == JobState.SUCCEEDED
                || state == JobState.FAILED
                || state == JobState.DELETED
                || state == JobState.QUARANTINED;
    }

    private static void finish(JobStore store, Job job, JobState state) {
        long version = job.version();
        job.transitionTo(state, Instant.now(), "test.finish", null);
        job.clearOwner();
        store.saveAtomic(job, version);
    }

    private static void promote(JobStore store, JobId id) {
        Job job = store.findById(id).orElseThrow();
        long version = job.version();
        job.transitionTo(JobState.ENQUEUED, Instant.now(), "test.promote", null);
        store.saveAtomic(job, version);
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
