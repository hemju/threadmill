package com.hemju.threadmill.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobLog;
import com.hemju.threadmill.core.JobReplacement;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.OversizedJobException;
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.engine.JobInterceptor;
import com.hemju.threadmill.core.engine.WorkflowInterceptor;
import com.hemju.threadmill.core.schedule.CronExpression;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobSearch;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.core.store.NodeHeartbeat;

/**
 * The single contract every {@code JobStore} must satisfy.
 *
 * <p>This class is intentionally written <strong>before</strong> any
 * concrete store. The in-memory, PostgreSQL, and Redis stores all extend it
 * and run an identical suite. Differences that legitimately exist between
 * backends are surfaced through
 * {@link com.hemju.threadmill.core.store.JobStoreCapabilities} and
 * exercised through capability-aware assertions.
 *
 * <p>Subclasses provide a fresh, empty {@code JobStore} per test by
 * implementing {@link #createStore()}.
 */
public abstract class AbstractJobStoreContractTest {

    protected JobStore store;

    /** Subclasses return a brand-new, empty store for each test. */
    protected abstract JobStore createStore();

    /**
     * Optional hook: subclasses may override to release resources between
     * tests. Default is no-op.
     */
    protected void tearDownStore() {}

    @BeforeEach
    void freshStore() {
        store = createStore();
    }

    // ================================================================ store identity

    @Test
    @DisplayName("describe() returns a non-blank operator-facing identifier")
    void describeReturnsNonBlankString() {
        String description = store.describe();
        assertThat(description).isNotNull().isNotBlank();
    }

    // ================================================================ insert / load

    @Test
    @DisplayName("insert then findById round-trips the job exactly")
    void insertAndLoadRoundTrip() {
        Job original = Jobs.enqueued("com.example.SendEmail");
        original.metadata().put("trace", "abc");
        store.insert(original);

        assertThat(original.version()).isEqualTo(1L);
        Optional<Job> loaded = store.findById(original.id());
        assertThat(loaded).isPresent();
        Job j = loaded.get();
        assertThat(j.id()).isEqualTo(original.id());
        assertThat(j.currentState()).isEqualTo(JobState.ENQUEUED);
        assertThat(j.version()).isEqualTo(1L);
        assertThat(j.metadata().get("trace")).contains("abc");
    }

    @Test
    @DisplayName("findById of a vanished id returns Optional.empty (not an exception)")
    void findVanishedJobIsEmpty() {
        assertThat(store.findById(JobId.newId())).isEmpty();
    }

    @Test
    @DisplayName("insert refuses a duplicate id")
    void insertRejectsDuplicates() {
        Job j = Jobs.enqueued("com.example.SendEmail");
        store.insert(j);
        Job dup = Job.builder().id(j.id()).spec(JobSpec.of("com.example.Other")).build();
        assertThatThrownBy(() -> store.insert(dup)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("4-byte Unicode round-trips in metadata")
    void fourByteUnicodeRoundTrip() {
        // 4-byte (supplementary-plane) characters: emoji, ancient script
        String exotic = "shipping ✈ 🚀 𐀀 𐀁 𐀂";
        Job j = Jobs.enqueued("com.example.SendEmail");
        j.metadata().put("note", exotic);
        store.insert(j);

        Job loaded = store.findById(j.id()).orElseThrow();
        assertThat(loaded.metadata().get("note")).contains(exotic);
    }

    // ================================================================ saveAtomic

    @Test
    @DisplayName("saveAtomic with the correct version updates persisted state and advances version")
    void saveAtomicSucceedsOnVersionMatch() {
        Job j = Jobs.enqueued("com.example.SendEmail");
        store.insert(j);

        long initial = j.version();
        j.transitionTo(JobState.PROCESSING, Instant.now());
        j.assignOwner(NodeId.newId(), Instant.now());
        store.saveAtomic(j, initial);

        assertThat(j.version()).isEqualTo(initial + 1);
        Job loaded = store.findById(j.id()).orElseThrow();
        assertThat(loaded.currentState()).isEqualTo(JobState.PROCESSING);
        assertThat(loaded.version()).isEqualTo(initial + 1);
    }

    @Test
    @DisplayName("saveAtomic with a stale version throws StaleJobException and leaves the in-memory version reusable")
    void saveAtomicThrowsOnStaleVersion() {
        Job j = Jobs.enqueued("com.example.SendEmail");
        store.insert(j);
        long initial = j.version();

        // Some other actor advances the persisted version.
        Job sameId = store.findById(j.id()).orElseThrow();
        sameId.transitionTo(JobState.PROCESSING, Instant.now());
        sameId.assignOwner(NodeId.newId(), Instant.now());
        store.saveAtomic(sameId, initial);

        // We still hold a stale view; saving must throw and not corrupt our version.
        j.transitionTo(JobState.PROCESSING, Instant.now());
        assertThatThrownBy(() -> store.saveAtomic(j, initial)).isInstanceOf(StaleJobException.class);
        assertThat(j.version()).isEqualTo(initial); // <-- the critical invariant
    }

    @Test
    @DisplayName("a failed save leaves the in-memory job reusable: a follow-up correct save succeeds")
    void failedSaveLeavesJobReusable() {
        Job j = Jobs.enqueued("com.example.SendEmail");
        store.insert(j);
        long v1 = j.version();

        // Force a save failure via an oversized payload: pile metadata
        // until the serialized form exceeds the cap.
        String bigChunk = "x".repeat(4096);
        for (int i = 0; i < 1000; i++) {
            j.metadata().put("k" + i, bigChunk);
        }
        long capacity = store.capabilities().maxSerializedJobBytes();
        assertThatThrownBy(() -> store.saveAtomic(j, v1)).isInstanceOf(OversizedJobException.class);
        assertThat(j.version()).isEqualTo(v1); // <-- no corruption

        // Recover and re-use the same job object successfully.
        // Need a fresh Job because we mutated metadata into oversize.
        Job recovered = store.findById(j.id()).orElseThrow();
        long v = recovered.version();
        recovered.metadata().put("tracer", "ok-after-failure");
        store.saveAtomic(recovered, v);
        assertThat(recovered.version()).isEqualTo(v + 1);
        assertThat(capacity).isPositive();
    }

    // ================================================================ claimReady

    @Test
    @DisplayName("claimReady moves up to max ENQUEUED jobs in a queue to PROCESSING with the requested owner")
    void claimReadyClaimsAndTransitions() {
        for (int i = 0; i < 5; i++) {
            store.insert(Jobs.enqueued("com.example.SendEmail"));
        }

        NodeId node = NodeId.newId();
        var beat = Instant.now();
        List<Job> claimed = store.claimReady(node, "default", 3, beat);
        assertThat(claimed).hasSize(3);
        for (Job j : claimed) {
            assertThat(j.currentState()).isEqualTo(JobState.PROCESSING);
            assertThat(j.ownerNodeId()).contains(node);
            assertThat(j.ownerHeartbeatAt()).isPresent();
        }
    }

    @Test
    @DisplayName("claimReady never returns the same job to two contending nodes (concurrent)")
    void claimReadyIsAtomicAcrossNodes() throws Exception {
        int total = 100;
        for (int i = 0; i < total; i++) {
            store.insert(Jobs.enqueued("com.example.SendEmail"));
        }

        int workers = 8;
        var start = new CountDownLatch(1);
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        Set<JobId> all = ConcurrentHashMap.newKeySet();
        var collisions = new ConcurrentHashMap<JobId, Integer>();
        try {
            List<java.util.concurrent.Future<List<Job>>> futures = new ArrayList<>();
            for (int w = 0; w < workers; w++) {
                NodeId node = NodeId.newId();
                var beat = Instant.now();
                futures.add(executor.submit(() -> {
                    start.await();
                    List<Job> mine = new ArrayList<>();
                    while (true) {
                        List<Job> got = store.claimReady(node, "default", 7, beat);
                        if (got.isEmpty()) break;
                        mine.addAll(got);
                    }
                    return mine;
                }));
            }
            start.countDown();
            for (var f : futures) {
                for (Job j : f.get(30, TimeUnit.SECONDS)) {
                    if (!all.add(j.id())) {
                        collisions.merge(j.id(), 1, Integer::sum);
                    }
                }
            }
        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
        assertThat(collisions).as("no double-claim across nodes").isEmpty();
        assertThat(all).hasSize(total);
    }

    @Test
    @DisplayName("claimReady only considers the requested queue")
    void claimReadyRespectsQueue() {
        for (int i = 0; i < 3; i++) store.insert(Jobs.onQueue("com.example.SendEmail", "default"));
        for (int i = 0; i < 3; i++) store.insert(Jobs.onQueue("com.example.SendEmail", "high"));

        List<Job> def = store.claimReady(NodeId.newId(), "default", 10, Instant.now());
        assertThat(def).hasSize(3);
        for (Job j : def) assertThat(j.queue()).isEqualTo("default");
    }

    @Test
    @DisplayName("enqueueIfAbsent coalesces concurrent producers onto one job id")
    void enqueueIfAbsentCoalescesConcurrentProducers() throws Exception {
        int producers = 8;
        var start = new CountDownLatch(1);
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        List<java.util.concurrent.Future<EnqueueResult>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < producers; i++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    Job job = Jobs.enqueued("com.example.SendEmail");
                    return store.enqueueIfAbsent(job, "delivery-1", Duration.ofMinutes(5), Instant.now());
                }));
            }
            start.countDown();
            List<EnqueueResult> results = new ArrayList<>();
            for (var future : futures) results.add(future.get(30, TimeUnit.SECONDS));
            List<JobId> created = results.stream()
                    .filter(EnqueueResult.Created.class::isInstance)
                    .map(EnqueueResult.Created.class::cast)
                    .map(EnqueueResult.Created::id)
                    .toList();
            List<JobId> coalesced = results.stream()
                    .filter(EnqueueResult.Coalesced.class::isInstance)
                    .map(EnqueueResult.Coalesced.class::cast)
                    .map(EnqueueResult.Coalesced::existingId)
                    .toList();
            assertThat(created).hasSize(1);
            assertThat(coalesced).hasSize(producers - 1).allMatch(created.get(0)::equals);
            assertThat(store.countsByState().get(JobState.ENQUEUED)).isEqualTo(1L);
        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("expired dedup keys create a new job after the prior job is terminal")
    void expiredDedupKeyCreatesNewJobAfterTerminal() {
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        Job first = Jobs.enqueued("com.example.SendEmail");
        EnqueueResult firstResult = store.enqueueIfAbsent(first, "delivery-2", Duration.ofMillis(10), now);
        assertThat(firstResult).isInstanceOf(EnqueueResult.Created.class);
        first.transitionTo(JobState.PROCESSING, now);
        first.transitionTo(JobState.SUCCEEDED, now);
        store.saveAtomic(first, first.version());

        Job second = Jobs.enqueued("com.example.SendEmail");
        EnqueueResult secondResult =
                store.enqueueIfAbsent(second, "delivery-2", Duration.ofMinutes(5), now.plusSeconds(1));
        assertThat(secondResult).isInstanceOf(EnqueueResult.Created.class);
        assertThat(((EnqueueResult.Created) secondResult).id()).isNotEqualTo(first.id());
    }

    @Test
    @DisplayName("retention keeps a terminal job whose dedup key is still live (TTL > retention age)")
    void retentionKeepsTerminalJobsWithALiveDedupKey() {
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var past = now.minus(Duration.ofDays(1));
        Job job = Jobs.enqueued("com.example.SendEmail");
        assertThat(store.enqueueIfAbsent(job, "invoice-7", Duration.ofMinutes(30), now))
                .isInstanceOf(EnqueueResult.Created.class);

        // Drive the job terminal, backdated so retention would otherwise sweep it.
        Job loaded = store.findById(job.id()).orElseThrow();
        loaded.transitionTo(JobState.PROCESSING, past);
        loaded.transitionTo(JobState.SUCCEEDED, past);
        store.saveAtomic(loaded, loaded.version());

        // Retention must NOT delete it while its dedup key is still live, or the
        // dedup TTL is silently capped at the retention age.
        store.deleteFinishedOlderThan(now, JobState.SUCCEEDED, 100);
        assertThat(store.findById(job.id())).isPresent();

        // A re-enqueue inside the TTL coalesces onto the surviving job, and the
        // returned id resolves (never a dangling reference).
        Job again = Jobs.enqueued("com.example.SendEmail");
        EnqueueResult result = store.enqueueIfAbsent(again, "invoice-7", Duration.ofMinutes(30), now.plusSeconds(1));
        assertThat(result).isInstanceOf(EnqueueResult.Coalesced.class);
        assertThat(((EnqueueResult.Coalesced) result).existingId()).isEqualTo(job.id());
        assertThat(store.findById(((EnqueueResult.Coalesced) result).existingId()))
                .isPresent();
    }

    @Test
    @DisplayName("execution updates persist check-ins, logs, and progress without bumping version")
    void executionUpdatesPersistWithoutBumpingVersion() {
        Job job = Jobs.enqueued("com.example.SendEmail");
        store.insert(job);
        NodeId node = NodeId.newId();
        Job claimed = store.claimReady(node, "default", 1, Instant.now()).get(0);
        long version = claimed.version();
        var checkin = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        claimed.checkIn(checkin);
        claimed.progress().update(0.5, "half");
        claimed.log().info("still alive");

        assertThat(store.saveExecutionUpdate(claimed, node)).isTrue();
        Job loaded = store.findById(claimed.id()).orElseThrow();
        assertThat(loaded.version()).isEqualTo(version);
        assertThat(loaded.lastCheckinAt()).contains(checkin);
        assertThat(loaded.progress().snapshot())
                .hasValueSatisfying(p -> assertThat(p.fraction()).isEqualTo(0.5));
        assertThat(loaded.log().snapshot()).extracting(JobLog.Entry::message).contains("still alive");
    }

    @Test
    @DisplayName("a zombie execution update from a previous attempt is rejected")
    void executionUpdateFromAPreviousAttemptIsRejected() {
        NodeId node = NodeId.newId();
        Job inserted = Jobs.enqueued("com.example.SendEmail");
        store.insert(inserted);

        // Attempt 1, claimed by this node — the stale writer holds this object.
        Job attemptOne = store.claimReady(node, "default", 1, Instant.now()).get(0);
        assertThat(attemptOne.attempts()).isEqualTo(1);

        // Orphan-reclaim shape: attempt 1 fails, is re-enqueued, and re-claimed by
        // the SAME node as attempt 2 (a separate reload, leaving attemptOne stale).
        long v = attemptOne.version();
        Job failed = store.findById(inserted.id()).orElseThrow();
        failed.transitionTo(JobState.FAILED, Instant.now(), "test", "orphaned");
        failed.clearOwner();
        store.saveAtomic(failed, v);
        Job reenqueued = store.findById(inserted.id()).orElseThrow();
        reenqueued.transitionTo(JobState.ENQUEUED, Instant.now(), "test", null);
        store.saveAtomic(reenqueued, reenqueued.version());
        Job attemptTwo = store.claimReady(node, "default", 1, Instant.now()).get(0);
        assertThat(attemptTwo.attempts()).isEqualTo(2);
        attemptTwo.checkIn(Instant.now());
        assertThat(store.saveExecutionUpdate(attemptTwo, node)).isTrue();

        // The attempt-1 writer passes the state and owner checks but must NOT
        // overwrite the live attempt's persisted state.
        attemptOne.log().info("zombie write");
        assertThat(store.saveExecutionUpdate(attemptOne, node)).isFalse();
        assertThat(store.findById(inserted.id()).orElseThrow().attempts()).isEqualTo(2);
    }

    // ================================================================ concurrency

    @Test
    @DisplayName("two SHARED jobs with the same concurrency key can claim together")
    void sharedJobsWithSameConcurrencyKeyClaimTogether() {
        store.insert(Jobs.withConcurrency("com.example.Export", "project:42", ConcurrencyMode.SHARED));
        store.insert(Jobs.withConcurrency("com.example.Export", "project:42", ConcurrencyMode.SHARED));

        List<Job> claimed = store.claimReady(NodeId.newId(), "default", 2, Instant.now());

        assertThat(claimed).hasSize(2);
        assertThat(claimed).allSatisfy(job -> {
            assertThat(job.currentState()).isEqualTo(JobState.PROCESSING);
            assertThat(job.concurrencyKey()).contains("project:42");
            assertThat(job.concurrencyMode()).contains(ConcurrencyMode.SHARED);
        });
    }

    @Test
    @DisplayName("different concurrency keys claim independently regardless of mode")
    void differentConcurrencyKeysClaimIndependently() {
        store.insert(Jobs.withConcurrency("com.example.Import", "project:42", ConcurrencyMode.EXCLUSIVE));
        store.insert(Jobs.withConcurrency("com.example.Import", "project:43", ConcurrencyMode.EXCLUSIVE));

        assertThat(store.claimReady(NodeId.newId(), "default", 2, Instant.now()))
                .hasSize(2);
    }

    @Test
    @DisplayName("an EXCLUSIVE job waits for an active SHARED job with the same concurrency key")
    void exclusiveWaitsForActiveSharedJobWithSameConcurrencyKey() {
        store.insert(Jobs.withConcurrency("com.example.Export", "project:42", ConcurrencyMode.SHARED));
        store.insert(Jobs.withConcurrency("com.example.Import", "project:42", ConcurrencyMode.EXCLUSIVE));

        List<Job> first = store.claimReady(NodeId.newId(), "default", 2, Instant.now());
        assertThat(first).hasSize(1);
        assertThat(first.get(0).concurrencyMode()).contains(ConcurrencyMode.SHARED);
        assertThat(store.claimReady(NodeId.newId(), "default", 2, Instant.now()))
                .isEmpty();

        finish(first.get(0), JobState.SUCCEEDED);
        List<Job> second = store.claimReady(NodeId.newId(), "default", 2, Instant.now());
        assertThat(second).hasSize(1);
        assertThat(second.get(0).concurrencyMode()).contains(ConcurrencyMode.EXCLUSIVE);
    }

    @Test
    @DisplayName("a retried job re-enters the queue at its original creation-time position")
    void retriedJobReentersTheQueueAtTheSamePositionOnEveryBackend() {
        var base = Instant.now().minusSeconds(5);
        // Same priority; a is created before b. Use fixed, ordered ids so the
        // relational backends' id tie-break is deterministic (a < b).
        Job a = concurrentJobWithId("com.example.H", fixedJobId(1), null, null, 0, base);
        Job b = concurrentJobWithId("com.example.H", fixedJobId(2), null, null, 0, base.plusMillis(1));
        store.insert(a);
        store.insert(b);

        // a is claimed first (earliest creation), fails, and is re-enqueued.
        Job claimedA =
                store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        assertThat(claimedA.id()).isEqualTo(a.id());
        finish(claimedA, JobState.FAILED);
        retryToEnqueued(a.id());

        // a re-entered ENQUEUED later in wall-clock time but must keep its place
        // ahead of b by creation order on every backend.
        assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                .extracting(Job::id)
                .containsExactly(a.id());
    }

    @Test
    @DisplayName("EXCLUSIVE jobs with the same concurrency key serialize")
    void exclusiveJobsWithSameConcurrencyKeySerialize() {
        store.insert(Jobs.withConcurrency("com.example.Import", "project:42", ConcurrencyMode.EXCLUSIVE));
        store.insert(Jobs.withConcurrency("com.example.Import", "project:42", ConcurrencyMode.EXCLUSIVE));

        List<Job> first = store.claimReady(NodeId.newId(), "default", 2, Instant.now());
        assertThat(first).hasSize(1);
        assertThat(store.claimReady(NodeId.newId(), "default", 2, Instant.now()))
                .isEmpty();

        finish(first.get(0), JobState.SUCCEEDED);
        assertThat(store.claimReady(NodeId.newId(), "default", 2, Instant.now()))
                .hasSize(1);
    }

    @Test
    @DisplayName("retried standalone EXCLUSIVE jobs for the same key still serialize")
    void retriedStandaloneExclusiveJobsStillSerialize() {
        var base = Instant.now().minusSeconds(5);
        Job a = concurrentJob("com.example.Import", "project:42", ConcurrencyMode.EXCLUSIVE, 0, base);
        Job b = concurrentJob("com.example.Import", "project:42", ConcurrencyMode.EXCLUSIVE, 0, base.plusMillis(1));
        store.insert(a);
        store.insert(b);

        // a runs first, fails (terminal). That frees b to run; b fails and retries.
        Job ca = store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        assertThat(ca.id()).isEqualTo(a.id());
        finish(ca, JobState.FAILED);

        Job cb = store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        assertThat(cb.id()).isEqualTo(b.id());
        finish(cb, JobState.FAILED);
        retryToEnqueued(b.id());

        // Now retry a too. Both a and b are ENQUEUED with attempts > 0; both are
        // EXCLUSIVE on the same key, so a claim may return at most one.
        retryToEnqueued(a.id());
        assertThat(store.claimReady(NodeId.newId(), "default", 2, Instant.now()))
                .hasSize(1);
    }

    @Test
    @DisplayName("a retried SHARED job never runs alongside a retried EXCLUSIVE job for the same key")
    void retriedSharedDoesNotRunWithRetriedExclusiveForSameKey() {
        var base = Instant.now().minusSeconds(5);
        Job excl = concurrentJob("com.example.Import", "project:42", ConcurrencyMode.EXCLUSIVE, 0, base);
        Job shared = concurrentJob("com.example.Export", "project:42", ConcurrencyMode.SHARED, 0, base.plusMillis(1));
        store.insert(excl);
        store.insert(shared);

        Job c1 = store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        assertThat(c1.id()).isEqualTo(excl.id());
        finish(c1, JobState.FAILED);

        Job c2 = store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        assertThat(c2.id()).isEqualTo(shared.id());
        finish(c2, JobState.FAILED);
        retryToEnqueued(shared.id());
        retryToEnqueued(excl.id());

        // EXCLUSIVE and SHARED on the same key must never both be PROCESSING.
        assertThat(store.claimReady(NodeId.newId(), "default", 2, Instant.now()))
                .hasSize(1);
    }

    @Test
    @DisplayName("concurrent claimers cannot both claim EXCLUSIVE jobs for the same concurrency key")
    void concurrentClaimersCannotBothClaimExclusiveJobsForSameConcurrencyKey() throws Exception {
        var base = Instant.now().minusSeconds(1);
        store.insert(concurrentJob("com.example.Import", "project:42", ConcurrencyMode.EXCLUSIVE, 0, base));
        store.insert(
                concurrentJob("com.example.Import", "project:42", ConcurrencyMode.EXCLUSIVE, 0, base.plusMillis(1)));

        var start = new CountDownLatch(1);
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            var first = executor.submit(() -> {
                start.await();
                return store.claimReady(NodeId.newId(), "default", 1, Instant.now());
            });
            var second = executor.submit(() -> {
                start.await();
                return store.claimReady(NodeId.newId(), "default", 1, Instant.now());
            });
            start.countDown();
            List<Job> claimed = new ArrayList<>();
            claimed.addAll(first.get(30, TimeUnit.SECONDS));
            claimed.addAll(second.get(30, TimeUnit.SECONDS));
            assertThat(claimed).hasSize(1);

            finish(claimed.get(0), JobState.SUCCEEDED);
            assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                    .hasSize(1);
        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("a SHARED job does not leapfrog an earlier pending EXCLUSIVE job for the same key")
    void sharedJobDoesNotLeapfrogEarlierPendingExclusiveForSameKey() {
        var base = Instant.now().minusSeconds(3);
        Job sharedA = concurrentJob("com.example.Export", "project:42", ConcurrencyMode.SHARED, 0, base);
        Job exclusiveB =
                concurrentJob("com.example.Import", "project:42", ConcurrencyMode.EXCLUSIVE, 0, base.plusMillis(1));
        Job sharedC = concurrentJob("com.example.Export", "project:42", ConcurrencyMode.SHARED, 0, base.plusMillis(2));
        store.insert(sharedA);
        store.insert(exclusiveB);
        store.insert(sharedC);

        List<Job> first = store.claimReady(NodeId.newId(), "default", 3, Instant.now());
        assertThat(first).extracting(Job::id).containsExactly(sharedA.id());
        finish(first.get(0), JobState.SUCCEEDED);

        List<Job> second = store.claimReady(NodeId.newId(), "default", 3, Instant.now());
        assertThat(second).extracting(Job::id).containsExactly(exclusiveB.id());
        finish(second.get(0), JobState.SUCCEEDED);

        assertThat(store.claimReady(NodeId.newId(), "default", 3, Instant.now()))
                .extracting(Job::id)
                .containsExactly(sharedC.id());
    }

    @Test
    @DisplayName("same-timestamp pending order uses job id as the tie-breaker")
    void sameTimestampPendingOrderUsesJobIdTieBreaker() {
        var sameTime = Instant.now().minusSeconds(3).truncatedTo(ChronoUnit.MILLIS);
        Job sharedA = concurrentJobWithId(
                "com.example.Export", fixedJobId(1), "project:42", ConcurrencyMode.SHARED, 0, sameTime);
        Job exclusiveB = concurrentJobWithId(
                "com.example.Import", fixedJobId(2), "project:42", ConcurrencyMode.EXCLUSIVE, 0, sameTime);
        Job sharedC = concurrentJobWithId(
                "com.example.Export", fixedJobId(3), "project:42", ConcurrencyMode.SHARED, 0, sameTime);
        store.insert(sharedA);
        store.insert(exclusiveB);
        store.insert(sharedC);

        List<Job> first = store.claimReady(NodeId.newId(), "default", 3, Instant.now());
        assertThat(first).extracting(Job::id).containsExactly(sharedA.id());
        finish(first.get(0), JobState.SUCCEEDED);

        List<Job> second = store.claimReady(NodeId.newId(), "default", 3, Instant.now());
        assertThat(second).extracting(Job::id).containsExactly(exclusiveB.id());
        finish(second.get(0), JobState.SUCCEEDED);

        assertThat(store.claimReady(NodeId.newId(), "default", 3, Instant.now()))
                .extracting(Job::id)
                .containsExactly(sharedC.id());
    }

    @Test
    @DisplayName("claimReady scans past a blocked same-key priority window")
    void claimReadyScansPastBlockedSameKeyPriorityWindow() {
        var base = Instant.now().minusSeconds(10);
        Job olderExclusive = concurrentJob("com.example.Import", "project:hot", ConcurrencyMode.EXCLUSIVE, -100, base);
        store.insert(olderExclusive);
        for (int i = 0; i < 150; i++) {
            store.insert(concurrentJob(
                    "com.example.Export", "project:hot", ConcurrencyMode.SHARED, 100, base.plusMillis(i + 1)));
        }

        assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                .extracting(Job::id)
                .containsExactly(olderExclusive.id());
    }

    @Test
    @DisplayName("claimReady scans past a blocked hot key to claim independent work")
    void claimReadyScansPastBlockedHotKeyToOtherKeys() {
        var base = Instant.now().minusSeconds(10);
        store.insert(concurrentJob("com.example.Import", "project:hot", ConcurrencyMode.EXCLUSIVE, -100, base));
        for (int i = 0; i < 150; i++) {
            store.insert(concurrentJob(
                    "com.example.Export", "project:hot", ConcurrencyMode.SHARED, 100, base.plusMillis(i + 1)));
        }
        Job independent = concurrentJob(
                "com.example.Import", "project:other", ConcurrencyMode.EXCLUSIVE, -10, base.plusSeconds(1));
        store.insert(independent);

        assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                .extracting(Job::id)
                .containsExactly(independent.id());
    }

    @Test
    @DisplayName("sustained contention never runs a writer with readers or another writer")
    void concurrencyKeyContentionMaintainsReadWriteInvariants() throws Exception {
        for (int i = 0; i < 100; i++) {
            store.insert(Jobs.withConcurrency("com.example.Export", "project:hot", ConcurrencyMode.SHARED));
        }
        for (int i = 0; i < 5; i++) {
            store.insert(Jobs.withConcurrency("com.example.Import", "project:hot", ConcurrencyMode.EXCLUSIVE));
        }

        var activeReaders = new AtomicInteger();
        var activeWriters = new AtomicInteger();
        var invariantBroken = new AtomicBoolean();
        var writersSeen = ConcurrentHashMap.<JobId>newKeySet();
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
            for (int worker = 0; worker < 8; worker++) {
                futures.add(executor.submit(() -> {
                    while (true) {
                        List<Job> claimed = store.claimReady(NodeId.newId(), "default", 1, Instant.now());
                        if (claimed.isEmpty()) {
                            return null;
                        }
                        Job job = claimed.get(0);
                        if (job.concurrencyMode().orElseThrow() == ConcurrencyMode.EXCLUSIVE) {
                            writersSeen.add(job.id());
                            int writers = activeWriters.incrementAndGet();
                            if (writers > 1 || activeReaders.get() > 0) invariantBroken.set(true);
                            Thread.sleep(2);
                            activeWriters.decrementAndGet();
                        } else {
                            int readers = activeReaders.incrementAndGet();
                            if (readers < 1 || activeWriters.get() > 0) invariantBroken.set(true);
                            Thread.sleep(2);
                            activeReaders.decrementAndGet();
                        }
                        finish(job, JobState.SUCCEEDED);
                    }
                }));
            }
            for (var future : futures) future.get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
        assertThat(invariantBroken).isFalse();
        assertThat(writersSeen).hasSize(5);
        assertThat(store.countsByState().get(JobState.SUCCEEDED)).isEqualTo(105L);
    }

    @Test
    @DisplayName("concurrent claimers under EXCLUSIVE-key contention never collectively stall")
    void unblockUnderContentionDoesNotSkipKeys() throws Exception {
        // Audit §6.1 — the reference's issue #694 shape:
        // when two workers concurrently run an unblock-by-concurrency-key
        // claim, neither must wrongly conclude "nothing to do" while there is
        // still pending work for that key. Threadmill defeats this with a
        // version-matched UPDATE inside the claim path; this test cements the
        // invariant on every backend.
        int total = 12;
        for (int i = 0; i < total; i++) {
            store.insert(Jobs.withConcurrency("com.example.Import", "project:hotkey", ConcurrencyMode.EXCLUSIVE));
        }

        var claimed = new AtomicInteger();
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
            for (int worker = 0; worker < 4; worker++) {
                futures.add(executor.submit(() -> {
                    while (true) {
                        List<Job> got = store.claimReady(NodeId.newId(), "default", 1, Instant.now());
                        if (got.isEmpty()) {
                            // We may legitimately see zero rows if a peer is between claim and
                            // finish. Re-check whether work is still pending; if so, retry.
                            if (store.countsByState().getOrDefault(JobState.ENQUEUED, 0L) == 0L) {
                                return null;
                            }
                            Thread.sleep(1);
                            continue;
                        }
                        claimed.incrementAndGet();
                        finish(got.get(0), JobState.SUCCEEDED);
                    }
                }));
            }
            for (var f : futures) f.get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
        assertThat(claimed.get()).isEqualTo(total);
        assertThat(store.countsByState().get(JobState.SUCCEEDED)).isEqualTo((long) total);
    }

    // ================================================================ bulk insert

    @Test
    @DisplayName("insertAll atomically inserts every job and advances each in-memory version")
    void insertAllAtomicallyAdvancesVersionForEverySuccessfulJob() {
        var batch = new ArrayList<Job>();
        for (int i = 0; i < 5; i++) {
            batch.add(Jobs.enqueued("com.example.SendEmail"));
        }
        List<JobId> ids = store.insertAll(batch);

        assertThat(ids).hasSize(5).extracting(Object::toString).doesNotHaveDuplicates();
        for (Job j : batch) {
            assertThat(j.version()).isEqualTo(1L);
            assertThat(store.findById(j.id())).isPresent();
        }
        assertThat(store.countsByState().get(JobState.ENQUEUED)).isEqualTo(5L);
    }

    @Test
    @DisplayName("insertAll preserves id order in the returned list")
    void insertAllPreservesIdOrderInReturnedList() {
        var batch = new ArrayList<Job>();
        for (int i = 0; i < 6; i++) batch.add(Jobs.enqueued("com.example.Handler"));
        List<JobId> ids = store.insertAll(batch);

        for (int i = 0; i < batch.size(); i++) {
            assertThat(ids.get(i)).isEqualTo(batch.get(i).id());
        }
    }

    @Test
    @DisplayName("insertAll rejects the whole batch if any single job is oversized — no in-memory version is mutated")
    void insertAllRejectsBatchAndLeavesAllVersionsAtZeroIfAnySerializationFails() {
        var batch = new ArrayList<Job>();
        for (int i = 0; i < 10; i++) batch.add(Jobs.enqueued("com.example.Handler"));
        // The 11th job is oversized.
        Job oversize = Jobs.enqueued("com.example.Handler");
        String chunk = "x".repeat(4096);
        for (int i = 0; i < 200; i++) oversize.metadata().put("k" + i, chunk);
        batch.add(oversize);

        long countBefore = store.countsByState().getOrDefault(JobState.ENQUEUED, 0L);
        assertThatThrownBy(() -> store.insertAll(batch)).isInstanceOf(OversizedJobException.class);
        for (Job j : batch) {
            assertThat(j.version()).as(j.id().toString()).isZero();
        }
        assertThat(store.countsByState().getOrDefault(JobState.ENQUEUED, 0L)).isEqualTo(countBefore);
    }

    @Test
    @DisplayName("insertAll accepts concurrency-keyed jobs without a fallback to per-job inserts")
    void insertAllAcceptsConcurrencyKeyedJobsWithoutFallback() {
        var batch = List.of(
                Jobs.withConcurrency("com.example.Import", "project:k1", ConcurrencyMode.EXCLUSIVE),
                Jobs.withConcurrency("com.example.Import", "project:k1", ConcurrencyMode.EXCLUSIVE),
                Jobs.withConcurrency("com.example.Export", "project:k2", ConcurrencyMode.SHARED));
        List<JobId> ids = store.insertAll(batch);
        assertThat(ids).hasSize(3);

        // Claim-time concurrency still serializes EXCLUSIVE jobs for the same key.
        List<Job> first = store.claimReady(NodeId.newId(), "default", 5, Instant.now());
        assertThat(first).hasSize(2); // one of the EXCLUSIVE + the SHARED for a different key
    }

    @Test
    @DisplayName("concurrent insertAll calls do not deadlock or lose jobs")
    void insertAllUnderContentionDoesNotDeadlockBeyondRetry() throws Exception {
        int workers = 4;
        int batchSize = 5;
        var latch = new CountDownLatch(1);
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            var futures = new ArrayList<java.util.concurrent.Future<?>>();
            for (int w = 0; w < workers; w++) {
                futures.add(executor.submit(() -> {
                    latch.await();
                    var batch = new ArrayList<Job>();
                    for (int i = 0; i < batchSize; i++) batch.add(Jobs.enqueued("com.example.Handler"));
                    store.insertAll(batch);
                    return null;
                }));
            }
            latch.countDown();
            for (var f : futures) f.get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
        assertThat(store.countsByState().get(JobState.ENQUEUED)).isEqualTo((long) workers * batchSize);
    }

    // ================================================================ queue pauses

    @Test
    @DisplayName("pauseQueue is idempotent and updates the reason on repeated calls")
    void pauseQueueIsIdempotent() {
        store.pauseQueue("alpha", "first");
        store.pauseQueue("alpha", "second");
        assertThat(store.listPausedQueues()).containsExactly("alpha");
    }

    @Test
    @DisplayName("resumeQueue is idempotent — resuming an unpaused queue is a no-op")
    void resumeQueueIsIdempotent() {
        store.resumeQueue("alpha");
        assertThat(store.listPausedQueues()).isEmpty();
        store.pauseQueue("alpha", null);
        store.resumeQueue("alpha");
        store.resumeQueue("alpha");
        assertThat(store.listPausedQueues()).isEmpty();
    }

    @Test
    @DisplayName("listPausedQueues reflects every pause and resume")
    void listPausedQueuesReflectsPausesAndResumes() {
        store.pauseQueue("alpha", "a");
        store.pauseQueue("beta", "b");
        assertThat(store.listPausedQueues()).containsExactlyInAnyOrder("alpha", "beta");
        store.resumeQueue("alpha");
        assertThat(store.listPausedQueues()).containsExactly("beta");
        store.resumeQueue("beta");
        assertThat(store.listPausedQueues()).isEmpty();
    }

    @Test
    @DisplayName("claimReady returns empty for a paused queue and resumes after unpause")
    void claimReadyReturnsEmptyForPausedQueue() {
        Job j = Jobs.enqueued("com.example.SendEmail");
        store.insert(j);
        store.pauseQueue(j.queue(), "ops paused");

        assertThat(store.claimReady(NodeId.newId(), j.queue(), 5, Instant.now()))
                .isEmpty();
        // The job is still ENQUEUED — pausing does not change job state.
        assertThat(store.findById(j.id()).orElseThrow().currentState()).isEqualTo(JobState.ENQUEUED);

        store.resumeQueue(j.queue());
        List<Job> claimed = store.claimReady(NodeId.newId(), j.queue(), 5, Instant.now());
        assertThat(claimed).extracting(Job::id).containsExactly(j.id());
    }

    @Test
    @DisplayName("pausing one queue does not affect another")
    void pausingOneQueueDoesNotAffectAnother() {
        Job onAlpha = Jobs.onQueue("com.example.SendEmail", "alpha");
        Job onBeta = Jobs.onQueue("com.example.SendEmail", "beta");
        store.insert(onAlpha);
        store.insert(onBeta);
        store.pauseQueue("beta", "test");

        assertThat(store.claimReady(NodeId.newId(), "alpha", 5, Instant.now()))
                .extracting(Job::id)
                .containsExactly(onAlpha.id());
        assertThat(store.claimReady(NodeId.newId(), "beta", 5, Instant.now())).isEmpty();
    }

    @Test
    @DisplayName("workflow concurrency is inherited and held until the last chain step terminates")
    void workflowConcurrencyIsInheritedThroughSimpleChain() {
        Job root = concurrentJob("com.example.Validate", "project:42", ConcurrencyMode.EXCLUSIVE, 10, Instant.now());
        store.insert(root);
        Job child1 = Jobs.awaitingWorkflowStep("com.example.Import", root);
        store.insert(child1);
        Job child2 = Jobs.awaitingWorkflowStep("com.example.Notify", child1);
        store.insert(child2);
        Job outsider = concurrentJob("com.example.Other", "project:42", ConcurrencyMode.EXCLUSIVE, -1, Instant.now());
        store.insert(outsider);

        Job claimedRoot =
                store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        finish(claimedRoot, JobState.SUCCEEDED);
        promote(child1.id());
        assertThat(store.claimReady(NodeId.newId(), "default", 2, Instant.now()))
                .extracting(Job::id)
                .containsExactly(child1.id());
        finish(store.findById(child1.id()).orElseThrow(), JobState.SUCCEEDED);
        promote(child2.id());
        assertThat(store.claimReady(NodeId.newId(), "default", 2, Instant.now()))
                .extracting(Job::id)
                .containsExactly(child2.id());
        finish(store.findById(child2.id()).orElseThrow(), JobState.SUCCEEDED);

        assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                .extracting(Job::id)
                .containsExactly(outsider.id());
    }

    @Test
    @DisplayName("a retried EXCLUSIVE workflow root reclaims under its own hold")
    void workflowRootRetryAfterFailureCanReclaimUnderItsOwnHold() {
        Job root = concurrentJob("com.example.Root", "project:42", ConcurrencyMode.EXCLUSIVE, 10, Instant.now());
        store.insert(root);
        Job child = Jobs.awaitingWorkflowStep("com.example.Child", root);
        store.insert(child);
        Job outsider = concurrentJob("com.example.Other", "project:42", ConcurrencyMode.EXCLUSIVE, -1, Instant.now());
        store.insert(outsider);

        Job claimedRoot =
                store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        assertThat(claimedRoot.id()).isEqualTo(root.id());
        finish(claimedRoot, JobState.FAILED);
        retryToEnqueued(root.id());

        // The retried root must be claimable again — its own AWAITING child
        // is not "earlier pending work" — and the outsider stays blocked.
        assertThat(store.claimReady(NodeId.newId(), "default", 2, Instant.now()))
                .extracting(Job::id)
                .containsExactly(root.id());
        finish(store.findById(root.id()).orElseThrow(), JobState.SUCCEEDED);
        promote(child.id());
        assertThat(store.claimReady(NodeId.newId(), "default", 2, Instant.now()))
                .extracting(Job::id)
                .containsExactly(child.id());
        finish(store.findById(child.id()).orElseThrow(), JobState.SUCCEEDED);
        assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                .extracting(Job::id)
                .containsExactly(outsider.id());
    }

    @Test
    @DisplayName("retry-resurrect of a workflow member does not double-release the hold")
    void workflowMemberRetryDoesNotDoubleReleaseTheHold() {
        Job root = concurrentJob("com.example.Root", "project:42", ConcurrencyMode.EXCLUSIVE, 10, Instant.now());
        store.insert(root);
        Job child1 = Jobs.awaitingWorkflowStep("com.example.Child1", root);
        Job child2 = Jobs.awaitingWorkflowStep("com.example.Child2", root);
        store.insert(child1);
        store.insert(child2);
        Job outsider = concurrentJob("com.example.Other", "project:42", ConcurrencyMode.EXCLUSIVE, -1, Instant.now());
        store.insert(outsider);

        Job claimedRoot =
                store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        assertThat(claimedRoot.id()).isEqualTo(root.id());
        finish(claimedRoot, JobState.FAILED);
        retryToEnqueued(root.id());
        assertThat(store.claimReady(NodeId.newId(), "default", 2, Instant.now()))
                .extracting(Job::id)
                .containsExactly(root.id());
        finish(store.findById(root.id()).orElseThrow(), JobState.SUCCEEDED);

        promote(child1.id());
        promote(child2.id());
        List<Job> children = store.claimReady(NodeId.newId(), "default", 2, Instant.now());
        assertThat(children).extracting(Job::id).containsExactlyInAnyOrder(child1.id(), child2.id());
        finish(children.get(0), JobState.SUCCEEDED);

        // The retry must not have double-decremented the hold: with one
        // child still PROCESSING the outsider cannot claim.
        assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                .isEmpty();
        finish(children.get(1), JobState.SUCCEEDED);
        assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                .extracting(Job::id)
                .containsExactly(outsider.id());
    }

    /** The standard retry path: FAILED -> SCHEDULED -> ENQUEUED. */
    private void retryToEnqueued(JobId id) {
        Job failed = store.findById(id).orElseThrow();
        long v = failed.version();
        failed.transitionTo(JobState.SCHEDULED, Instant.now(), "engine.retry-after-failure", null);
        failed.scheduleAt(Instant.now());
        failed.clearOwner();
        store.saveAtomic(failed, v);
        promote(id);
    }

    @Test
    @DisplayName("workflow concurrency with branching releases only after all siblings terminate")
    void workflowConcurrencyWithBranchingReleasesAfterAllSiblingsTerminate() {
        Job root = concurrentJob("com.example.Root", "project:42", ConcurrencyMode.EXCLUSIVE, 10, Instant.now());
        store.insert(root);
        Job child1 = Jobs.awaitingWorkflowStep("com.example.Child1", root);
        Job child2 = Jobs.awaitingWorkflowStep("com.example.Child2", root);
        Job child3 = Jobs.awaitingWorkflowStep("com.example.Child3", root);
        store.insert(child1);
        store.insert(child2);
        store.insert(child3);
        Job outsider = concurrentJob("com.example.Other", "project:42", ConcurrencyMode.EXCLUSIVE, -1, Instant.now());
        store.insert(outsider);

        Job claimedRoot =
                store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        finish(claimedRoot, JobState.SUCCEEDED);
        promote(child1.id());
        promote(child2.id());
        promote(child3.id());
        List<Job> children = store.claimReady(NodeId.newId(), "default", 3, Instant.now());
        assertThat(children).extracting(Job::id).containsExactlyInAnyOrder(child1.id(), child2.id(), child3.id());
        finish(children.get(0), JobState.SUCCEEDED);
        finish(children.get(1), JobState.FAILED);
        assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                .isEmpty();
        finish(children.get(2), JobState.SUCCEEDED);

        assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                .extracting(Job::id)
                .containsExactly(outsider.id());
    }

    @Test
    @DisplayName("workflow concurrency releases after partial failure once every child is terminal")
    void workflowConcurrencyPartialFailureReleasesAfterEveryChildIsTerminal() {
        Job root = concurrentJob("com.example.Root", "project:42", ConcurrencyMode.EXCLUSIVE, 10, Instant.now());
        store.insert(root);
        Job child1 = Jobs.awaitingWorkflowStep("com.example.Child1", root);
        Job child2 = Jobs.awaitingWorkflowStep("com.example.Child2", root);
        store.insert(child1);
        store.insert(child2);
        Job outsider = concurrentJob("com.example.Other", "project:42", ConcurrencyMode.EXCLUSIVE, -1, Instant.now());
        store.insert(outsider);

        Job claimedRoot =
                store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        finish(claimedRoot, JobState.SUCCEEDED);
        promote(child1.id());
        promote(child2.id());
        List<Job> children = store.claimReady(NodeId.newId(), "default", 2, Instant.now());
        finish(children.get(0), JobState.FAILED);
        assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                .isEmpty();
        finish(children.get(1), JobState.SUCCEEDED);

        assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                .extracting(Job::id)
                .containsExactly(outsider.id());
    }

    @Test
    @DisplayName("workflow concurrency abandons waiting descendants after an intermediate failure")
    void workflowConcurrencyIntermediateFailureAbandonsWaitingDescendants() {
        Job root = concurrentJob("com.example.Root", "project:42", ConcurrencyMode.EXCLUSIVE, 10, Instant.now());
        store.insert(root);
        Job child1 = Jobs.awaitingWorkflowStep("com.example.Child1", root);
        store.insert(child1);
        Job child2 = Jobs.awaitingWorkflowStep("com.example.Child2", child1);
        store.insert(child2);
        Job outsider = concurrentJob("com.example.Other", "project:42", ConcurrencyMode.EXCLUSIVE, -1, Instant.now());
        store.insert(outsider);

        Job claimedRoot =
                store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        finish(claimedRoot, JobState.SUCCEEDED);
        promote(child1.id());
        Job claimedChild =
                store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        finish(claimedChild, JobState.FAILED);

        new WorkflowInterceptor(store)
                .onProcessingFailed(
                        store.findById(child1.id()).orElseThrow(),
                        null,
                        new RuntimeException("boom"),
                        JobInterceptor.FailureCause.EXCEPTION);

        assertThat(store.findById(child2.id()).orElseThrow().currentState()).isEqualTo(JobState.DELETED);
        assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                .extracting(Job::id)
                .containsExactly(outsider.id());
    }

    @Test
    @DisplayName("a poisoned workflow root releases its concurrency key when it is quarantined")
    void poisonedWorkflowRootReleasesConcurrencyKeyOnQuarantine() {
        Job root = concurrentJob("com.example.Root", "project:42", ConcurrencyMode.EXCLUSIVE, 10, Instant.now());
        Job outsider = concurrentJob("com.example.Other", "project:42", ConcurrencyMode.EXCLUSIVE, -1, Instant.now());
        store.insert(root);
        store.insert(outsider);

        Job claimedRoot =
                store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        finish(claimedRoot, JobState.QUARANTINED);

        assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                .extracting(Job::id)
                .containsExactly(outsider.id());
    }

    @Test
    @DisplayName("a workflow child inserted after root claim keeps the workflow hold")
    void workflowChildInsertedAfterRootClaimKeepsWorkflowHold() {
        Job root = concurrentJob("com.example.Root", "project:42", ConcurrencyMode.EXCLUSIVE, 10, Instant.now());
        store.insert(root);
        Job claimedRoot =
                store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);

        Job child = Jobs.awaitingWorkflowStep("com.example.Child", claimedRoot);
        store.insert(child);
        Job outsider = concurrentJob("com.example.Other", "project:42", ConcurrencyMode.EXCLUSIVE, -1, Instant.now());
        store.insert(outsider);

        finish(claimedRoot, JobState.SUCCEEDED);
        promote(child.id());

        List<Job> claimedChild = store.claimReady(NodeId.newId(), "default", 2, Instant.now());
        assertThat(claimedChild).extracting(Job::id).containsExactly(child.id());
        assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                .isEmpty();

        finish(claimedChild.get(0), JobState.SUCCEEDED);
        assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                .extracting(Job::id)
                .containsExactly(outsider.id());
    }

    @Test
    @DisplayName("concurrency key validation rejects blank, oversize, and missing mode")
    void concurrencyKeyValidationRejectsInvalidDefinitions() {
        assertThatThrownBy(() -> Jobs.withConcurrency("com.example.Bad", " ", ConcurrencyMode.EXCLUSIVE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("concurrencyKey");
        assertThatThrownBy(() -> Jobs.withConcurrency("com.example.Bad", "x".repeat(257), ConcurrencyMode.EXCLUSIVE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("concurrencyKey");
        assertThatThrownBy(() -> Jobs.withConcurrency("com.example.Bad", "project:42", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("concurrencyMode");
    }

    @Test
    @DisplayName("a cold concurrency key claims normally")
    void coldConcurrencyKeyClaimsNormally() {
        store.insert(Jobs.withConcurrency("com.example.Import", "project:new", ConcurrencyMode.EXCLUSIVE));

        assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                .hasSize(1);
    }

    // ================================================================ housekeeping

    @Test
    @DisplayName("findDueForPromotion returns SCHEDULED jobs whose time is at or before now")
    void findDueForPromotion() {
        var past = Instant.now().minusSeconds(60);
        var future = Instant.now().plusSeconds(60);
        Job due = Jobs.scheduled("com.example.SendEmail", past);
        Job laterOn = Jobs.scheduled("com.example.SendEmail", future);
        store.insert(due);
        store.insert(laterOn);

        List<Job> result = store.findDueForPromotion(Instant.now(), 10);
        assertThat(result).extracting(Job::id).containsExactly(due.id());
    }

    @Test
    @DisplayName("findOrphaned returns PROCESSING jobs whose heartbeat has expired")
    void findOrphaned() {
        Job alive = Jobs.enqueued("com.example.SendEmail");
        Job stale = Jobs.enqueued("com.example.SendEmail");
        store.insert(alive);
        store.insert(stale);

        var beatLong = Instant.now();
        store.claimReady(NodeId.newId(), "default", 1, Instant.now().minus(2, ChronoUnit.HOURS));
        store.claimReady(NodeId.newId(), "default", 1, beatLong);

        var cutoff = Instant.now().minus(30, ChronoUnit.MINUTES);
        List<Job> orphans = store.findOrphaned(cutoff, 10);
        assertThat(orphans).hasSize(1);
        assertThat(orphans.get(0).ownerHeartbeatAt())
                .hasValueSatisfying(b -> assertThat(b).isBefore(cutoff));
    }

    @Test
    @DisplayName("node heartbeats round-trip")
    void nodeHeartbeatsRoundTrip() {
        NodeId node = NodeId.newId();
        assertThat(store.readNodeHeartbeat(node)).isEmpty();
        var at = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        store.recordNodeHeartbeat(node, at);
        assertThat(store.readNodeHeartbeat(node)).contains(at);
        assertThat(store.listNodeHeartbeats()).anySatisfy(h -> {
            assertThat(h.nodeId()).isEqualTo(node);
            assertThat(h.lastHeartbeatAt()).isEqualTo(at);
        });
    }

    @Test
    @DisplayName("stale node heartbeats can be deleted without touching fresh nodes")
    void staleNodeHeartbeatsCanBeDeleted() {
        NodeId stale = NodeId.newId();
        NodeId fresh = NodeId.newId();
        var cutoff = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        var staleAt = cutoff.minusSeconds(60);
        var freshAt = cutoff.plusMillis(1);
        store.recordNodeHeartbeat(stale, staleAt);
        store.recordNodeHeartbeat(fresh, freshAt);

        assertThat(store.deleteNodeHeartbeatsOlderThan(cutoff)).isEqualTo(1L);
        assertThat(store.readNodeHeartbeat(stale)).isEmpty();
        assertThat(store.readNodeHeartbeat(fresh)).contains(freshAt);
        assertThat(store.listNodeHeartbeats())
                .extracting(NodeHeartbeat::nodeId)
                .contains(fresh)
                .doesNotContain(stale);
    }

    @Test
    @DisplayName("maintenance lease is exclusive, renewable by its holder, releasable, and expires")
    void maintenanceLeaseSemantics() throws InterruptedException {
        NodeId a = NodeId.newId();
        NodeId b = NodeId.newId();
        assertThat(store.readMaintenanceLeaseOwner()).isEmpty();
        assertThat(store.acquireOrRenewMaintenanceLease(a, Duration.ofMillis(100)))
                .isTrue();
        assertThat(store.readMaintenanceLeaseOwner()).contains(a);
        assertThat(store.acquireOrRenewMaintenanceLease(b, Duration.ofSeconds(1)))
                .isFalse();
        assertThat(store.acquireOrRenewMaintenanceLease(a, Duration.ofSeconds(1)))
                .isTrue();
        store.releaseMaintenanceLease(b);
        assertThat(store.readMaintenanceLeaseOwner()).contains(a);
        store.releaseMaintenanceLease(a);
        assertThat(store.acquireOrRenewMaintenanceLease(b, Duration.ofMillis(100)))
                .isTrue();
        Thread.sleep(220);
        assertThat(store.acquireOrRenewMaintenanceLease(a, Duration.ofSeconds(1)))
                .isTrue();
        assertThat(store.readMaintenanceLeaseOwner()).contains(a);
    }

    @Test
    @DisplayName("maintenance lease rejects null and non-positive durations")
    void maintenanceLeaseRejectsInvalidDuration() {
        assertThatThrownBy(() -> store.acquireOrRenewMaintenanceLease(NodeId.newId(), null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.acquireOrRenewMaintenanceLease(NodeId.newId(), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.acquireOrRenewMaintenanceLease(NodeId.newId(), Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ================================================================ counts & search

    @Test
    @DisplayName("countsByState returns every state, defaulting to 0")
    void countsByState() {
        for (int i = 0; i < 3; i++) store.insert(Jobs.enqueued("com.example.SendEmail"));
        Map<JobState, Long> counts = store.countsByState();
        assertThat(counts.keySet()).contains(JobState.values());
        assertThat(counts.get(JobState.ENQUEUED)).isEqualTo(3L);
        assertThat(counts.get(JobState.SUCCEEDED)).isEqualTo(0L);
    }

    @Test
    @DisplayName("queueDepths and oldestEnqueuedAt report active queues without claiming")
    void queueOperationalQueries() {
        Job def = Jobs.onQueue("com.example.SendEmail", "default");
        Job high1 = Jobs.onQueue("com.example.SendEmail", "high priority");
        Job high2 = Jobs.onQueue("com.example.SendEmail", "high priority");
        store.insert(def);
        store.insert(high1);
        store.insert(high2);

        Map<String, Long> depths = store.queueDepths();
        assertThat(depths.get("default")).isEqualTo(1L);
        assertThat(depths.get("high priority")).isEqualTo(2L);
        assertThat(store.oldestEnqueuedAt("high priority")).isPresent();
        assertThat(store.listEnqueuedQueues()).contains("default", "high priority");

        store.claimReady(NodeId.newId(), "high priority", 2, Instant.now());
        assertThat(store.queueDepths().getOrDefault("high priority", 0L)).isEqualTo(0L);
        assertThat(store.listEnqueuedQueues()).contains("default").doesNotContain("high priority");
        assertThat(store.oldestEnqueuedAt("high priority")).isEmpty();
    }

    @Test
    @DisplayName("searchJobs filters jobs and returns stable newest-first pages")
    void searchJobsFiltersAndPages() {
        var base = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        Job alpha = Job.builder()
                .spec(JobSpec.of("com.example.A"))
                .queue("alpha")
                .createdAt(base)
                .build();
        Job betaOld = Job.builder()
                .spec(JobSpec.of("com.example.A"))
                .queue("beta")
                .createdAt(base.plusSeconds(1))
                .build();
        Job betaNew = Job.builder()
                .spec(JobSpec.of("com.example.A"))
                .queue("beta")
                .createdAt(base.plusSeconds(2))
                .build();
        Job otherHandler = Job.builder()
                .spec(JobSpec.of("com.example.B"))
                .queue("beta")
                .createdAt(base.plusSeconds(3))
                .build();
        store.insert(alpha);
        store.insert(betaOld);
        store.insert(betaNew);
        store.insert(otherHandler);

        if (!store.capabilities().supportsRichSearch()) {
            var firstPage = store.searchJobs(new JobSearch(JobState.ENQUEUED, null, null, 1, 0));
            assertThat(firstPage).extracting(Job::id).containsExactly(otherHandler.id());

            var secondPage = store.searchJobs(new JobSearch(JobState.ENQUEUED, null, null, 1, 1));
            assertThat(secondPage).extracting(Job::id).containsExactly(betaNew.id());
            return;
        }

        var firstPage = store.searchJobs(new JobSearch(JobState.ENQUEUED, "beta", "com.example.A", 1, 0));
        assertThat(firstPage).extracting(Job::id).containsExactly(betaNew.id());

        var secondPage = store.searchJobs(new JobSearch(JobState.ENQUEUED, "beta", "com.example.A", 1, 1));
        assertThat(secondPage).extracting(Job::id).containsExactly(betaOld.id());
    }

    @Test
    @DisplayName("oldestProcessingHeartbeat reports the oldest processing owner heartbeat")
    void oldestProcessingHeartbeat() {
        store.insert(Jobs.enqueued("com.example.SendEmail"));
        store.insert(Jobs.enqueued("com.example.SendEmail"));
        var older = Instant.now().minus(2, ChronoUnit.HOURS).truncatedTo(ChronoUnit.MILLIS);
        var newer = Instant.now().minus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.MILLIS);
        store.claimReady(NodeId.newId(), "default", 1, older);
        store.claimReady(NodeId.newId(), "default", 1, newer);
        assertThat(store.oldestProcessingHeartbeat())
                .hasValueSatisfying(at -> assertThat(at).isBefore(newer.plusMillis(1)));
    }

    @Test
    @DisplayName("user-visible names are validated before storage")
    void userVisibleNamesAreValidated() {
        assertThatThrownBy(() -> store.claimReady(NodeId.newId(), " ", 1, Instant.now()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("queue");
        assertThatThrownBy(() -> store.tryAcquireMutex("bad\nname", "holder", Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mutex");
    }

    @Test
    @DisplayName("findByHandlerSignature returns matching jobs")
    void findByHandlerSignature() {
        store.insert(Jobs.enqueued("com.example.A"));
        store.insert(Jobs.enqueued("com.example.A"));
        store.insert(Jobs.enqueued("com.example.B"));

        List<Job> result = store.findByHandlerSignature("com.example.A", 10);
        assertThat(result).hasSize(2);
        assertThat(result).allMatch(j -> j.spec().handlerType().equals("com.example.A"));
    }

    // ================================================================ retention

    @Test
    @DisplayName("retention hard-deletes SUCCEEDED jobs older than the cutoff")
    void retentionDeletesEligibleJobs() {
        Job old = Jobs.enqueued("com.example.SendEmail");
        Job recent = Jobs.enqueued("com.example.SendEmail");
        store.insert(old);
        store.insert(recent);

        var oldAt = Instant.now().minus(48, ChronoUnit.HOURS);
        var recentAt = Instant.now();
        old.transitionTo(JobState.PROCESSING, oldAt);
        old.transitionTo(JobState.SUCCEEDED, oldAt);
        store.saveAtomic(old, old.version());
        recent.transitionTo(JobState.PROCESSING, recentAt);
        recent.transitionTo(JobState.SUCCEEDED, recentAt);
        store.saveAtomic(recent, recent.version());

        var cutoff = Instant.now().minus(24, ChronoUnit.HOURS);
        long deleted = store.deleteFinishedOlderThan(cutoff, JobState.SUCCEEDED, 100);
        assertThat(deleted).isEqualTo(1L);
        assertThat(store.findById(old.id())).isEmpty();
        assertThat(store.findById(recent.id())).isPresent();
    }

    // ================================================================ vanished

    @Test
    @DisplayName("softDelete of a vanished id is a no-op (returns false)")
    void softDeleteOfVanishedIsNoOp() {
        assertThat(store.softDelete(JobId.newId())).isFalse();
    }

    @Test
    @DisplayName("softDelete transitions an existing job to DELETED")
    void softDeleteTransitions() {
        Job j = Jobs.enqueued("com.example.SendEmail");
        store.insert(j);
        assertThat(store.softDelete(j.id())).isTrue();
        Job loaded = store.findById(j.id()).orElseThrow();
        assertThat(loaded.currentState()).isEqualTo(JobState.DELETED);
    }

    // ================================================================ size cap

    @Test
    @DisplayName("insert refuses an oversize job cleanly and does not corrupt the job's version")
    void oversizeInsertIsRejectedCleanly() {
        Job j = Jobs.enqueued("com.example.SendEmail");
        String bigChunk = "x".repeat(2048);
        for (int i = 0; i < 2000; i++) j.metadata().put("k" + i, bigChunk);

        long before = j.version();
        assertThatThrownBy(() -> store.insert(j)).isInstanceOf(OversizedJobException.class);
        assertThat(j.version()).isEqualTo(before);
    }

    // ================================================================ mutex

    @Test
    @DisplayName("tryAcquireMutex rejects null lease durations with IllegalArgumentException")
    void mutexRejectsNullLease() {
        assertThatThrownBy(() -> store.tryAcquireMutex("m", "node-a", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("tryAcquireMutex rejects zero or negative lease durations")
    void mutexRejectsNonPositiveLease() {
        assertThatThrownBy(() -> store.tryAcquireMutex("m", "node-a", Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.tryAcquireMutex("m", "node-a", Duration.ofMillis(-5)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("mutex is exclusive, reentrant for the same holder, and expires at the lease end")
    void mutexLeaseSemantics() throws InterruptedException {
        assertThat(store.tryAcquireMutex("m", "node-a", Duration.ofMillis(80))).isTrue();
        assertThat(store.tryAcquireMutex("m", "node-b", Duration.ofSeconds(5))).isFalse();
        // Reentrant: same holder re-acquires and the new lease overwrites the prior one.
        assertThat(store.tryAcquireMutex("m", "node-a", Duration.ofSeconds(5))).isTrue();
        store.releaseMutex("m", "wrong-holder"); // no-op
        assertThat(store.tryAcquireMutex("m", "node-b", Duration.ofSeconds(1))).isFalse();
        store.releaseMutex("m", "node-a");
        assertThat(store.tryAcquireMutex("m", "node-b", Duration.ofMillis(80))).isTrue();
        Thread.sleep(220);
        assertThat(store.tryAcquireMutex("m", "node-c", Duration.ofSeconds(5))).isTrue();
    }

    // ================================================================ replaceJob

    @Test
    @DisplayName("replaceJob swaps spec/queue/priority/scheduledFor and bumps version")
    void replaceJobAppliesNewDefinition() {
        Job j = Jobs.enqueued("com.example.Original");
        store.insert(j);
        long v = j.version();

        boolean replaced = store.replaceJob(
                j.id(),
                v,
                JobReplacement.builder()
                        .spec(JobSpec.of("com.example.Replacement", new JobArgument("java.lang.String", "\"new\"")))
                        .queue("high")
                        .priority(7)
                        .build());
        assertThat(replaced).isTrue();

        Job loaded = store.findById(j.id()).orElseThrow();
        assertThat(loaded.version()).isEqualTo(v + 1);
        assertThat(loaded.spec().handlerType()).isEqualTo("com.example.Replacement");
        assertThat(loaded.queue()).isEqualTo("high");
        assertThat(loaded.priority()).isEqualTo(7);
        assertThat(loaded.currentState()).isEqualTo(JobState.ENQUEUED);
        assertThat(store.findByHandlerSignature("com.example.Original", 10)).isEmpty();
        assertThat(store.findByHandlerSignature("com.example.Replacement", 10))
                .extracting(j2 -> j2.id())
                .containsExactly(j.id());
    }

    @Test
    @DisplayName("replaceJob throws StaleJobException when the expected version no longer matches")
    void replaceJobThrowsOnStaleVersion() {
        Job j = Jobs.enqueued("com.example.Original");
        store.insert(j);
        long initial = j.version();

        // First replacement bumps the version.
        store.replaceJob(j.id(), initial, JobReplacement.ofSpec(JobSpec.of("com.example.A")));

        // Stale call with the old expected version must throw.
        assertThatThrownBy(() -> store.replaceJob(j.id(), initial, JobReplacement.ofSpec(JobSpec.of("com.example.B"))))
                .isInstanceOf(StaleJobException.class);
    }

    @Test
    @DisplayName("replaceJob returns false when the job is in a non-replaceable state (e.g. PROCESSING)")
    void replaceJobRejectsNonReplaceableState() {
        Job j = Jobs.enqueued("com.example.Original");
        store.insert(j);
        store.claimReady(NodeId.newId(), "default", 1, Instant.now());

        Job claimed = store.findById(j.id()).orElseThrow();
        assertThat(claimed.currentState()).isEqualTo(JobState.PROCESSING);

        boolean replaced = store.replaceJob(
                j.id(), claimed.version(), JobReplacement.ofSpec(JobSpec.of("com.example.NewHandler")));
        assertThat(replaced).isFalse();
        assertThat(store.findById(j.id()).orElseThrow().spec().handlerType()).isEqualTo("com.example.Original");
    }

    @Test
    @DisplayName("replaceJob on a vanished id returns false (defined non-exceptional)")
    void replaceJobOnVanishedIsFalse() {
        assertThat(store.replaceJob(JobId.newId(), 1L, JobReplacement.ofSpec(JobSpec.of("com.example.H"))))
                .isFalse();
    }

    @Test
    @DisplayName("replaceJob preserves concurrencyKey, concurrencyMode, and workflowRootId")
    void replaceJobPreservesConcurrencyKeyModeAndWorkflowRoot() {
        Job root = concurrentJob("com.example.Root", "project:42", ConcurrencyMode.EXCLUSIVE, 10, Instant.now());
        store.insert(root);
        Job child = Jobs.awaitingWorkflowStep("com.example.Child", root);
        store.insert(child);
        Job outsider = concurrentJob("com.example.Other", "project:42", ConcurrencyMode.EXCLUSIVE, -1, Instant.now());
        store.insert(outsider);

        Job loadedChild = store.findById(child.id()).orElseThrow();
        boolean replaced = store.replaceJob(
                child.id(), loadedChild.version(), JobReplacement.ofSpec(JobSpec.of("com.example.ReplacedChild")));
        assertThat(replaced).isTrue();

        Job after = store.findById(child.id()).orElseThrow();
        assertThat(after.spec().handlerType()).isEqualTo("com.example.ReplacedChild");
        assertThat(after.concurrencyKey()).contains("project:42");
        assertThat(after.concurrencyMode()).contains(ConcurrencyMode.EXCLUSIVE);
        assertThat(after.workflowRootId()).isEqualTo(root.id());

        // Workflow-hold accounting must still release exactly once, at the true end of the chain.
        Job claimedRoot =
                store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        assertThat(claimedRoot.id()).isEqualTo(root.id());
        finish(claimedRoot, JobState.SUCCEEDED);
        promote(child.id());
        assertThat(store.claimReady(NodeId.newId(), "default", 2, Instant.now()))
                .extracting(Job::id)
                .containsExactly(child.id());
        finish(store.findById(child.id()).orElseThrow(), JobState.SUCCEEDED);
        assertThat(store.claimReady(NodeId.newId(), "default", 1, Instant.now()))
                .extracting(Job::id)
                .containsExactly(outsider.id());
    }

    // ================================================================ recurring ownership

    @Test
    @DisplayName("findCronTaskState is empty when no schedule state has been recorded")
    void cronStateForATaskWithoutRecordedStateIsEmptyOnEveryBackend() {
        // The SPI makes schedule state the caller's responsibility; a task with
        // no recorded state must read as empty on every backend (Redis used to
        // fabricate a present-but-all-null state).
        store.upsertCronTask(cronTask("nightly"));
        assertThat(store.findCronTaskState("nightly")).isEmpty();
    }

    @Test
    @DisplayName("cron-task ownership tracks namespace-owned tasks")
    void cronTaskOwnershipTracksNamespace() {
        store.upsertCronTask(cronTask("owned-a"));
        store.upsertCronTask(cronTask("owned-b"));
        store.upsertCronTask(cronTask("manual"));
        store.recordCronTaskOwnership("app-a", "owned-a");
        store.recordCronTaskOwnership("app-a", "owned-b");

        assertThat(store.listCronTaskNamesOwnedBy("app-a")).containsExactlyInAnyOrder("owned-a", "owned-b");

        store.deleteCronTask("owned-a");
        assertThat(store.listCronTaskNamesOwnedBy("app-a")).containsExactly("owned-b");
        assertThat(store.findCronTask("manual")).isPresent();
    }

    // ================================================================ capabilities

    @Test
    @DisplayName("the store advertises a positive size cap")
    void storeCapabilitiesAreSensible() {
        assertThat(store.capabilities().maxSerializedJobBytes()).isPositive();
        assertThat(store.capabilities().maxClaimBatch()).isPositive();
        assertThat(store.capabilities().supportsConcurrencyGroups()).isTrue();
    }

    private void finish(Job job, JobState terminalState) {
        long version = job.version();
        job.transitionTo(terminalState, Instant.now(), "test.finish", null);
        job.clearOwner();
        store.saveAtomic(job, version);
    }

    private static CronTask cronTask(String name) {
        return new CronTask(
                name,
                new CronTask.Trigger.CronExpr(CronExpression.parse("* * * * *")),
                "com.example.Handler",
                new JobArgument("com.example.Payload", "{}"),
                "default",
                0,
                CronTask.MissedRunPolicy.DROP,
                ZoneId.of("UTC"),
                true);
    }

    private void promote(JobId id) {
        Job job = store.findById(id).orElseThrow();
        long version = job.version();
        job.transitionTo(JobState.ENQUEUED, Instant.now(), "test.promote", null);
        store.saveAtomic(job, version);
    }

    private static Job concurrentJob(
            String handler, String key, ConcurrencyMode mode, int priority, Instant createdAt) {
        return concurrentJobWithId(handler, JobId.newId(), key, mode, priority, createdAt);
    }

    private static Job concurrentJobWithId(
            String handler, JobId id, String key, ConcurrencyMode mode, int priority, Instant createdAt) {
        return Job.builder()
                .id(id)
                .spec(JobSpec.of(handler, new JobArgument("java.lang.String", "\"hello\"")))
                .concurrencyKey(key)
                .concurrencyMode(mode)
                .priority(priority)
                .createdAt(createdAt)
                .build();
    }

    private static JobId fixedJobId(int value) {
        return JobId.of(new UUID(0L, value));
    }
}
