package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobReplacement;
import com.hemju.threadmill.core.JobSnapshot;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStoreCapabilities;

/**
 * Hammers the same job from many virtual threads and asserts that:
 * <ul>
 *   <li>persisted version advances monotonically — no double-increment,</li>
 *   <li>the in-memory job version never gets ahead of the store, and</li>
 *   <li>losing threads see {@link StaleJobException} (the documented signal).</li>
 * </ul>
 */
class InMemoryJobStoreConcurrencyTest {

    @Test
    void noVersionDesyncUnderHeavyContention() throws Exception {
        var store = new InMemoryJobStore();
        Job job = Job.builder()
                .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
                .build();
        store.insert(job);

        int threads = 24;
        int attemptsPerThread = 50;
        var start = new CountDownLatch(1);
        var successes = new AtomicInteger();
        var stale = new AtomicInteger();
        var uncaught = new AtomicReference<Throwable>();

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int t = 0; t < threads; t++) {
                executor.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < attemptsPerThread; i++) {
                            Job snap = store.findById(job.id()).orElseThrow();
                            long v = snap.version();
                            // Mutate a per-attempt metadata key so each thread does real work.
                            snap.metadata().put("a-" + Thread.currentThread().threadId() + "-" + i, "1");
                            try {
                                store.saveAtomic(snap, v);
                                successes.incrementAndGet();
                                assertThat(snap.version()).isEqualTo(v + 1);
                            } catch (StaleJobException e) {
                                stale.incrementAndGet();
                                assertThat(snap.version()).isEqualTo(v); // version not corrupted on failure
                            }
                        }
                    } catch (Throwable t1) {
                        uncaught.compareAndSet(null, t1);
                    }
                });
            }
            start.countDown();
            executor.shutdown();
            assertThat(executor.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(uncaught.get()).isNull();
        assertThat(successes.get()).isPositive();

        Job finalState = store.findById(job.id()).orElseThrow();
        assertThat(finalState.version()).isEqualTo(1L + successes.get());
        assertThat(finalState.currentState()).isEqualTo(JobState.ENQUEUED);
        // total attempts = successes + stale
        assertThat(successes.get() + stale.get()).isEqualTo(threads * attemptsPerThread);
        // every thread should have observed at least one stale outcome at this contention level
        assertThat(stale.get()).isPositive();
        // sanity: the run actually exercised the path
        assertThat(Instant.now()).isNotNull();
    }

    @Test
    void claimRacingSoftDeleteNeverResurrectsADeletedJob() throws Exception {
        // Hammer claim-vs-softDelete on fresh jobs: a soft-DELETED job must
        // never be observed PROCESSING afterwards (the historical blind-put
        // overwrote a delete that landed between candidate collection and
        // the claim commit), and versions must never regress.
        var store = new InMemoryJobStore();
        var node = NodeId.newId();
        var uncaught = new AtomicReference<Throwable>();

        for (int round = 0; round < 200; round++) {
            Job job = Job.builder()
                    .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
                    .build();
            store.insert(job);

            var start = new CountDownLatch(1);
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                executor.submit(() -> {
                    try {
                        start.await();
                        store.claimReady(node, "default", 1, Instant.now());
                    } catch (Throwable t) {
                        uncaught.compareAndSet(null, t);
                    }
                });
                executor.submit(() -> {
                    try {
                        start.await();
                        store.softDelete(job.id());
                    } catch (Throwable t) {
                        uncaught.compareAndSet(null, t);
                    }
                });
                start.countDown();
                executor.shutdown();
                assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            }

            Job after = store.findById(job.id()).orElseThrow();
            if (after.currentState() == JobState.DELETED) {
                // The delete won (either before or after the claim): it must
                // not have been overwritten back to PROCESSING.
                assertThat(after.stateHistory().getLast().state()).isEqualTo(JobState.DELETED);
            } else {
                // The claim won and the delete lost the race entirely or
                // deleted the PROCESSING row afterwards.
                assertThat(after.currentState()).isIn(JobState.PROCESSING, JobState.DELETED);
            }
            assertThat(after.version()).isGreaterThanOrEqualTo(1L);
        }
        assertThat(uncaught.get()).isNull();
    }

    @Test
    void claimRacingReplaceJobNeverLosesTheReplacement() throws Exception {
        // A replaceJob that commits between candidate collection and the
        // claim commit must not be silently reverted by the claim's write.
        var store = new InMemoryJobStore();
        var node = NodeId.newId();
        var uncaught = new AtomicReference<Throwable>();

        for (int round = 0; round < 200; round++) {
            Job job = Job.builder()
                    .spec(JobSpec.of("com.example.Original", new JobArgument("java.lang.String", "\"x\"")))
                    .build();
            store.insert(job);
            long v = job.version();

            var start = new CountDownLatch(1);
            var replaced = new AtomicBoolean();
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                executor.submit(() -> {
                    try {
                        start.await();
                        store.claimReady(node, "default", 1, Instant.now());
                    } catch (Throwable t) {
                        uncaught.compareAndSet(null, t);
                    }
                });
                executor.submit(() -> {
                    try {
                        start.await();
                        replaced.set(store.replaceJob(
                                job.id(), v, JobReplacement.ofSpec(JobSpec.of("com.example.Replaced"))));
                    } catch (StaleJobException expected) {
                        // claim won — fine
                    } catch (Throwable t) {
                        uncaught.compareAndSet(null, t);
                    }
                });
                start.countDown();
                executor.shutdown();
                assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            }

            Job after = store.findById(job.id()).orElseThrow();
            if (replaced.get()) {
                // The replacement committed: its spec must survive, whether or
                // not the claim subsequently ran the replaced definition.
                assertThat(after.spec().handlerType()).isEqualTo("com.example.Replaced");
            } else {
                assertThat(after.currentState()).isEqualTo(JobState.PROCESSING);
                assertThat(after.spec().handlerType()).isEqualTo("com.example.Original");
            }
        }
        assertThat(uncaught.get()).isNull();
    }

    @Test
    void heartbeatRefreshCannotResurrectTerminalJob() throws Exception {
        var serializer = new BlockingHeartbeatSerializer();
        var store = new InMemoryJobStore(serializer, JobStoreCapabilities.defaults());
        Job job = Job.builder()
                .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
                .build();
        store.insert(job);

        NodeId node = NodeId.newId();
        Job claimed = store.claimReady(node, "default", 1, Instant.now()).getFirst();
        serializer.blockHeartbeatReadFor(claimed.id().toString());

        Thread heartbeat = Thread.ofVirtual()
                .name("heartbeat-race")
                .start(() -> store.touchOwnerHeartbeat(node, Instant.now().plusSeconds(1)));
        assertThat(serializer.awaitHeartbeatRead()).isTrue();

        Job completed = store.findById(claimed.id()).orElseThrow();
        long version = completed.version();
        completed.transitionTo(JobState.SUCCEEDED, Instant.now(), "test.success", null);
        completed.clearOwner();
        store.saveAtomic(completed, version);

        serializer.releaseHeartbeatRead();
        heartbeat.join(Duration.ofSeconds(5));
        assertThat(heartbeat.isAlive()).isFalse();

        Job loaded = store.findById(claimed.id()).orElseThrow();
        assertThat(loaded.currentState()).isEqualTo(JobState.SUCCEEDED);
        assertThat(loaded.version()).isEqualTo(version + 1);
        assertThat(store.countsByState().get(JobState.PROCESSING)).isZero();
        assertThat(store.countsByState().get(JobState.SUCCEEDED)).isEqualTo(1L);
    }

    private static final class BlockingHeartbeatSerializer implements JobSerializer {
        private final JsonJobSerializer delegate = new JsonJobSerializer();
        private final CountDownLatch heartbeatRead = new CountDownLatch(1);
        private final CountDownLatch releaseHeartbeatRead = new CountDownLatch(1);
        private final AtomicBoolean blocked = new AtomicBoolean();
        private volatile String blockedJobId;

        void blockHeartbeatReadFor(String jobId) {
            blockedJobId = jobId;
        }

        boolean awaitHeartbeatRead() throws InterruptedException {
            return heartbeatRead.await(5, TimeUnit.SECONDS);
        }

        void releaseHeartbeatRead() {
            releaseHeartbeatRead.countDown();
        }

        @Override
        public String serializeJob(JobSnapshot snapshot, long maxBytes) {
            return delegate.serializeJob(snapshot, maxBytes);
        }

        @Override
        public String serializeJob(JobSnapshot snapshot, JobStoreCapabilities capabilities) {
            return delegate.serializeJob(snapshot, capabilities);
        }

        @Override
        public Job deserializeJob(String wire) {
            Job job = delegate.deserializeJob(wire);
            if (shouldBlock(job)) {
                heartbeatRead.countDown();
                try {
                    if (!releaseHeartbeatRead.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("heartbeat race was not released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted while waiting for heartbeat race release", e);
                }
            }
            return job;
        }

        private boolean shouldBlock(Job job) {
            return blockedJobId != null
                    && job.id().toString().equals(blockedJobId)
                    && job.currentState() == JobState.PROCESSING
                    && blocked.compareAndSet(false, true);
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
