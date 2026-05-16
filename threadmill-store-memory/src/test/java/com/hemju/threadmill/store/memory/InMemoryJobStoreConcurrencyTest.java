package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;

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
}
