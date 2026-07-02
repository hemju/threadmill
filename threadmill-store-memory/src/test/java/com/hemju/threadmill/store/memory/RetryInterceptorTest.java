package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.JobInterceptor;
import com.hemju.threadmill.core.engine.RetryInterceptor;
import com.hemju.threadmill.core.engine.RetryPolicy;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStore;

/**
 * Unit-level retry-interceptor regressions: backoff arithmetic, sub-second
 * policies, malformed per-job metadata, and concurrent policy registration.
 */
class RetryInterceptorTest {

    private InMemoryJobStore store;
    private final JsonJobSerializer serializer = new JsonJobSerializer();

    @BeforeEach
    void setUp() {
        store = new InMemoryJobStore();
    }

    private Job failedAfterFirstAttempt(String metaKey, String metaValue) {
        JobArgument arg = serializer.serializePayload(new EngineTestHandlers.HelloPayload("x"));
        Job.Builder b = Job.builder().spec(new JobSpec("com.example.H", List.of(arg)));
        if (metaKey != null) {
            b.metadata(metaKey, metaValue);
        }
        Job job = b.build();
        store.insert(job);
        Job claimed =
                store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        long version = claimed.version();
        claimed.transitionTo(JobState.FAILED, Instant.now(), "engine.exception", "boom");
        claimed.clearOwner();
        store.saveAtomic(claimed, version);
        return claimed;
    }

    @Test
    void recoveryScanReschedulesAStrandedFailedJobWithBudget() {
        var interceptor = new RetryInterceptor(store, 3, Duration.ofMillis(100));
        // FAILED with attempts=1 of 3 and no reschedule — the crash window
        // between the terminal FAILED save and the reschedule save.
        Job stranded = failedAfterFirstAttempt(null, null);

        int recovered = interceptor.recoverStrandedFailures(10, Duration.ZERO);

        assertThat(recovered).isEqualTo(1);
        assertThat(store.findById(stranded.id()).orElseThrow().currentState()).isEqualTo(JobState.SCHEDULED);
    }

    @Test
    void recoveryScanLeavesFinalFailedJobsAlone() {
        var interceptor = new RetryInterceptor(store, 3, Duration.ofMillis(100));
        // Per-job override caps the budget at 1 — this FAILED job is final.
        Job finalFailure = failedAfterFirstAttempt("threadmill.retry.maxAttempts", "1");

        int recovered = interceptor.recoverStrandedFailures(10, Duration.ZERO);

        assertThat(recovered).isZero();
        assertThat(store.findById(finalFailure.id()).orElseThrow().currentState())
                .isEqualTo(JobState.FAILED);
    }

    @Test
    void recoveryScanLeavesYoungFailedJobsToTheLiveHook() {
        var interceptor = new RetryInterceptor(store, 3, Duration.ofMillis(100));
        Job young = failedAfterFirstAttempt(null, null);

        int recovered = interceptor.recoverStrandedFailures(10, Duration.ofMinutes(5));

        assertThat(recovered).isZero();
        assertThat(store.findById(young.id()).orElseThrow().currentState()).isEqualTo(JobState.FAILED);
    }

    @Test
    void shutdownInterruptRescheduledImmediatelyWithoutConsumingAnAttempt() {
        var interceptor = new RetryInterceptor(store, 3, Duration.ofSeconds(10));
        Job failed = failedAfterFirstAttempt(null, null);
        assertThat(failed.attempts()).isEqualTo(1);

        interceptor.onProcessingFailed(
                failed, null, new InterruptedException("node closing"), JobInterceptor.FailureCause.SHUTDOWN);

        Job rescheduled = store.findById(failed.id()).orElseThrow();
        assertThat(rescheduled.currentState()).isEqualTo(JobState.SCHEDULED);
        // Immediately due — a surviving node picks it up at the next promotion.
        assertThat(rescheduled.scheduledFor().orElseThrow()).isBeforeOrEqualTo(Instant.now());
        // The claim-time increment is reverted: rolling deploys never erode budget.
        assertThat(rescheduled.attempts()).isZero();
    }

    @Test
    void firstRetryDelayEqualsInitialBackoff() {
        var interceptor = new RetryInterceptor(store, 3, Duration.ofSeconds(10));
        Job failed = failedAfterFirstAttempt(null, null);

        Instant before = Instant.now();
        interceptor.onProcessingFailed(
                failed, null, new RuntimeException("boom"), JobInterceptor.FailureCause.EXCEPTION);

        Job scheduled = store.findById(failed.id()).orElseThrow();
        assertThat(scheduled.currentState()).isEqualTo(JobState.SCHEDULED);
        Duration delay = Duration.between(before, scheduled.scheduledFor().orElseThrow());
        // attempts == 1 on the first failure: the delay must be one
        // initialBackoff, not 2x (the historical off-by-one).
        assertThat(delay).isGreaterThan(Duration.ofSeconds(8)).isLessThan(Duration.ofSeconds(12));
    }

    @Test
    void subSecondBackoffIsNotTruncatedToZero() {
        var interceptor = new RetryInterceptor(store, 3, Duration.ofMillis(500));
        Job failed = failedAfterFirstAttempt(null, null);

        Instant before = Instant.now();
        interceptor.onProcessingFailed(
                failed, null, new RuntimeException("boom"), JobInterceptor.FailureCause.EXCEPTION);

        Job scheduled = store.findById(failed.id()).orElseThrow();
        Duration delay = Duration.between(before, scheduled.scheduledFor().orElseThrow());
        assertThat(delay).isGreaterThan(Duration.ofMillis(250)).isLessThan(Duration.ofSeconds(1));
    }

    @Test
    void transientStoreErrorOnRescheduleIsRetriedNotSwallowed() {
        // The FAILED save has already landed; a transient store blip on the
        // reschedule must not silently strand the job in FAILED with budget left.
        // A proxy injects a single saveAtomic failure on the SCHEDULED transition.
        var blips = new java.util.concurrent.atomic.AtomicInteger(0);
        JobStore flakyStore = (JobStore) Proxy.newProxyInstance(
                JobStore.class.getClassLoader(), new Class<?>[] {JobStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("saveAtomic")
                            && ((Job) args[0]).currentState() == JobState.SCHEDULED
                            && blips.getAndIncrement() == 0) {
                        throw new RuntimeException("transient store blip");
                    }
                    try {
                        return method.invoke(store, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });

        JobArgument arg = serializer.serializePayload(new EngineTestHandlers.HelloPayload("x"));
        Job job = Job.builder().spec(new JobSpec("com.example.H", List.of(arg))).build();
        store.insert(job);
        Job claimed =
                store.claimReady(NodeId.newId(), "default", 1, Instant.now()).get(0);
        long version = claimed.version();
        claimed.transitionTo(JobState.FAILED, Instant.now(), "engine.exception", "boom");
        claimed.clearOwner();
        store.saveAtomic(claimed, version);

        var interceptor = new RetryInterceptor(flakyStore, 3, Duration.ofMillis(50));
        interceptor.onProcessingFailed(
                claimed, null, new RuntimeException("boom"), JobInterceptor.FailureCause.EXCEPTION);

        assertThat(blips.get()).isGreaterThanOrEqualTo(2); // first throw, then a successful retry
        assertThat(store.findById(claimed.id()).orElseThrow().currentState()).isEqualTo(JobState.SCHEDULED);
    }

    @Test
    void malformedRetryMetadataFallsBackToTheDefaultPolicyAndStillRetries() {
        var interceptor = new RetryInterceptor(store, 3, Duration.ofMillis(100));
        Job failed = failedAfterFirstAttempt(RetryInterceptor.META_MAX_ATTEMPTS, "abc");

        interceptor.onProcessingFailed(
                failed, null, new RuntimeException("boom"), JobInterceptor.FailureCause.EXCEPTION);

        // The malformed override must not silently cancel the retry.
        Job scheduled = store.findById(failed.id()).orElseThrow();
        assertThat(scheduled.currentState()).isEqualTo(JobState.SCHEDULED);
    }

    @Test
    void concurrentPolicyRegistrationDoesNotBreakTheFailurePath() throws Exception {
        var interceptor = new RetryInterceptor(store, 3, Duration.ofMillis(10));
        var firstError = new AtomicReference<Throwable>();
        var start = new CountDownLatch(1);

        Thread registrar = Thread.ofVirtual().unstarted(() -> {
            try {
                start.await();
                for (int i = 0; i < 2_000; i++) {
                    interceptor.policyFor(RuntimeException.class, RetryPolicy.of(2, Duration.ofMillis(i + 1)));
                    interceptor.policyFor(IllegalStateException.class, RetryPolicy.of(3, Duration.ofMillis(i + 1)));
                }
            } catch (Throwable t) {
                firstError.compareAndSet(null, t);
            }
        });
        Thread failer = Thread.ofVirtual().unstarted(() -> {
            try {
                start.await();
                for (int i = 0; i < 200; i++) {
                    Job failed = failedAfterFirstAttempt(null, null);
                    interceptor.onProcessingFailed(
                            failed, null, new IllegalStateException("boom"), JobInterceptor.FailureCause.EXCEPTION);
                }
            } catch (Throwable t) {
                firstError.compareAndSet(null, t);
            }
        });
        registrar.start();
        failer.start();
        start.countDown();
        registrar.join();
        failer.join();

        assertThat(firstError.get()).isNull();
    }
}
