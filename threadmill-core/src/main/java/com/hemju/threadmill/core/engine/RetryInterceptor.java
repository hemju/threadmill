package com.hemju.threadmill.core.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.store.JobStore;

/**
 * Built-in retry policy implemented <strong>as an interceptor</strong>, with
 * the precedence model documented in {@link RetryPolicy}: per-job &gt;
 * per-exception-type &gt; global default.
 *
 * <ul>
 *   <li><strong>Per-job override:</strong> a job's metadata may contain
 *       the key {@code "threadmill.retry.maxAttempts"} (an integer) and/or
 *       {@code "threadmill.retry.initialBackoffSeconds"} (a long). Either
 *       key alone activates the per-job override.</li>
 *   <li><strong>Per-exception-type:</strong> register via
 *       {@link #policyFor(Class, RetryPolicy)}. The most specific match
 *       (deepest in the exception's class hierarchy) wins.</li>
 *   <li><strong>Global default:</strong> the constructor arguments.</li>
 * </ul>
 *
 * <p>Quarantine failures never retry.
 */
public final class RetryInterceptor implements JobInterceptor {

    public static final String META_MAX_ATTEMPTS = "threadmill.retry.maxAttempts";
    public static final String META_INITIAL_BACKOFF_SECONDS = "threadmill.retry.initialBackoffSeconds";

    private final JobStore store;
    private final RetryPolicy defaultPolicy;
    private final Map<Class<? extends Throwable>, RetryPolicy> byExceptionType = new LinkedHashMap<>();

    public RetryInterceptor(JobStore store, int maxAttempts, Duration initialBackoff) {
        this.store = Objects.requireNonNull(store, "store");
        this.defaultPolicy = new RetryPolicy(maxAttempts, initialBackoff);
    }

    /** Register a per-exception-type retry policy. Most-specific wins. */
    public RetryInterceptor policyFor(Class<? extends Throwable> exceptionType, RetryPolicy policy) {
        Objects.requireNonNull(exceptionType, "exceptionType");
        Objects.requireNonNull(policy, "policy");
        byExceptionType.put(exceptionType, policy);
        return this;
    }

    @Override
    public void onProcessingFailed(Job job, JobExecutionContext ctx, Throwable cause, FailureCause kind) {
        if (kind == FailureCause.QUARANTINE) return;
        RetryPolicy policy = effectivePolicy(job, cause);

        int attempts = job.attempts();
        if (attempts >= policy.maxAttempts()) {
            return;
        }
        long backoffSeconds = Math.min(
                policy.initialBackoff().toSeconds() * (1L << Math.min(attempts, 10)),
                Duration.ofHours(1).toSeconds());
        var next = Instant.now().plusSeconds(Math.max(0, backoffSeconds));
        long expectedVersion = job.version();
        try {
            job.transitionTo(
                    JobState.SCHEDULED,
                    Instant.now(),
                    "engine.retry-after-failure",
                    "retry " + (attempts + 1) + " of " + policy.maxAttempts());
            job.scheduleAt(next);
            job.clearOwner();
            store.saveAtomic(job, expectedVersion);
        } catch (StaleJobException ignored) {
            // Another node beat us to the next state for this job — fine.
        }
    }

    /** Public for testing — returns the policy that would apply given a job + cause. */
    RetryPolicy effectivePolicy(Job job, Throwable cause) {
        // 1. Per-job override via metadata.
        var attemptsMeta = job.metadata().get(META_MAX_ATTEMPTS);
        var backoffMeta = job.metadata().get(META_INITIAL_BACKOFF_SECONDS);
        if (attemptsMeta.isPresent() || backoffMeta.isPresent()) {
            int attempts = attemptsMeta.map(Integer::parseInt).orElse(defaultPolicy.maxAttempts());
            Duration backoff =
                    backoffMeta.map(s -> Duration.ofSeconds(Long.parseLong(s))).orElse(defaultPolicy.initialBackoff());
            return new RetryPolicy(attempts, backoff);
        }
        // 2. Per-exception-type policy (most specific match wins).
        RetryPolicy match = null;
        Class<?> best = null;
        if (cause != null) {
            for (var e : byExceptionType.entrySet()) {
                if (e.getKey().isInstance(cause)) {
                    if (best == null || best.isAssignableFrom(e.getKey())) {
                        match = e.getValue();
                        best = e.getKey();
                    }
                }
            }
        }
        if (match != null) return match;
        // 3. Global default.
        return defaultPolicy;
    }
}
