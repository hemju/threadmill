package com.hemju.threadmill.core.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.JobStateEntry;
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.store.JobSearch;
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

    private static final Logger LOG = LoggerFactory.getLogger(RetryInterceptor.class);

    private static final int RESCHEDULE_SAVE_ATTEMPTS = 3;
    private static final long RESCHEDULE_SAVE_BACKOFF_MS = 50L;

    private final JobStore store;
    private final RetryPolicy defaultPolicy;
    // Iterated from concurrent worker virtual threads while policyFor may
    // still register entries; most-specific matching scans every entry, so
    // iteration order is irrelevant.
    private final Map<Class<? extends Throwable>, RetryPolicy> byExceptionType = new ConcurrentHashMap<>();

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
        if (kind == FailureCause.SHUTDOWN) {
            rescheduleShutdownInterrupted(job);
            return;
        }
        RetryPolicy policy = effectivePolicy(job, cause);
        if (job.attempts() >= policy.maxAttempts()) {
            return;
        }
        rescheduleWithBackoff(job, policy, "engine.retry-after-failure");
    }

    private void rescheduleWithBackoff(Job job, RetryPolicy policy, String reason) {
        int attempts = job.attempts();
        // Stores increment attempts at claim, so attempts is already 1 on the
        // first failure: the first retry waits exactly initialBackoff and the
        // delay doubles per subsequent attempt. Computed in millis so a
        // sub-second policy is not truncated to an immediate retry.
        long capMillis = Duration.ofHours(1).toMillis();
        long initialMillis = Math.max(0, policy.initialBackoff().toMillis());
        long backoffMillis = initialMillis >= capMillis
                ? capMillis
                : Math.min(capMillis, initialMillis << Math.min(Math.max(0, attempts - 1), 10));
        var next = Instant.now().plusMillis(backoffMillis);
        long expectedVersion = job.version();
        try {
            job.transitionTo(
                    JobState.SCHEDULED,
                    Instant.now(),
                    reason,
                    "retry " + (attempts + 1) + " of " + policy.maxAttempts());
            job.scheduleAt(next);
            job.clearOwner();
            saveRescheduleWithRetry(job, expectedVersion);
        } catch (StaleJobException ignored) {
            // Another node beat us to the next state for this job — fine.
        }
    }

    /**
     * Recovery scan for jobs stranded in FAILED with unspent retry budget —
     * the crash window between the terminal FAILED save and this
     * interceptor's reschedule save leaves exactly that shape, and without a
     * scan such jobs are stranded forever. Pages through the whole FAILED
     * population; only jobs older than {@code minAge} are touched, so a
     * reschedule that is mid-flight on another node is never raced.
     *
     * <p>The original exception is gone, so per-exception-type policies
     * cannot apply here: the ceiling is the per-job metadata override or the
     * global default. A stranded job possibly getting one extra retry beats
     * a stranded job never running again — handlers are idempotent by
     * contract. Runs on the maintenance leader at the retention cadence,
     * BEFORE the workflow reconciliation sweep, so a recovered parent is
     * SCHEDULED again by the time the sweep judges its AWAITING children.
     */
    public int recoverStrandedFailures(int pageSize, Duration minAge) {
        Instant cutoff = Instant.now().minus(minAge);
        int recovered = 0;
        int size = Math.max(1, pageSize);
        for (int offset = 0; ; offset += size) {
            List<Job> failed = store.searchJobs(new JobSearch(JobState.FAILED, null, null, size, offset));
            for (Job job : failed) {
                List<JobStateEntry> history = job.stateHistory();
                if (history.isEmpty()) continue;
                if (history.getLast().at().isAfter(cutoff)) continue;
                RetryPolicy policy = effectivePolicy(job, null);
                if (job.attempts() >= policy.maxAttempts()) continue;
                rescheduleWithBackoff(job, policy, "engine.retry-recovered");
                recovered++;
            }
            if (failed.size() < size) {
                return recovered;
            }
        }
    }

    /**
     * A shutdown-interrupted attempt is not the job's fault: reschedule it
     * immediately (a surviving node picks it up at the next promotion) and
     * revert the claim-time attempt increment so rolling deploys never
     * consume retry budget. Never final — budget is not consulted.
     */
    private void rescheduleShutdownInterrupted(Job job) {
        long expectedVersion = job.version();
        try {
            job.revertAttempt();
            job.transitionTo(
                    JobState.SCHEDULED, Instant.now(), "engine.retry-after-shutdown", "requeued by node shutdown");
            job.scheduleAt(Instant.now());
            job.clearOwner();
            saveRescheduleWithRetry(job, expectedVersion);
        } catch (StaleJobException ignored) {
            // Another node beat us to the next state for this job — fine.
        }
    }

    /**
     * Persist the FAILED&nbsp;→&nbsp;SCHEDULED retry transition, retrying transient
     * store errors with a short bounded backoff. The terminal FAILED save has
     * already landed, so a swallowed reschedule here permanently strands the job
     * in FAILED with unspent retry budget — the reschedule must be as resilient
     * as the terminal save itself. {@link StaleJobException} is not transient
     * (another node moved the job) and propagates to the caller's catch.
     */
    private void saveRescheduleWithRetry(Job job, long expectedVersion) {
        int attempt = 0;
        while (true) {
            try {
                store.saveAtomic(job, expectedVersion);
                return;
            } catch (StaleJobException stale) {
                throw stale;
            } catch (RuntimeException transientError) {
                if (++attempt >= RESCHEDULE_SAVE_ATTEMPTS) {
                    throw transientError;
                }
                try {
                    Thread.sleep(RESCHEDULE_SAVE_BACKOFF_MS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw transientError;
                }
            }
        }
    }

    /** Public for testing — returns the policy that would apply given a job + cause. */
    RetryPolicy effectivePolicy(Job job, Throwable cause) {
        // 1. Per-job override via metadata. Metadata is user-mutable — a
        // malformed value must fall through to the remaining precedence
        // levels instead of silently cancelling the retry.
        var attemptsMeta = job.metadata().get(META_MAX_ATTEMPTS);
        var backoffMeta = job.metadata().get(META_INITIAL_BACKOFF_SECONDS);
        if (attemptsMeta.isPresent() || backoffMeta.isPresent()) {
            try {
                int attempts = attemptsMeta.map(s -> Integer.parseInt(s.trim())).orElse(defaultPolicy.maxAttempts());
                Duration backoff = backoffMeta
                        .map(s -> Duration.ofSeconds(Long.parseLong(s.trim())))
                        .orElse(defaultPolicy.initialBackoff());
                return new RetryPolicy(attempts, backoff);
            } catch (RuntimeException malformed) {
                LOG.warn(
                        "Ignoring malformed per-job retry metadata for job {} ({}) — applying the remaining precedence levels",
                        job.id(),
                        malformed.toString());
            }
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
