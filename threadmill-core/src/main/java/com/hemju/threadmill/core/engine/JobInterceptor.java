package com.hemju.threadmill.core.engine;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.handler.JobExecutionContext;

/**
 * Lifecycle interception SPI for the processing engine.
 *
 * <p>Interceptors run inside the same engine thread the job runs on. They
 * are invoked once per job, in registration order, around the execution.
 *
 * <p><strong>Retry is implemented as an interceptor</strong>, not
 * special-cased in the engine — see {@code RetryInterceptor}. This keeps
 * the failure path uniform: anything that wants to react to a failure
 * registers as an interceptor.
 */
public interface JobInterceptor {

    /** Invoked just before the handler runs. */
    default void onProcessingStarting(Job job, JobExecutionContext ctx) {}

    /** Invoked after the handler returns normally. */
    default void onProcessingSucceeded(Job job, JobExecutionContext ctx) {}

    /**
     * Invoked exactly once when the engine decides a job has failed —
     * regardless of whether the cause was a thrown exception, a timeout,
     * or an orphan reclaim. This is the engine's single failure path.
     */
    default void onProcessingFailed(Job job, JobExecutionContext ctx, Throwable cause, FailureCause causeKind) {}

    /** Invoked when the engine transitions a job between states. */
    default void onStateChange(Job job, JobState from, JobState to) {}

    /** What triggered a failure transition. */
    enum FailureCause {
        EXCEPTION,
        TIMEOUT,
        ORPHAN_RECLAIM,
        QUARANTINE
    }
}
