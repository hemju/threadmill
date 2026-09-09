package com.hemju.threadmill.core.engine;

import com.hemju.threadmill.core.FailureDecision;
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
   * Resolve a durable disposition before FAILED is saved. Return null to defer
   * to the next interceptor. The first decision wins; absent a policy the
   * engine records final failure. This hook must not write to the store or
   * perform external effects. Completion notification still uses onProcessingFailed.
   */
  default FailureDecision onProcessingFailureDecision(
      Job job, JobExecutionContext ctx, Throwable cause, FailureCause kind) {
    return null;
  }

  /**
   * Invoked exactly once when the engine decides a job has failed —
   * regardless of whether the cause was a thrown exception, a timeout,
   * or an orphan reclaim. This is the engine's single failure path.
   */
  default void onProcessingFailed(
      Job job, JobExecutionContext ctx, Throwable cause, FailureCause causeKind) {}

  /**
   * Release attempt-local resources on every engine exit, including stale writes,
   * quarantine, shutdown and orphan recovery. Runs on the execution's own thread
   * in reverse registration order. The same context instance identifies this
   * execution; recovery uses a separate context. This hook does not certify a
   * persisted outcome and must not enqueue work or change job state.
   */
  default void onProcessingFinished(Job job, JobExecutionContext ctx) {}

  /** Invoked when the engine transitions a job between states. */
  default void onStateChange(Job job, JobState from, JobState to) {}

  /** What triggered a failure transition. */
  enum FailureCause {
    EXCEPTION,
    TIMEOUT,
    ORPHAN_RECLAIM,
    QUARANTINE,
    /**
     * The handler was interrupted because its node is shutting down —
     * not the job's fault. {@code RetryInterceptor} reschedules the job
     * immediately without consuming a retry attempt, so rolling deploys
     * do not erode retry budgets.
     */
    SHUTDOWN
  }
}
