package com.hemju.threadmill.spring;

import java.util.List;
import java.util.Objects;

import com.hemju.threadmill.core.JobId;

/**
 * An after-commit enqueue whose persistence could not be confirmed. Spring's
 * auto-configured scheduler publishes this event after the business transaction
 * has committed. A lost acknowledgement can leave the jobs durably inserted:
 * inspect the reserved ids before deciding how to recover. The event is an
 * observation, not a durable outbox or an automatic retry mechanism.
 *
 * @param jobIds immutable reserved ids for the failed insert operation
 * @param cause the store failure; may contain application-specific diagnostics
 */
public record AfterCommitEnqueueFailure(List<JobId> jobIds, Throwable cause) {
  public AfterCommitEnqueueFailure {
    jobIds = List.copyOf(jobIds);
    Objects.requireNonNull(cause, "cause");
  }
}
