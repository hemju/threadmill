package com.hemju.threadmill.core.handler;

import java.time.Instant;
import java.util.Optional;

import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobLog;
import com.hemju.threadmill.core.JobMetadata;
import com.hemju.threadmill.core.JobProgress;
import com.hemju.threadmill.core.NodeId;

/**
 * The view of a job exposed to user code during a run.
 *
 * <p>Critically, the context is <strong>not the same mutable structure the
 * engine serializes</strong>. It exposes the user-touchable areas
 * ({@link JobLog}, {@link JobProgress}, {@link JobMetadata}) directly — those
 * types are thread-safe and the engine snapshots them before serialization —
 * and otherwise gives the handler read-only access to identity and timing.
 *
 * <p>Per-execution propagation (job id, attempt number, MDC) is implemented
 * with {@code ScopedValue} (final in Java 25), not {@code ThreadLocal}, so
 * it survives across virtual threads spawned by handler code.
 */
public interface JobExecutionContext {

    /** The id of the job being executed. */
    JobId jobId();

    /** The id of the node executing the job. */
    NodeId nodeId();

    /** The attempt number, starting at 1. */
    int attempt();

    /** The instant the engine claimed this job for this attempt. */
    Instant claimedAt();

    /** Append-only per-job log. */
    JobLog log();

    /** Progress reporting for the job. */
    JobProgress progress();

    /** Mutable per-job metadata. */
    JobMetadata metadata();

    /** Record that this long-running job is alive and making progress. */
    default void checkIn() {}

    /** Record a check-in and append a user-visible log message. */
    default void checkIn(String message) {
        checkIn();
        log(message);
    }

    /** Update the current fraction complete, from {@code 0.0} through {@code 1.0}. */
    default void updateProgress(double fractionComplete) {
        progress().update(fractionComplete);
    }

    /** Append an INFO entry to the per-job log. */
    default void log(String message) {
        log().info(message);
    }

    /**
     * Record a result for this job. The engine persists it together with
     * the {@code SUCCEEDED} state transition. The result is bounded by the
     * same job size cap as the rest of the job body.
     */
    default void setResult(Object value) {
        // default no-op; the engine's ExecutionContext overrides this.
    }

    /** Read the result previously set by this handler, if any. */
    default Optional<Object> readResult() {
        return Optional.empty();
    }
}
