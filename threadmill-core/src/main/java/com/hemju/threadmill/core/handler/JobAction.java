package com.hemju.threadmill.core.handler;

/**
 * A {@link JobHandler} for jobs that need no per-invocation payload.
 *
 * <p>Equivalent to {@code JobHandler<NoPayload>}, but the user implements a
 * one-argument {@code run(JobExecutionContext)} so the unused payload type
 * parameter disappears entirely from the handler signature. Typical use:
 * recurring housekeeping, heartbeats, periodic sweeps — anything where every
 * invocation is identical.
 *
 * <p>From the engine's perspective a {@code JobAction} is a normal
 * {@code JobHandler<NoPayload>}: registration, routing, serialisation, and
 * dispatch go through the same paths. The engine always invokes
 * {@link #run(NoPayload, JobExecutionContext)}; the default delegates to the
 * one-argument variant so the user only writes the latter.
 */
public interface JobAction extends JobHandler<NoPayload> {

    @Override
    default void run(NoPayload payload, JobExecutionContext ctx) throws Exception {
        run(ctx);
    }

    /**
     * Run the job. See {@link JobHandler#run(JobPayload, JobExecutionContext)}
     * for the at-least-once delivery and exception semantics — both apply
     * here unchanged.
     */
    void run(JobExecutionContext ctx) throws Exception;
}
