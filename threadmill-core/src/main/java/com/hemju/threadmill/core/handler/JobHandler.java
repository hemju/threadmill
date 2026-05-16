package com.hemju.threadmill.core.handler;

/**
 * The user-implemented entry point for a job.
 *
 * <p>A {@code JobHandler} is the typed counterpart to a {@link JobPayload}.
 * One handler class corresponds to one payload type, and a job's
 * {@code JobSpec} names the handler's fully-qualified type. The engine
 * resolves the handler instance through a {@link JobHandlerResolver} —
 * usually backed by the host's DI container.
 *
 * <p>Threadmill provides <strong>at-least-once</strong> delivery: a handler
 * may be invoked more than once for the same logical job (for example after
 * a node crash). Implementations must therefore be idempotent.
 *
 * @param <P> the payload type this handler consumes
 */
@FunctionalInterface
public interface JobHandler<P extends JobPayload> {

    /**
     * Run the job.
     *
     * @param payload  the typed payload to operate on
     * @param ctx      the per-execution context
     * @throws Exception any exception thrown here funnels through the engine's
     *                   single failure path: state transition to {@code FAILED}
     *                   plus interceptor notification
     */
    void run(P payload, JobExecutionContext ctx) throws Exception;
}
