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
 * <p>For handlers that do not need a per-invocation payload — periodic
 * housekeeping, heartbeats, sweeps — implement {@link JobAction} instead.
 * It is a specialization of {@code JobHandler<NoPayload>} with a no-payload
 * run signature, so neither the user nor the engine carries an unused
 * type parameter.
 *
 * <p>Threadmill provides <strong>at-least-once</strong> delivery: a handler
 * may be invoked more than once for the same logical job (for example after
 * a node crash). Implementations must therefore be idempotent.
 *
 * <p>Ordinary exceptions and {@link AssertionError} are isolated to the job.
 * {@link VirtualMachineError} and {@link ThreadDeath}, including either error
 * wrapped in a cause chain, escape the engine as process-fatal conditions and
 * are never converted into a job failure or retry.
 *
 * @param <P> the payload type this handler consumes
 */
public interface JobHandler<P extends JobPayload> {

  /**
   * Run the job.
   *
   * @param payload the typed payload to operate on
   * @param ctx     the per-execution context
   * @throws Exception any non-fatal exception thrown here funnels through the
   *                   engine's single failure path: state transition to
   *                   {@code FAILED} plus interceptor notification
   */
  void run(P payload, JobExecutionContext ctx) throws Exception;
}
