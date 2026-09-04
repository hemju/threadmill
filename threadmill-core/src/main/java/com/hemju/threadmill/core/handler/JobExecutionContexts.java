package com.hemju.threadmill.core.handler;

/**
 * Propagation of the current {@link JobExecutionContext} to handler code.
 *
 * <p>The engine binds {@link #CURRENT} around every {@code handler.run(...)}
 * call, and {@link JobExecutionContext#current()} reads it. The binding is a
 * Java 25 {@code ScopedValue}: it is inherited by threads forked through a
 * {@code StructuredTaskScope} opened inside the bound scope, but
 * <strong>not</strong> by virtual threads the handler starts directly or by
 * tasks submitted to a plain executor. Code that fans out through an executor
 * wraps each task with {@code EngineScopedValues.capturing(...)} to carry the
 * binding across.
 *
 * <p>This class lives with the handler API rather than the engine so that the
 * handler-facing accessor does not depend on engine internals; the engine's
 * {@code EngineScopedValues.CURRENT} is the same scoped value under its
 * engine-side name.
 */
public final class JobExecutionContexts {

  /** The currently executing job's context. Unbound outside a handler run. */
  public static final ScopedValue<JobExecutionContext> CURRENT = ScopedValue.newInstance();

  private JobExecutionContexts() {}
}
