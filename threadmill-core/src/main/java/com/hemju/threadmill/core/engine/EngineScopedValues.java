package com.hemju.threadmill.core.engine;

import com.hemju.threadmill.core.handler.JobExecutionContext;

/**
 * Java 25 scoped values used by the engine to publish per-execution context
 * into handler code.
 *
 * <p>{@code java.lang.ScopedValue} is final in Java 25 (JEP 506). It is
 * inherited by virtual threads spawned from inside the bound scope, so a
 * handler that fans out work into a virtual-thread executor still sees the
 * right {@link JobExecutionContext}.
 *
 * <p>Threadmill exposes a single binding here; if richer published values
 * are needed later, add them through the same accessor.
 */
public final class EngineScopedValues {

    /** The currently-executing job's context, or null when not inside a handler. */
    public static final ScopedValue<JobExecutionContext> CURRENT = ScopedValue.newInstance();

    private EngineScopedValues() {}
}
