package com.hemju.threadmill.core.engine;

import java.util.Objects;

import com.hemju.threadmill.core.handler.JobExecutionContext;

/**
 * Java 25 scoped values used by the engine to publish per-execution context
 * into handler code.
 *
 * <p>{@code java.lang.ScopedValue} is final in Java 25 (JEP 506). Bindings
 * are inherited <em>only</em> by threads forked through a
 * {@code StructuredTaskScope} opened inside the bound scope — they are
 * <em>not</em> inherited by {@code Thread.ofVirtual().start(...)} or by
 * tasks submitted to {@code Executors.newVirtualThreadPerTaskExecutor()}.
 * On such threads {@code CURRENT.get()} throws
 * {@code NoSuchElementException}. A handler that fans out through a plain
 * executor must wrap each task with {@link #capturing(Runnable)} to carry
 * the {@link JobExecutionContext} across.
 *
 * <p>Threadmill exposes a single binding here; if richer published values
 * are needed later, add them through the same accessor.
 */
public final class EngineScopedValues {

    /** The currently-executing job's context, or null when not inside a handler. */
    public static final ScopedValue<JobExecutionContext> CURRENT = ScopedValue.newInstance();

    /**
     * Wrap {@code task} so the calling thread's current
     * {@link JobExecutionContext} binding is re-established around its run.
     * Use this when fanning out to a plain (virtual-thread) executor, which
     * does not inherit scoped-value bindings; structured concurrency via
     * {@code StructuredTaskScope} inherits them without help.
     *
     * <p>If no context is bound on the calling thread, the task is returned
     * unchanged.
     */
    public static Runnable capturing(Runnable task) {
        Objects.requireNonNull(task, "task");
        if (!CURRENT.isBound()) return task;
        JobExecutionContext context = CURRENT.get();
        return () -> ScopedValue.where(CURRENT, context).run(task);
    }

    private EngineScopedValues() {}
}
