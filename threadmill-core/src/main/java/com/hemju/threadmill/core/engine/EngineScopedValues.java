package com.hemju.threadmill.core.engine;

import java.util.Objects;

import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobExecutionContexts;

/**
 * Java 25 scoped values used by the engine to publish per-execution context
 * into handler code.
 *
 * <p>{@code java.lang.ScopedValue} is final in Java 25 (JEP 506). Bindings
 * are inherited <em>only</em> by threads forked through a
 * {@code StructuredTaskScope} opened inside the bound scope — they are
 * <em>not</em> inherited by {@code Thread.ofVirtual().start(...)} or by
 * tasks submitted to {@code Executors.newVirtualThreadPerTaskExecutor()}.
 * On such threads {@code JobExecutionContexts.CURRENT.get()} throws
 * {@code NoSuchElementException}. A handler that fans out through a plain
 * executor must wrap each task with {@link #capturing(Runnable)} to carry
 * the {@link JobExecutionContext} across.
 *
 * <p>The scoped value itself is owned by the handler API
 * ({@link JobExecutionContexts#CURRENT}) so that
 * {@link JobExecutionContext#current()} does not depend on the engine.
 */
public final class EngineScopedValues {

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
    if (!JobExecutionContexts.CURRENT.isBound()) return task;
    JobExecutionContext context = JobExecutionContexts.CURRENT.get();
    return () -> ScopedValue.where(JobExecutionContexts.CURRENT, context).run(task);
  }

  private EngineScopedValues() {}
}
