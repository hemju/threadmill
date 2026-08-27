package com.hemju.threadmill.spring;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.store.JobStore;

/**
 * Shared after-commit nudge behaviour for the transactional schedulers.
 *
 * <p><strong>Nudges are after-commit in every enqueue mode, including
 * {@code join_transaction}.</strong> Joining the caller's transaction would
 * be the consistent-looking choice, but coalescing is by design one store
 * cell per task, so a joined nudge holds that row's write lock for the whole
 * business transaction: every concurrent producer of the same task serializes
 * behind it, capping throughput at one transaction at a time per task and
 * opening lock-ordering deadlocks that did not exist before. It fails
 * silently — correct at low rate, collapsing under load.
 *
 * <p>What joining would buy is closing the crash window between the caller's
 * commit and the nudge write, and issue #108 explicitly declares that window
 * a non-goal: the task's backstop schedule bounds the worst case. Paying a
 * scaling cliff for a guarantee the design does not need is the wrong trade,
 * so the nudge is deferred to {@code afterCommit} everywhere. Rollback
 * semantics are unchanged either way — {@code afterCommit} does not fire on
 * rollback.
 *
 * <p>Note that the store's nudge write must additionally refuse to join the
 * caller's connection even when invoked from {@code afterCommit}: Spring has
 * committed by then but has not yet unbound its resources, so a joining
 * boundary would run the write in a fresh transaction nobody commits. The
 * PostgreSQL store owns that transaction explicitly for this reason.
 *
 * <p><strong>Deduplicated per transaction.</strong> The documented usage is
 * "nudge once per work item and let the engine collapse them", so a batch
 * importer writing 500 rows calls this 500 times in one transaction. Each
 * distinct task is validated once and written once: the in-JVM coalescer only
 * collapses <em>concurrent</em> callers, and 500 sequential calls on the
 * commit thread would otherwise be 500 store round trips for byte-identical
 * writes.
 *
 * <p>That per-transaction batch is held by the registered synchronisation
 * itself and found by scanning the current synchronisation list —
 * deliberately not by {@code bindResource}. Spring's {@code suspend()} does
 * not unbind custom resources, so a resource-held batch would leak into a
 * {@code REQUIRES_NEW} inner transaction: the inner would skip registering
 * its own callback, and an inner commit under an outer rollback would drop
 * the nudge entirely. Synchronisation lists <em>are</em> suspended and
 * restored, so scoping to them is correct for nested and independent
 * transactions alike.
 */
final class DeferredNudge {

  private DeferredNudge() {}

  /**
   * Validate immediately and record the nudge after the caller's
   * transaction commits; with no active synchronisation, nudge now.
   *
   * @param nudgeNow performs the immediate nudge for a given task name
   */
  static void onCommit(String taskName, JobStore store, Consumer<String> nudgeNow, Logger log) {
    Objects.requireNonNull(taskName, "taskName");
    if (!TransactionSynchronizationManager.isSynchronizationActive()) {
      nudgeNow.accept(taskName);
      return;
    }
    // Fail fast while the caller can still react: the deferred write
    // re-validates in the store, but a throw from afterCommit is
    // contained below rather than surfaced to anyone.
    CronTask task = store
        .findCronTask(taskName)
        .orElseThrow(
            () -> new IllegalArgumentException("Unknown recurring task '" + taskName + "'"));
    if (!task.enabled()) {
      throw new IllegalStateException(
          "Recurring task '" + taskName + "' is disabled; an explicit pause wins over a nudge");
    }
    batchForCurrentTransaction(nudgeNow, log).add(taskName);
  }

  private static NudgeBatch batchForCurrentTransaction(Consumer<String> nudgeNow, Logger log) {
    for (TransactionSynchronization existing :
        TransactionSynchronizationManager.getSynchronizations()) {
      if (existing instanceof NudgeBatch batch) return batch;
    }
    NudgeBatch batch = new NudgeBatch(nudgeNow, log);
    TransactionSynchronizationManager.registerSynchronization(batch);
    return batch;
  }

  /** One transaction's set of tasks to nudge, flushed once on commit. */
  private static final class NudgeBatch implements TransactionSynchronization {

    // Insertion-ordered so the writes fire in the order the caller asked
    // for them, which keeps logs and traces readable.
    private final Set<String> taskNames = new LinkedHashSet<>();
    private final Consumer<String> nudgeNow;
    private final Logger log;

    NudgeBatch(Consumer<String> nudgeNow, Logger log) {
      this.nudgeNow = nudgeNow;
      this.log = log;
    }

    void add(String taskName) {
      taskNames.add(taskName);
    }

    @Override
    public void afterCommit() {
      for (String taskName : taskNames) {
        // Spring runs after-commit callbacks in a bare loop with no
        // per-item isolation: a throw here would silently skip every
        // later-registered synchronisation. A lost nudge degrades to
        // backstop-schedule latency by design, but say so loudly.
        try {
          nudgeNow.accept(taskName);
        } catch (RuntimeException e) {
          log.error(
              "Threadmill after-commit nudge for recurring task '{}' was NOT recorded; "
                  + "the task's backstop schedule bounds the recovery latency",
              taskName,
              e);
        }
      }
    }
  }
}
