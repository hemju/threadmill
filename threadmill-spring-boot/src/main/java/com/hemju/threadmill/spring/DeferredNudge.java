package com.hemju.threadmill.spring;

import java.util.Objects;

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
 * rollback — and the lock is held for microseconds instead of the caller's
 * transaction.
 */
final class DeferredNudge {

    private DeferredNudge() {}

    /**
     * Validate immediately and record the nudge after the caller's
     * transaction commits; with no active synchronisation, nudge now.
     *
     * @param nudgeNow the immediate nudge to run (after commit, or straight
     *                 away outside a transaction)
     */
    static void onCommit(String taskName, JobStore store, Runnable nudgeNow, Logger log) {
        Objects.requireNonNull(taskName, "taskName");
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            nudgeNow.run();
            return;
        }
        // Fail fast while the caller can still react: the deferred write
        // re-validates in the store, but a throw from afterCommit is
        // contained below rather than surfaced to anyone.
        CronTask task = store.findCronTask(taskName)
                .orElseThrow(() -> new IllegalArgumentException("Unknown recurring task '" + taskName + "'"));
        if (!task.enabled()) {
            throw new IllegalStateException(
                    "Recurring task '" + taskName + "' is disabled; an explicit pause wins over a nudge");
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                // Spring runs after-commit callbacks in a bare loop with no
                // per-item isolation: a throw here would silently skip every
                // later-registered synchronisation. A lost nudge degrades to
                // backstop-schedule latency by design, but say so loudly.
                try {
                    nudgeNow.run();
                } catch (RuntimeException e) {
                    log.error(
                            "Threadmill after-commit nudge for recurring task '{}' was NOT recorded; "
                                    + "the task's backstop schedule bounds the recovery latency",
                            taskName,
                            e);
                }
            }
        });
    }
}
