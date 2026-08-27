package com.hemju.threadmill.soak.harness;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory stand-in for an application's outbox / work table, used by the
 * {@code nudge-pump} scenario.
 *
 * <p>Producers append a work row and then nudge the recurring pump task; the
 * pump handler drains everything visible. That is the exact shape issue #108
 * exists for, and it makes the feature's load-bearing guarantee checkable
 * end-to-end from the trace: a row appended before a pump run starts must be
 * drained by the time that run finishes. A swallowed nudge shows up as a row
 * that sits undrained while runs happen after it.
 *
 * <p>Static like {@link SoakExecutionTrace} and for the same reason: handlers
 * are resolved reflectively, so a static holder is the only injection point.
 * Runs are one-per-JVM (the endurance orchestrator gives each backend its own
 * process) and the harness's JUnit smokes run sequentially, so
 * {@link #reset()} at scenario construction is enough isolation.
 */
public final class SoakOutbox {

  private static final ConcurrentLinkedQueue<Long> ROWS = new ConcurrentLinkedQueue<>();
  private static final AtomicLong NEXT_SEQ = new AtomicLong();
  private static final AtomicLong RUNS_STARTED = new AtomicLong();

  private SoakOutbox() {}

  /** Clear all rows and restart sequence numbering; called once per run. */
  public static void reset() {
    ROWS.clear();
    NEXT_SEQ.set(0);
    RUNS_STARTED.set(0);
  }

  /**
   * Count one pump execution, recorded at the start of the handler body.
   *
   * <p>This is how the scenario holds the tail of a run open: the harness's
   * generic drain phase waits for active <em>jobs</em>, and a nudge that has
   * not yet been materialized is not a job — so without this the nodes can
   * stop within milliseconds of the final nudge, long before the ≤1s
   * maintenance tick could serve it, and the run-after-wake completeness
   * check would fail on a healthy engine.
   */
  public static void runStarted() {
    RUNS_STARTED.incrementAndGet();
  }

  /** Pump executions started so far this run. */
  public static long runsStarted() {
    return RUNS_STARTED.get();
  }

  /**
   * Append one work row and emit {@code work_recorded}.
   *
   * <p>The trace event is emitted <em>after</em> the row is visible, so a
   * recorded timestamp always postdates actual visibility — the invariant
   * can then treat "recorded before a run started" as "must have been
   * drained by that run" without a race.
   *
   * @return the row's sequence number
   */
  public static long append() {
    long seq = NEXT_SEQ.incrementAndGet();
    ROWS.add(seq);
    var fields = new LinkedHashMap<String, Object>();
    fields.put("seq", seq);
    SoakExecutionTrace.emit("work_recorded", fields);
    return seq;
  }

  /**
   * Drain every currently-visible row and emit {@code work_drained} with the
   * exact sequence numbers taken. Emitting the ids (rather than a count)
   * is what lets the invariant prove membership instead of guessing from
   * totals.
   */
  public static List<Long> drainAll() {
    var drained = new ArrayList<Long>();
    for (Long seq = ROWS.poll(); seq != null; seq = ROWS.poll()) {
      drained.add(seq);
    }
    var fields = new LinkedHashMap<String, Object>();
    fields.put("count", drained.size());
    fields.put("seqs", drained);
    SoakExecutionTrace.emit("work_drained", fields);
    return drained;
  }

  /** Rows currently waiting to be drained; for end-of-run reporting. */
  public static int pending() {
    return ROWS.size();
  }
}
