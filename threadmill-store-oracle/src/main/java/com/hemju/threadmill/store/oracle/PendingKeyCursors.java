package com.hemju.threadmill.store.oracle;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bounded per-queue keyset cursors for the keyed claim lane, guarded by short
 * in-memory critical sections; JDBC work always happens outside this object.
 *
 * <p>A cursor is a disposable fairness hint: it lets later polls reach keys
 * beyond a page of blocked early keys. Mutations are accepted only from the
 * exact generation a poll observed (reference identity), so a slow concurrent
 * poll cannot overwrite a newer cursor. Access-order eviction keeps active
 * queues' progress and drops the least recently polled hint when more queues
 * are active than the bound retains.
 */
final class PendingKeyCursors {

  /** One cursor generation; equal {@code after} values remain distinct generations. */
  static final class Cursor {
    private final String after;

    Cursor(String after) {
      this.after = after;
    }

    String after() {
      return after;
    }
  }

  private final int maxTracked;
  private final Map<String, Cursor> cursors = new LinkedHashMap<>(16, 0.75f, true);

  PendingKeyCursors(int maxTracked) {
    if (maxTracked < 1) {
      throw new IllegalArgumentException("maxTracked must be positive");
    }
    this.maxTracked = maxTracked;
  }

  synchronized Cursor current(String queue) {
    return cursors.get(queue);
  }

  synchronized void advance(String queue, Cursor expected, String nextKey) {
    if (expected != null) {
      if (cursors.get(queue) == expected) {
        cursors.put(queue, new Cursor(nextKey));
      }
      return;
    }
    if (cursors.containsKey(queue)) {
      return;
    }
    if (cursors.size() >= maxTracked) {
      cursors.remove(cursors.keySet().iterator().next());
    }
    cursors.put(queue, new Cursor(nextKey));
  }

  synchronized void clear(String queue, Cursor expected) {
    if (expected != null && cursors.get(queue) == expected) {
      cursors.remove(queue);
    }
  }
}
