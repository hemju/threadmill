package com.hemju.threadmill.store.postgres;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bounded cursor hints guarded by short in-memory critical sections. JDBC work
 * always happens outside this object. Access-order eviction preserves active
 * queues' fairness progress and discards the least recently polled hint when
 * more queues are active than the bound can retain.
 */
final class PendingKeyCursors {
  private final int maxTracked;
  private final Map<String, PendingKeyCursor> cursors = new LinkedHashMap<>(16, 0.75f, true);

  PendingKeyCursors(int maxTracked) {
    if (maxTracked < 1) {
      throw new IllegalArgumentException("maxTracked must be positive");
    }
    this.maxTracked = maxTracked;
  }

  synchronized PendingKeyCursor current(String queue) {
    return cursors.get(queue);
  }

  synchronized void advance(String queue, PendingKeyCursor expected, String nextKey) {
    if (expected != null) {
      if (isCurrent(queue, expected)) {
        cursors.put(queue, new PendingKeyCursor(nextKey));
      }
      return;
    }
    if (cursors.containsKey(queue)) {
      return;
    }
    if (cursors.size() >= maxTracked) {
      var leastRecentlyUsed = cursors.keySet().iterator().next();
      cursors.remove(leastRecentlyUsed);
    }
    cursors.put(queue, new PendingKeyCursor(nextKey));
  }

  synchronized void clear(String queue, PendingKeyCursor expected) {
    if (expected != null && isCurrent(queue, expected)) {
      cursors.remove(queue);
    }
  }

  private boolean isCurrent(String queue, PendingKeyCursor expected) {
    // Reference identity, never equals(): a wrapped generation may carry the
    // same after value as a stale generation observed by another poll.
    return cursors.get(queue) == expected;
  }
}
