package com.hemju.threadmill.store.postgres;

/**
 * One cursor generation. Instances with the same {@link #after} value remain
 * distinct generations; {@link PendingKeyCursors} accepts mutations only from
 * the exact generation a poll observed.
 */
final class PendingKeyCursor {
  private final String after;

  PendingKeyCursor(String after) {
    this.after = after;
  }

  String after() {
    return after;
  }
}
