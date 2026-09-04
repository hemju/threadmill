package com.hemju.threadmill.store.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PostgresPendingKeyCursorsTest {

  @Test
  void staleGenerationCannotAdvanceOrClearCursorAfterValueWraps() {
    var cursors = new PostgresJobStore.PendingKeyCursors(3);
    cursors.advance("queue", null, "a");
    var stale = cursors.current("queue");

    cursors.advance("queue", stale, "b");
    var middle = cursors.current("queue");
    cursors.advance("queue", middle, "a");
    var current = cursors.current("queue");

    assertThat(current).isNotSameAs(stale);
    assertThat(current.after()).isEqualTo(stale.after());

    cursors.advance("queue", stale, "stale-advance");
    cursors.clear("queue", stale);

    assertThat(cursors.current("queue")).isSameAs(current);
    assertThat(cursors.current("queue").after()).isEqualTo("a");
  }

  @Test
  void boundedEvictionRotatesAcrossTrackedQueues() {
    var cursors = new PostgresJobStore.PendingKeyCursors(3);
    cursors.advance("queue-1", null, "a");
    cursors.advance("queue-2", null, "a");
    cursors.advance("queue-3", null, "a");

    cursors.advance("queue-4", null, "a");
    assertThat(cursors.current("queue-1")).isNull();

    cursors.advance("queue-1", null, "b");
    assertThat(cursors.current("queue-2")).isNull();

    cursors.advance("queue-5", null, "a");
    assertThat(cursors.current("queue-3")).isNull();
    assertThat(cursors.current("queue-1")).isNotNull();
    assertThat(cursors.current("queue-4")).isNotNull();
    assertThat(cursors.current("queue-5")).isNotNull();
  }
}
