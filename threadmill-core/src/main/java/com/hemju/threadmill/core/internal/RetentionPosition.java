package com.hemju.threadmill.core.internal;

import java.time.Instant;

import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.store.RetentionCursor;

/** Internal time/id cursor encoding shared by the bundled stores, not a public wire contract. */
public record RetentionPosition(Instant at, JobId id) implements Comparable<RetentionPosition> {
  public RetentionCursor cursor() {
    return new RetentionCursor(at + "/" + id);
  }

  public static RetentionPosition from(RetentionCursor cursor) {
    var parts = cursor.value().split("/", 2);
    if (parts.length != 2) throw new IllegalArgumentException("Invalid retention cursor");
    return new RetentionPosition(Instant.parse(parts[0]), JobId.parse(parts[1]));
  }

  @Override
  public int compareTo(RetentionPosition other) {
    int time = at.compareTo(other.at);
    return time == 0 ? id.compareTo(other.id) : time;
  }
}
