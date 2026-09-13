package com.hemju.threadmill.soak.harness;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LockEventsWriterTest {
  @Test
  void highCardinalitySummaryIsBoundedWithoutDroppingRawLockEvents(@TempDir Path directory)
      throws Exception {
    var tracePath = directory.resolve("trace.jsonl");
    try (var trace = new SoakTraceWriter(tracePath)) {
      for (int i = 0; i < 2000; i++) {
        var fields = Map.<String, Object>of(
            "jobId", "job-" + i, "lockKey", "key-" + i, "lockMode", "SHARED");
        trace.emit("lock_acquired", fields);
        trace.emit("lock_released", fields);
      }
    }
    var output = new OutputDir(directory, false);
    var summary = LockEventsWriter.write(tracePath, output);
    assertThat(summary.byKey()).hasSize(128);
    assertThat(summary.byKey().values().stream()
            .mapToLong(SummaryReport.LockStats::acquires)
            .sum())
        .isEqualTo(2000);
    assertThat(summary.byKey().get("(additional keys)").maxConcurrentShared()).isEqualTo(1);
    try (var rows = Files.lines(output.lockEventsJsonl())) {
      assertThat(rows.count()).isEqualTo(2000);
    }
  }
}
