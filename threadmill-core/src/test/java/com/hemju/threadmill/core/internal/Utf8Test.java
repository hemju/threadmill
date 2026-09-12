package com.hemju.threadmill.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;

class Utf8Test {
  @Test
  void byteBudgetMatchesTheJdkForEveryUtf16CodeUnitAndSupplementaryPairs() {
    for (int c = 0; c <= Character.MAX_VALUE; c++) {
      var value = String.valueOf((char) c);
      assertThat(Utf8.length(value)).isEqualTo(value.getBytes(StandardCharsets.UTF_8).length);
    }
    for (var value :
        List.of("", "ascii", "😀中é", "\ud800x\udc00", "\ud800\ud800\udc00", "😀".repeat(1000))) {
      assertThat(Utf8.length(value)).isEqualTo(value.getBytes(StandardCharsets.UTF_8).length);
    }
  }
}
