package com.hemju.threadmill.test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Frozen job bodies emitted by the released v0.3.0 serializer. */
public final class LegacyJobFixtures {
  public static final List<String> NAMES = List.of(
      "ready",
      "scheduled",
      "processing",
      "failed",
      "succeeded-parent",
      "awaiting-child",
      "deleted",
      "quarantined");

  private LegacyJobFixtures() {}

  /** Read the original UTF-8 body, including its final resource newline. */
  public static String wire(String name) {
    if (!NAMES.contains(name)) throw new IllegalArgumentException("Unknown fixture " + name);
    try (var input = LegacyJobFixtures.class.getResourceAsStream(
        "/com/hemju/threadmill/test/compatibility/v0.3.0/" + name + ".json")) {
      if (input == null) throw new IllegalStateException("Missing fixture " + name);
      return new String(input.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException failure) {
      throw new UncheckedIOException(failure);
    }
  }
}
