package com.hemju.threadmill.core.schedule;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Names;
import com.hemju.threadmill.core.spec.JobArgument;

class CronTaskTest {

  @Test
  void derivedConcurrencyKeyIsNamespacedAndOnlyPresentWhenExclusive() {
    assertThat(CronTask.concurrencyKeyFor("nightly-sweep")).isEqualTo("recurring:nightly-sweep");
    assertThat(exclusiveTask("nightly-sweep", true).derivedConcurrencyKey())
        .isEqualTo("recurring:nightly-sweep");
    assertThat(exclusiveTask("nightly-sweep", false).derivedConcurrencyKey()).isNull();
  }

  @Test
  void derivedConcurrencyKeyFitsTheKeyCapWithoutSplittingSurrogatePairs() {
    // Task names may be up to Names.MAX_LENGTH characters of arbitrary
    // non-control text; Job.concurrencyKey caps at 256 UTF-8 bytes, and a
    // name of 4-byte code points blows straight through it. The truncation
    // must stay under the cap, stay on a code-point boundary (a split
    // surrogate pair is not valid UTF-16 and would corrupt the key), and
    // stay distinct for names sharing a prefix.
    String emoji = "🚀".repeat(120); // 120 astral code points, 480 UTF-8 bytes
    String key = CronTask.concurrencyKeyFor(emoji);

    assertThat(key.getBytes(UTF_8).length).isLessThanOrEqualTo(256);
    assertThat(key).startsWith("recurring:");
    // A truncation that split a surrogate pair would not survive a UTF-8
    // round trip — the orphaned half decodes back as U+FFFD.
    assertThat(new String(key.getBytes(UTF_8), UTF_8)).isEqualTo(key);
    assertThat(key).isEqualTo(CronTask.concurrencyKeyFor(emoji));
    assertThat(key).isNotEqualTo(CronTask.concurrencyKeyFor(emoji + "🚀"));

    String longAscii = "a".repeat(Names.MAX_LENGTH);
    assertThat(CronTask.concurrencyKeyFor(longAscii)).isEqualTo("recurring:" + longAscii);
  }

  private static CronTask exclusiveTask(String name, boolean exclusive) {
    return new CronTask(
        name,
        new CronTask.Trigger.Interval(Duration.ofMinutes(1)),
        "com.example.Handler",
        new JobArgument("com.example.Payload", "{}"),
        "default",
        0,
        null,
        null,
        exclusive,
        CronTask.MissedRunPolicy.DROP,
        ZoneId.of("UTC"),
        true);
  }

  @Test
  void subSecondTimeoutIsRejectedLoudly() {
    // The materialized instance carries the timeout as whole seconds; a
    // sub-second value would silently truncate to "use the global timeout".
    assertThatThrownBy(() -> task(Duration.ofMillis(500), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at least one second");
    assertThatThrownBy(() -> task(Duration.ZERO, null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> task(Duration.ofSeconds(-5), null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void nonPositiveMaxAttemptsIsRejectedLoudly() {
    assertThatThrownBy(() -> task(null, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at least one");
    assertThatThrownBy(() -> task(null, -3)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void nullOverridesMeanTheEngineDefaultsAndTheConvenienceConstructorDefaultsToThem() {
    assertThat(task(null, null).timeout()).isNull();
    assertThat(task(null, null).maxAttempts()).isNull();
    assertThat(task(Duration.ofSeconds(1), 1).timeout()).isEqualTo(Duration.ofSeconds(1));
    assertThat(task(Duration.ofSeconds(1), 1).maxAttempts()).isEqualTo(1);
    var convenience = new CronTask(
        "t",
        new CronTask.Trigger.Interval(Duration.ofMinutes(1)),
        "com.example.Handler",
        new JobArgument("com.example.Payload", "{}"),
        "default",
        0,
        CronTask.MissedRunPolicy.DROP,
        ZoneId.of("UTC"),
        true);
    assertThat(convenience.timeout()).isNull();
    assertThat(convenience.maxAttempts()).isNull();
  }

  private static CronTask task(Duration timeout, Integer maxAttempts) {
    return new CronTask(
        "t",
        new CronTask.Trigger.Interval(Duration.ofMinutes(1)),
        "com.example.Handler",
        new JobArgument("com.example.Payload", "{}"),
        "default",
        0,
        timeout,
        maxAttempts,
        CronTask.MissedRunPolicy.DROP,
        ZoneId.of("UTC"),
        true);
  }
}
