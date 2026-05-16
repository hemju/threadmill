package com.hemju.threadmill.core.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;

class CronExpressionTest {

    private static final ZoneId UTC = ZoneOffset.UTC;

    @Test
    void everyMinuteWildcardFiresOnTheNextMinute() {
        Instant t = LocalDateTime.of(2026, 6, 1, 10, 30, 15).toInstant(ZoneOffset.UTC);
        Instant next = CronExpression.parse("* * * * *").nextAfter(t, UTC);
        assertThat(next).isEqualTo(LocalDateTime.of(2026, 6, 1, 10, 31, 0).toInstant(ZoneOffset.UTC));
    }

    @Test
    void specificMinute() {
        Instant t = LocalDateTime.of(2026, 6, 1, 10, 30, 0).toInstant(ZoneOffset.UTC);
        Instant next = CronExpression.parse("45 * * * *").nextAfter(t, UTC);
        assertThat(next).isEqualTo(LocalDateTime.of(2026, 6, 1, 10, 45, 0).toInstant(ZoneOffset.UTC));
    }

    @Test
    void stepEveryFiveMinutes() {
        Instant t = LocalDateTime.of(2026, 6, 1, 10, 33, 0).toInstant(ZoneOffset.UTC);
        Instant next = CronExpression.parse("*/5 * * * *").nextAfter(t, UTC);
        assertThat(next).isEqualTo(LocalDateTime.of(2026, 6, 1, 10, 35, 0).toInstant(ZoneOffset.UTC));
    }

    @Test
    void rangeAndList() {
        Instant t = LocalDateTime.of(2026, 6, 1, 8, 59, 0).toInstant(ZoneOffset.UTC);
        Instant next = CronExpression.parse("0,30 9-17 * * *").nextAfter(t, UTC);
        assertThat(next).isEqualTo(LocalDateTime.of(2026, 6, 1, 9, 0, 0).toInstant(ZoneOffset.UTC));
    }

    @Test
    void dayOfWeekSundayAcceptedBothAs0And7() {
        // Sunday June 7th 2026.
        Instant fri = LocalDateTime.of(2026, 6, 5, 12, 0, 0).toInstant(ZoneOffset.UTC);
        Instant nextWith0 = CronExpression.parse("0 9 * * 0").nextAfter(fri, UTC);
        Instant nextWith7 = CronExpression.parse("0 9 * * 7").nextAfter(fri, UTC);
        assertThat(nextWith0).isEqualTo(LocalDateTime.of(2026, 6, 7, 9, 0).toInstant(ZoneOffset.UTC));
        assertThat(nextWith7).isEqualTo(LocalDateTime.of(2026, 6, 7, 9, 0).toInstant(ZoneOffset.UTC));
    }

    @Test
    void rejectsWrongFieldCount() {
        assertThatThrownBy(() -> CronExpression.parse("* * * *")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsOutOfRange() {
        assertThatThrownBy(() -> CronExpression.parse("60 * * * *")).isInstanceOf(IllegalArgumentException.class);
    }
}
