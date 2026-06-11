package com.hemju.threadmill.core.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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
    void serializesAsTheBareSourceExpression() throws Exception {
        // Dashboard JSON renders CronExpression through the host's
        // ObjectMapper; a bean with no visible properties breaks /overview
        // and /recurring (500 under FAIL_ON_EMPTY_BEANS, {} under Jackson 3).
        var mapper = new ObjectMapper();
        assertThat(mapper.writeValueAsString(CronExpression.parse("*/5 * * * *")))
                .isEqualTo("\"*/5 * * * *\"");
    }

    @Test
    void rejectsWrongFieldCount() {
        assertThatThrownBy(() -> CronExpression.parse("* * * *")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @Timeout(5)
    void parseRejectsZeroAndNegativeSteps() {
        assertThatThrownBy(() -> CronExpression.parse("*/0 * * * *"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("step");
        assertThatThrownBy(() -> CronExpression.parse("*/-1 * * * *")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CronExpression.parse("* * * * */0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("step");
    }

    @Test
    @Timeout(5)
    void sundayAsSevenWorksInRangesListsAndSteps() {
        // Friday June 5th 2026; Sunday June 7th, Saturday June 6th.
        Instant fri = LocalDateTime.of(2026, 6, 5, 12, 0, 0).toInstant(ZoneOffset.UTC);

        // 5-7 = Fri-Sun: next fire is Saturday 09:00.
        Instant nextFriToSun = CronExpression.parse("0 9 * * 5-7").nextAfter(fri, UTC);
        assertThat(nextFriToSun).isEqualTo(LocalDateTime.of(2026, 6, 6, 9, 0).toInstant(ZoneOffset.UTC));

        // 1-7 = every day.
        Instant nextEveryDay = CronExpression.parse("0 9 * * 1-7").nextAfter(fri, UTC);
        assertThat(nextEveryDay).isEqualTo(LocalDateTime.of(2026, 6, 6, 9, 0).toInstant(ZoneOffset.UTC));

        // */7 over 0..7 = Sunday only (and must not hang).
        Instant nextStepSeven = CronExpression.parse("0 9 * * */7").nextAfter(fri, UTC);
        assertThat(nextStepSeven).isEqualTo(LocalDateTime.of(2026, 6, 7, 9, 0).toInstant(ZoneOffset.UTC));

        // 0,7 = Sunday, both spellings in one list.
        Instant nextList = CronExpression.parse("0 9 * * 0,7").nextAfter(fri, UTC);
        assertThat(nextList).isEqualTo(LocalDateTime.of(2026, 6, 7, 9, 0).toInstant(ZoneOffset.UTC));
    }

    @Test
    void rejectsOutOfRange() {
        assertThatThrownBy(() -> CronExpression.parse("60 * * * *")).isInstanceOf(IllegalArgumentException.class);
    }
}
