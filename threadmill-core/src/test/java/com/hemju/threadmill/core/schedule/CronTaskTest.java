package com.hemju.threadmill.core.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.spec.JobArgument;

class CronTaskTest {

    @Test
    void subSecondTimeoutIsRejectedLoudly() {
        // The materialized instance carries the timeout as whole seconds; a
        // sub-second value would silently truncate to "use the global timeout".
        assertThatThrownBy(() -> task(Duration.ofMillis(500), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one second");
        assertThatThrownBy(() -> task(Duration.ZERO, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> task(Duration.ofSeconds(-5), null)).isInstanceOf(IllegalArgumentException.class);
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
