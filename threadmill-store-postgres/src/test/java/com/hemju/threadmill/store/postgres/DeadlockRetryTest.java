package com.hemju.threadmill.store.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class DeadlockRetryTest {

    @Test
    void hasSqlStateWalksNextExceptionAndCauseChains() {
        var batch = new BatchUpdateException("batch failed", null, 0, new int[0], (Throwable) null);
        batch.setNextException(new SQLException("duplicate key", "23505"));
        assertThat(DeadlockRetry.hasSqlState(batch, "23505")).isTrue();

        var wrapped = new SQLException("outer", new SQLException("inner", "23505"));
        assertThat(DeadlockRetry.hasSqlState(wrapped, "23505")).isTrue();

        assertThat(DeadlockRetry.hasSqlState(new SQLException("x", "08000"), "23505"))
                .isFalse();
    }

    @Test
    void recognisesDeadlockBySqlState() {
        assertThat(DeadlockRetry.isRetryable(new SQLException("d", "40P01"))).isTrue();
        assertThat(DeadlockRetry.isRetryable(new SQLException("s", "40001"))).isTrue();
        assertThat(DeadlockRetry.isRetryable(new SQLException("o", "23505"))).isFalse();
    }

    @Test
    void recognisesDeadlockNestedInChainedException() {
        var outer = new SQLException("outer", "23000");
        outer.setNextException(new SQLException("inner deadlock", "40P01"));
        assertThat(DeadlockRetry.isRetryable(outer)).isTrue();
    }

    @Test
    void recognisesDeadlockNestedInCause() {
        var cause = new SQLException("deadlock", "40P01");
        var wrapper = new SQLException("wrapped", "XX000", cause);
        assertThat(DeadlockRetry.isRetryable(wrapper)).isTrue();
    }

    @Test
    void retriesUntilSuccessOnDeadlock() throws SQLException {
        var calls = new AtomicInteger();
        int result = DeadlockRetry.run(
                () -> {
                    if (calls.incrementAndGet() < 3) {
                        throw new SQLException("flaky", "40P01");
                    }
                    return 42;
                },
                5,
                1,
                2);
        assertThat(result).isEqualTo(42);
        assertThat(calls.get()).isEqualTo(3);
    }

    @Test
    void rethrowsNonRetryable() {
        var calls = new AtomicInteger();
        assertThatThrownBy(() -> DeadlockRetry.run(
                        () -> {
                            calls.incrementAndGet();
                            throw new SQLException("uniq", "23505");
                        },
                        5,
                        1,
                        2))
                .isInstanceOf(SQLException.class)
                .hasFieldOrPropertyWithValue("SQLState", "23505");
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void givesUpAfterMaxAttempts() {
        assertThatThrownBy(() -> DeadlockRetry.run(
                        () -> {
                            throw new SQLException("perma", "40P01");
                        },
                        3,
                        1,
                        2))
                .isInstanceOf(SQLException.class)
                .hasFieldOrPropertyWithValue("SQLState", "40P01");
    }
}
