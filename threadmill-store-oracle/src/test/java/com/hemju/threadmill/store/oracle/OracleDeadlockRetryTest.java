package com.hemju.threadmill.store.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class OracleDeadlockRetryTest {

  private static SQLException ora(int code) {
    return new SQLException("ORA-" + code, "61000", code);
  }

  @Test
  void retriesDeadlocksAndSerializationFailuresUntilSuccess() throws SQLException {
    var attempts = new AtomicInteger();
    String result = OracleDeadlockRetry.run(
        () -> {
          int attempt = attempts.incrementAndGet();
          if (attempt == 1) throw ora(OracleDeadlockRetry.ORA_DEADLOCK);
          if (attempt == 2) throw ora(OracleDeadlockRetry.ORA_SERIALIZATION_FAILURE);
          return "done";
        },
        5,
        1,
        2);

    assertThat(result).isEqualTo("done");
    assertThat(attempts).hasValue(3);
  }

  @Test
  void rethrowsOtherFailuresImmediately() {
    var attempts = new AtomicInteger();
    var uniqueViolation = ora(OracleJdbc.UNIQUE_VIOLATION);

    assertThatThrownBy(() -> OracleDeadlockRetry.run(
            () -> {
              attempts.incrementAndGet();
              throw uniqueViolation;
            },
            5,
            1,
            2))
        .isSameAs(uniqueViolation);
    assertThat(attempts).hasValue(1);
  }

  @Test
  void givesUpAfterTheAttemptBudget() {
    var attempts = new AtomicInteger();

    assertThatThrownBy(() -> OracleDeadlockRetry.run(
            () -> {
              attempts.incrementAndGet();
              throw ora(OracleDeadlockRetry.ORA_DEADLOCK);
            },
            3,
            1,
            2))
        .isInstanceOf(SQLException.class);
    assertThat(attempts).hasValue(3);
  }

  @Test
  void recognisesErrorCodesThroughBatchAndCauseChains() {
    var batch = new BatchUpdateException("batch failed", new int[0]);
    batch.setNextException(ora(OracleDeadlockRetry.ORA_DEADLOCK));
    var wrapped = new SQLException("wrapper", ora(OracleJdbc.UNIQUE_VIOLATION));

    assertThat(OracleDeadlockRetry.isRetryable(batch)).isTrue();
    assertThat(OracleJdbc.hasErrorCode(wrapped, OracleJdbc.UNIQUE_VIOLATION)).isTrue();
    assertThat(OracleDeadlockRetry.isRetryable(wrapped)).isFalse();
  }
}
