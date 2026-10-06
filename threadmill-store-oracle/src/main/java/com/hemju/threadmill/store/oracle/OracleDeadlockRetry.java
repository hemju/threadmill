package com.hemju.threadmill.store.oracle;

import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Bounded retry for transient lock conflicts on hot, multi-index rows.
 *
 * <p>Deadlocks on a busy queue table are <strong>normal</strong>, not
 * exceptional. Oracle reports them as {@code ORA-00060}; serializable
 * transactions additionally see {@code ORA-08177}. Oracle rolls back only the
 * statement that detected the deadlock, but a self-owned store transaction is
 * rolled back as a whole and retried after a short, jittered sleep.
 *
 * <p>Any other exception is rethrown unchanged.
 */
public final class OracleDeadlockRetry {

  /** ORA-00060: deadlock detected while waiting for resource. */
  public static final int ORA_DEADLOCK = 60;

  /** ORA-08177: can't serialize access for this transaction. */
  public static final int ORA_SERIALIZATION_FAILURE = 8177;

  private OracleDeadlockRetry() {}

  /**
   * An action that may fail with a transient conflict.
   *
   * @param <T> result type
   */
  @FunctionalInterface
  public interface SqlAction<T> {
    /**
     * Run the action once.
     *
     * @return the result
     * @throws SQLException when the action fails
     */
    T run() throws SQLException;
  }

  /**
   * Run {@code action} with the default retry budget (5 attempts, 5–50 ms backoff).
   *
   * @param action the action to run
   * @param <T> result type
   * @return the action result
   * @throws SQLException the last failure when it is not retryable or retries are exhausted
   */
  public static <T> T run(SqlAction<T> action) throws SQLException {
    return run(action, 5, 5, 50);
  }

  /**
   * Run {@code action}, retrying on {@code ORA-00060} / {@code ORA-08177} up to
   * {@code maxAttempts} times with an exponentially backing-off, jittered
   * sleep between {@code minBackoffMs} and {@code maxBackoffMs} milliseconds.
   *
   * @param action the action to run
   * @param maxAttempts maximum number of attempts, at least one
   * @param minBackoffMs initial backoff in milliseconds
   * @param maxBackoffMs backoff cap in milliseconds
   * @param <T> result type
   * @return the action result
   * @throws SQLException the last failure when it is not retryable or retries are exhausted
   */
  public static <T> T run(
      SqlAction<T> action, int maxAttempts, long minBackoffMs, long maxBackoffMs)
      throws SQLException {
    for (int attempt = 1; ; attempt++) {
      try {
        return action.run();
      } catch (SQLException e) {
        if (!isRetryable(e) || attempt >= maxAttempts) {
          throw e;
        }
        sleepBackoff(attempt, minBackoffMs, maxBackoffMs);
      }
    }
  }

  /**
   * Whether {@code e} reports a transient lock conflict anywhere in its chains.
   *
   * @param e the failure
   * @return {@code true} for {@code ORA-00060} and {@code ORA-08177}
   */
  public static boolean isRetryable(SQLException e) {
    return OracleJdbc.hasErrorCode(e, ORA_DEADLOCK)
        || OracleJdbc.hasErrorCode(e, ORA_SERIALIZATION_FAILURE);
  }

  private static void sleepBackoff(int attempt, long minMs, long maxMs) {
    long base = Math.min(maxMs, minMs << Math.min(attempt - 1, 6));
    long jitter = ThreadLocalRandom.current().nextLong(base + 1);
    try {
      Thread.sleep(base + jitter);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while retrying a transient SQL conflict", ie);
    }
  }
}
