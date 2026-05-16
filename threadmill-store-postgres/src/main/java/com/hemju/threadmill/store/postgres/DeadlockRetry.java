package com.hemju.threadmill.store.postgres;

import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Bounded retry helper for operations against hot, multi-index rows.
 *
 * <p>Deadlocks on a busy queue table are <strong>normal</strong>, not
 * exceptional. PostgreSQL surfaces them with SQLSTATE {@code 40P01}; on
 * serializable isolation we also see {@code 40001}. Both are transient and
 * the right response is a short, jittered sleep followed by another attempt.
 *
 * <p>Non-deadlock exceptions are not retried — they are rethrown unchanged.
 */
public final class DeadlockRetry {

    /** PostgreSQL's deadlock_detected. */
    public static final String SQLSTATE_DEADLOCK = "40P01";

    /** PostgreSQL's serialization_failure. */
    public static final String SQLSTATE_SERIALIZATION_FAILURE = "40001";

    private DeadlockRetry() {}

    @FunctionalInterface
    public interface SqlAction<T> {
        T run() throws SQLException;
    }

    public static <T> T run(SqlAction<T> action) throws SQLException {
        return run(action, 5, 5, 50);
    }

    /**
     * Run {@code action}, retrying on {@code 40P01} / {@code 40001} up to
     * {@code maxAttempts} times with an exponentially-backing-off, jittered
     * sleep between {@code minBackoffMs} and {@code maxBackoffMs} milliseconds.
     */
    public static <T> T run(SqlAction<T> action, int maxAttempts, long minBackoffMs, long maxBackoffMs)
            throws SQLException {
        SQLException lastError = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return action.run();
            } catch (SQLException e) {
                if (!isRetryable(e) || attempt == maxAttempts) {
                    throw e;
                }
                lastError = e;
                sleepBackoff(attempt, minBackoffMs, maxBackoffMs);
            }
        }
        throw lastError == null ? new SQLException("DeadlockRetry exhausted") : lastError;
    }

    public static boolean isRetryable(SQLException e) {
        SQLException cur = e;
        while (cur != null) {
            String state = cur.getSQLState();
            if (SQLSTATE_DEADLOCK.equals(state) || SQLSTATE_SERIALIZATION_FAILURE.equals(state)) {
                return true;
            }
            cur = cur.getNextException();
        }
        // Some drivers surface the deadlock through getCause() instead.
        Throwable cause = e.getCause();
        while (cause != null) {
            if (cause instanceof SQLException sqlCause) {
                String state = sqlCause.getSQLState();
                if (SQLSTATE_DEADLOCK.equals(state) || SQLSTATE_SERIALIZATION_FAILURE.equals(state)) {
                    return true;
                }
            }
            cause = cause.getCause();
        }
        return false;
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
