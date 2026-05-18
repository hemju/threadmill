package com.hemju.threadmill.core;

/**
 * Thrown when the backing store rejects a write because its configured storage
 * or memory budget has been exhausted.
 */
public final class StoreCapacityExceededException extends RuntimeException {

    public StoreCapacityExceededException(String message, Throwable cause) {
        super(message, cause);
    }
}
