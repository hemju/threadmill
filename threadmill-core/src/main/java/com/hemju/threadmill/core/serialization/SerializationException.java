package com.hemju.threadmill.core.serialization;

/**
 * Thrown when a serialization or deserialization fails for reasons other
 * than payload size. Size-related failures use
 * {@link com.hemju.threadmill.core.OversizedJobException}.
 */
public class SerializationException extends RuntimeException {

    public SerializationException(String message) {
        super(message);
    }

    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
