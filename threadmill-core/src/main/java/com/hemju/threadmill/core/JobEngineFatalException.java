package com.hemju.threadmill.core;

/**
 * Thrown by engine internals when a condition occurs that the engine cannot
 * recover from on its own (corrupt persisted state, an irrecoverable I/O
 * fault, an invariant violation). The cluster pauses; the engine does not
 * crash the host process.
 */
public class JobEngineFatalException extends RuntimeException {

    public JobEngineFatalException(String message) {
        super(message);
    }

    public JobEngineFatalException(String message, Throwable cause) {
        super(message, cause);
    }
}
