package com.hemju.threadmill.core.handler;

/**
 * SPI for resolving the {@link JobHandler} instance that should run a job.
 *
 * <p>Two implementations are expected to be common: a host-DI-backed
 * resolver (Spring) that looks up beans, and a reflective
 * resolver that constructs the handler from its no-arg constructor for
 * environments without a DI container.
 *
 * <p>Resolution failures are deterministic: an unresolvable handler causes
 * the engine to move the job into {@code QUARANTINED} rather than failing
 * the dispatcher loop.
 */
public interface JobHandlerResolver {

    /**
     * Resolve a handler instance for a fully-qualified type name.
     *
     * @param handlerTypeName fully-qualified handler type name as carried in
     *                        the {@code JobSpec}
     * @return a usable handler instance
     * @throws HandlerResolutionException if the type cannot be located,
     *         instantiated, or assigned to {@link JobHandler}
     */
    JobHandler<?> resolve(String handlerTypeName) throws HandlerResolutionException;

    /** Thrown when a handler cannot be resolved. */
    class HandlerResolutionException extends Exception {
        public HandlerResolutionException(String message) {
            super(message);
        }

        public HandlerResolutionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
