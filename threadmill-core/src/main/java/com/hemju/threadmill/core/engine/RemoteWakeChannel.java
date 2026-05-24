package com.hemju.threadmill.core.engine;

import java.util.function.Consumer;

/**
 * Optional cross-node wake channel for "queue X may have claimable work".
 *
 * <p>Remote wake is a latency optimization, never a correctness mechanism.
 * Implementations must swallow transient publish/listen failures and rely on
 * the dispatcher's normal poll interval as the fallback.
 */
public interface RemoteWakeChannel extends AutoCloseable {

    /** Publish a best-effort wake hint for {@code queue}. */
    void publish(String queue);

    /**
     * Start listening for remote wake hints and call {@code wakeSink} with the
     * queue name from each valid message.
     */
    void start(Consumer<String> wakeSink);

    /** Stop listening and release any owned resources. */
    @Override
    void close();
}
