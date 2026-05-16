package com.hemju.threadmill.core.engine;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * In-process notification channel for "a new claimable job just landed on
 * queue X". When a producer and {@link ProcessingNode} share the same bus,
 * this closes the same-JVM latency gap between a producer's
 * {@code store.insert(...)} and the dispatcher's next poll: without a shared
 * bus, a same-process producer/consumer pair is always bounded below by
 * {@code pollInterval}; with one, dispatch is sub-millisecond.
 *
 * <p>This is strictly a same-JVM optimisation. Cross-node enqueues are not
 * notified — the receiving node's dispatcher still picks the job up on its
 * next poll, which is why {@code pollInterval} remains a load-bearing
 * fallback for correctness.
 *
 * <p>Sinks are registered by {@link ProcessingNode} at construction time and
 * called by the scheduling beans after each ENQUEUED-state insert. A bus
 * with no sinks (submitter-only nodes, tests) is a no-op.
 */
public final class LocalWakeBus {

    private final List<Consumer<String>> sinks = new CopyOnWriteArrayList<>();

    /**
     * Register a sink that will be notified of every wake. Typical caller:
     * {@code wakeBus.register(node::wake)} during {@code ProcessingNode}
     * wiring.
     *
     * @return a handle that unregisters the sink when run
     */
    public Runnable register(Consumer<String> sink) {
        Consumer<String> registered = Objects.requireNonNull(sink, "sink");
        sinks.add(registered);
        return () -> sinks.remove(registered);
    }

    /**
     * Notify all registered sinks that a job was inserted into {@code queue}.
     * Exceptions thrown by individual sinks are swallowed: the wake is an
     * opportunistic latency hint, never a correctness path.
     */
    public void wake(String queue) {
        Objects.requireNonNull(queue, "queue");
        for (Consumer<String> sink : sinks) {
            try {
                sink.accept(queue);
            } catch (Throwable ignored) {
                // Wake is opportunistic; failures fall back to the next poll.
            }
        }
    }
}
