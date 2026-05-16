package com.hemju.threadmill.core.engine;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Weight resolver for a queue-family lane.
 *
 * <p>A weight of zero pauses a matching queue. Negative weights are rejected.
 */
public final class QueueWeights {

    private final Function<String, Integer> resolver;

    private QueueWeights(Function<String, Integer> resolver) {
        this.resolver = Objects.requireNonNull(resolver, "resolver");
    }

    /** Equal weight for every matching queue. */
    public static QueueWeights uniform() {
        return new QueueWeights(queue -> 1);
    }

    /** Static per-queue weights; unlisted queues default to weight 1. */
    public static QueueWeights fromMap(Map<String, Integer> weights) {
        Objects.requireNonNull(weights, "weights");
        var copy = Map.copyOf(weights);
        copy.forEach((queue, weight) -> requireNonNegative(queue, weight));
        return new QueueWeights(queue -> copy.getOrDefault(queue, 1));
    }

    /** Dynamic per-queue weights, resolved once per discovery cadence. */
    public static QueueWeights from(Function<String, Integer> resolver) {
        return new QueueWeights(resolver);
    }

    public int weightFor(String queue) {
        return requireNonNegative(queue, resolver.apply(queue));
    }

    private static int requireNonNegative(String queue, Integer weight) {
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(weight, "weight");
        if (weight < 0) {
            throw new IllegalArgumentException("queue weight must not be negative for " + queue);
        }
        return weight;
    }
}
