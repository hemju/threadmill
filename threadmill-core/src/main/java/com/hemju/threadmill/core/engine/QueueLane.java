package com.hemju.threadmill.core.engine;

import java.util.Objects;

import com.hemju.threadmill.core.Names;

/**
 * A named queue with its own reserved worker capacity.
 *
 * <p>Per-queue lanes are the engine's defence against starvation: a flood
 * of jobs on one queue cannot occupy the capacity of another. Recurring
 * and other time-sensitive system jobs should run on an isolated lane
 * (the {@link com.hemju.threadmill.core.schedule.Scheduler#SYSTEM_QUEUE}
 * is reserved for this).
 *
 * @param queue     the queue name, or the queue-family pattern for a family lane
 * @param workers   the reserved worker count for this lane
 * @param family    queue-family metadata for a pattern lane, or {@code null} for a fixed queue
 */
public record QueueLane(String queue, int workers, QueueFamily family) {

    public QueueLane(String queue, int workers) {
        this(queue, workers, null);
    }

    public QueueLane {
        Names.requireName("queue", Objects.requireNonNull(queue, "queue"));
        if (workers <= 0) throw new IllegalArgumentException("workers must be positive");
        if (family != null && !queue.equals(family.pattern())) {
            throw new IllegalArgumentException("queue-family lane queue must equal its pattern");
        }
    }

    static QueueLane family(String pattern, int workers, QueueWeights weights) {
        return new QueueLane(pattern, workers, new QueueFamily(pattern, weights));
    }

    boolean isFamily() {
        return family != null;
    }
}
