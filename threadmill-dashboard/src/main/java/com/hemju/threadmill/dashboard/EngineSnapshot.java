package com.hemju.threadmill.dashboard;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.core.store.JobStoreCapabilities;
import com.hemju.threadmill.core.store.NodeHeartbeat;

/**
 * A point-in-time snapshot of the engine's observable state — the data the
 * dashboard renders and the data any external monitor can scrape directly.
 *
 * <p><strong>Observability is data-first.</strong> A UI is one consumer of
 * this. Metrics exporters and ad-hoc tools are equally valid consumers.
 *
 * <p>The snapshot is always-cheap: per-state counts come from the
 * incrementally-maintained counters that every store maintains, never a
 * scan of the full jobs table.
 */
public record EngineSnapshot(
        Instant takenAt,
        Map<JobState, Long> countsByState,
        Map<String, Long> queueDepths,
        Map<String, Instant> oldestEnqueuedAt,
        Instant oldestProcessingHeartbeat,
        List<NodeHeartbeat> nodeHeartbeats,
        List<CronTask> cronTasks,
        Set<String> pausedQueues,
        JobStoreCapabilities capabilities) {

    public EngineSnapshot {
        Objects.requireNonNull(takenAt, "takenAt");
        Objects.requireNonNull(countsByState, "countsByState");
        Objects.requireNonNull(queueDepths, "queueDepths");
        Objects.requireNonNull(oldestEnqueuedAt, "oldestEnqueuedAt");
        Objects.requireNonNull(nodeHeartbeats, "nodeHeartbeats");
        Objects.requireNonNull(cronTasks, "cronTasks");
        Objects.requireNonNull(pausedQueues, "pausedQueues");
        Objects.requireNonNull(capabilities, "capabilities");
    }

    /** Sample the store. Never holds long-running locks; never scans the jobs table. */
    public static EngineSnapshot of(JobStore store) {
        Objects.requireNonNull(store, "store");
        Map<String, Long> depths = store.queueDepths();
        Map<String, Instant> oldest = new LinkedHashMap<>();
        for (String queue : depths.keySet()) {
            store.oldestEnqueuedAt(queue).ifPresent(at -> oldest.put(queue, at));
        }
        return new EngineSnapshot(
                Instant.now(),
                store.countsByState(),
                depths,
                Map.copyOf(oldest),
                store.oldestProcessingHeartbeat().orElse(null),
                store.listNodeHeartbeats(),
                store.listCronTasks(),
                store.listPausedQueues(),
                store.capabilities());
    }

    /** Convenience: number of jobs currently in the given state. */
    public long count(JobState state) {
        return countsByState.getOrDefault(state, 0L);
    }
}
