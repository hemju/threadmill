package com.hemju.threadmill.core;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import com.hemju.threadmill.core.spec.JobSpec;

/**
 * Immutable, point-in-time view of a {@link Job}.
 *
 * <p>This is what the engine serializes into the {@code JobStore}. Because
 * the snapshot is immutable, concurrent mutation of the source {@code Job}
 * during serialization cannot produce a torn write — by construction.
 *
 * <p>Storage backends should treat the snapshot as opaque payload plus a
 * fixed set of indexable fields ({@code currentState}, {@code queue},
 * {@code scheduledFor}, owner, etc.).
 */
public record JobSnapshot(
        JobId id,
        JobSpec spec,
        String queue,
        int priority,
        Instant createdAt,
        String cronTaskName,
        JobRelationship relationship,
        JobId workflowRootId,
        String concurrencyKey,
        ConcurrencyMode concurrencyMode,
        List<JobStateEntry> stateHistory,
        Map<String, String> metadata,
        List<JobLog.Entry> log,
        JobProgress.Snapshot progress,
        long version,
        NodeId ownerNodeId,
        Instant ownerHeartbeatAt,
        Instant lastCheckinAt,
        Instant scheduledFor,
        JobResult result,
        int attempts) {

    public JobState currentState() {
        return stateHistory.get(stateHistory.size() - 1).state();
    }
}
