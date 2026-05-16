package com.hemju.threadmill.core.store;

import java.time.Instant;
import java.util.Objects;

import com.hemju.threadmill.core.NodeId;

/**
 * Last observed heartbeat for a registered processing node.
 */
public record NodeHeartbeat(NodeId nodeId, Instant lastHeartbeatAt) {

    public NodeHeartbeat {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(lastHeartbeatAt, "lastHeartbeatAt");
    }
}
