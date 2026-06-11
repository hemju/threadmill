package com.hemju.threadmill.store.redis;

import java.time.Duration;

import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;

/**
 * Shared Lettuce {@link ClusterClientOptions} for Threadmill's cluster clients.
 *
 * <p>Lettuce disables both periodic and adaptive cluster topology refresh by
 * default. Without a refresh, a master failover leaves the client pinned to the
 * dead node's stale partition map — it reconnects forever to an endpoint that is
 * down and never learns about the promoted replica, stalling the whole engine
 * until the original node returns or the JVM restarts. Every Threadmill engine
 * key shares the {@code {threadmill}} hash tag, so a single master owns all
 * engine state and that stall is total.
 *
 * <p>Enabling periodic refresh plus all adaptive triggers (MOVED/ASK redirects,
 * reconnect attempts, persistent disconnects, uncovered slots) lets the client
 * re-resolve the topology and route to the promoted master within seconds.
 */
final class RedisClusterOptions {

    private RedisClusterOptions() {}

    static ClusterClientOptions topologyRefreshing() {
        var refresh = ClusterTopologyRefreshOptions.builder()
                .enablePeriodicRefresh(Duration.ofSeconds(30))
                .enableAllAdaptiveRefreshTriggers()
                .adaptiveRefreshTriggersTimeout(Duration.ofSeconds(30))
                .build();
        return ClusterClientOptions.builder().topologyRefreshOptions(refresh).build();
    }
}
