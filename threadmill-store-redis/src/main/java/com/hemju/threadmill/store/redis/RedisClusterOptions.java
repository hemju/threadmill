package com.hemju.threadmill.store.redis;

import java.time.Duration;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;

/**
 * Shared Lettuce client options for Threadmill's internally-created Redis
 * clients (standalone, sentinel, and cluster).
 *
 * <p><strong>Command timeout.</strong> Lettuce defaults to a 60s command
 * timeout and buffers commands while disconnected, so an outage made each store
 * call block ~60s; the dispatcher's breaker needs several consecutive failures,
 * so detection took on the order of ten minutes — orders of magnitude slower
 * than the rest of the engine's tuning. A bounded {@link TimeoutOptions} caps
 * each command so failures surface in seconds. It also keeps a single command
 * from outliving the 30s per-key claim lock (a command blocking past the lock
 * TTL could let another node race the workflow-hold bookkeeping the lock
 * protects), since the timeout is well under that TTL.
 *
 * <p><strong>Cluster topology refresh.</strong> Lettuce disables periodic and
 * adaptive cluster topology refresh by default; without it a master failover
 * leaves the client pinned to the dead node's stale partition map. Every engine
 * key shares the {@code {threadmill}} hash tag, so that stall is total.
 */
final class RedisClusterOptions {

    static final Duration COMMAND_TIMEOUT = Duration.ofSeconds(10);

    private RedisClusterOptions() {}

    static TimeoutOptions timeoutOptions() {
        return TimeoutOptions.builder()
                .fixedTimeout(COMMAND_TIMEOUT)
                .timeoutCommands(true)
                .build();
    }

    /** Options for an internally-created standalone / sentinel client. */
    static ClientOptions standaloneOptions() {
        return ClientOptions.builder().timeoutOptions(timeoutOptions()).build();
    }

    /** Options for an internally-created cluster client: timeout + topology refresh. */
    static ClusterClientOptions topologyRefreshing() {
        var refresh = ClusterTopologyRefreshOptions.builder()
                .enablePeriodicRefresh(Duration.ofSeconds(30))
                .enableAllAdaptiveRefreshTriggers()
                .adaptiveRefreshTriggersTimeout(Duration.ofSeconds(30))
                .build();
        return ClusterClientOptions.builder()
                .topologyRefreshOptions(refresh)
                .timeoutOptions(timeoutOptions())
                .build();
    }
}
