package com.hemju.threadmill.store.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import io.lettuce.core.cluster.ClusterTopologyRefreshOptions.RefreshTrigger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Guards the cluster topology-refresh configuration so it cannot silently
 * regress back to Lettuce's defaults (refresh disabled), which would make a
 * master failover stall the engine until a restart.
 */
class RedisClusterOptionsTest {

    @Test
    @DisplayName("standalone and cluster options apply a bounded command timeout")
    void optionsApplyABoundedCommandTimeout() {
        assertThat(RedisClusterOptions.standaloneOptions().getTimeoutOptions().isTimeoutCommands())
                .isTrue();
        assertThat(RedisClusterOptions.topologyRefreshing().getTimeoutOptions().isTimeoutCommands())
                .isTrue();
        // Commands must not be able to outlive the 30s per-key claim lock.
        assertThat(RedisClusterOptions.COMMAND_TIMEOUT).isLessThanOrEqualTo(Duration.ofSeconds(30));
    }

    @Test
    @DisplayName("cluster options enable periodic refresh and all adaptive triggers")
    void clusterOptionsEnablePeriodicAndAdaptiveRefresh() {
        var options = RedisClusterOptions.topologyRefreshing();
        var refresh = options.getTopologyRefreshOptions();

        assertThat(refresh.isPeriodicRefreshEnabled()).isTrue();
        assertThat(refresh.getRefreshPeriod()).isEqualTo(Duration.ofSeconds(30));
        assertThat(refresh.getAdaptiveRefreshTriggers())
                .contains(
                        RefreshTrigger.MOVED_REDIRECT,
                        RefreshTrigger.ASK_REDIRECT,
                        RefreshTrigger.PERSISTENT_RECONNECTS,
                        RefreshTrigger.UNCOVERED_SLOT,
                        RefreshTrigger.UNKNOWN_NODE);
    }
}
