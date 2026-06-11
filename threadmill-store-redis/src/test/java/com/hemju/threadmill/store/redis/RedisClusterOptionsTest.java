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
