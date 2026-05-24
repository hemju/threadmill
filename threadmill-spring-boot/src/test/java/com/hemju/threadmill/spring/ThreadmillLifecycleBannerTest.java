package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

class ThreadmillLifecycleBannerTest {

    private ProcessingNode node;

    @AfterEach
    void tearDown() {
        if (node != null) node.close();
    }

    @Test
    void startupBannerLogsStoreCapabilitiesAndLanes() {
        var store = new InMemoryJobStore();
        node = ProcessingNode.builder(store)
                .config(ProcessingNodeConfig.defaults())
                .lane("default", 4)
                .lane("system", 2)
                .build();

        String banner = ThreadmillLifecycle.renderBanner(node);

        assertThat(banner).contains("Threadmill engine started");
        assertThat(banner).contains(node.nodeId().toString());
        assertThat(banner).contains("In-Memory (volatile, single-JVM)");
        assertThat(banner).contains("capabilities: maxJob=");
        assertThat(banner).contains("6 across 2 lane(s)");
        assertThat(banner).contains("default x4");
        assertThat(banner).contains("system x2");
        assertThat(banner).contains("Polling      : poll=");
        assertThat(banner).contains("Maintenance  : poll=");
        assertThat(banner).contains("Master lease : ");
    }

    @Test
    void bannerWorksWithDefaultSingleLaneNode() {
        var store = new InMemoryJobStore();
        node = ProcessingNode.builder(store).build();

        String banner = ThreadmillLifecycle.renderBanner(node);

        assertThat(banner).contains("Threadmill engine started");
        assertThat(banner).contains("In-Memory");
        assertThat(banner).contains("1 lane(s)");
    }
}
