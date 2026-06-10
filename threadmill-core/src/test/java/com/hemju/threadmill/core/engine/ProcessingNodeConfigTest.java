package com.hemju.threadmill.core.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class ProcessingNodeConfigTest {

    @Test
    void defaultsSeparateMaintenanceFromHeartbeatAndRetention() {
        var config = ProcessingNodeConfig.defaults();
        // Three distinct cadences, each at the value that matches its concern.
        assertThat(config.maintenancePollInterval()).isEqualTo(Duration.ofSeconds(1));
        assertThat(config.claimHeartbeat()).isEqualTo(Duration.ofSeconds(15));
        assertThat(config.retentionInterval()).isEqualTo(Duration.ofHours(1));
    }

    @Test
    void builderRoundTripsTheNewKnobs() {
        var config = ProcessingNodeConfig.builder()
                .maintenancePollInterval(Duration.ofMillis(250))
                .retentionInterval(Duration.ofMinutes(30))
                .build();
        assertThat(config.maintenancePollInterval()).isEqualTo(Duration.ofMillis(250));
        assertThat(config.retentionInterval()).isEqualTo(Duration.ofMinutes(30));
    }

    @Test
    void terminalStateRetentionAgesAreConfigurableAndValidated() {
        var config = ProcessingNodeConfig.builder()
                .succeededRetention(Duration.ofDays(3))
                .failedRetention(Duration.ofDays(14))
                .deletedRetention(Duration.ofDays(2))
                .quarantinedRetention(Duration.ofDays(60))
                .build();
        assertThat(config.succeededRetention()).isEqualTo(Duration.ofDays(3));
        assertThat(config.failedRetention()).isEqualTo(Duration.ofDays(14));
        assertThat(config.deletedRetention()).isEqualTo(Duration.ofDays(2));
        assertThat(config.quarantinedRetention()).isEqualTo(Duration.ofDays(60));

        assertThatThrownBy(() -> ProcessingNodeConfig.builder()
                        .succeededRetention(Duration.ZERO)
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("succeededRetention");
        assertThatThrownBy(() -> ProcessingNodeConfig.builder()
                        .failedRetention(Duration.ofDays(-1))
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("failedRetention");
    }

    @Test
    void zeroOrNegativeMaintenancePollIntervalIsRejected() {
        assertThatThrownBy(() -> ProcessingNodeConfig.builder()
                        .maintenancePollInterval(Duration.ZERO)
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maintenancePollInterval");
    }

    @Test
    void zeroOrNegativeRetentionIntervalIsRejected() {
        assertThatThrownBy(() -> ProcessingNodeConfig.builder()
                        .retentionInterval(Duration.ofSeconds(-1))
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retentionInterval");
    }
}
