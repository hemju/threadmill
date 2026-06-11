package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;

/**
 * The four terminal-state retention ages are tunable through Spring properties
 * (they previously had no {@code ThreadmillProperties} fields and could only be
 * changed by replacing the whole {@code ProcessingNodeConfig} bean).
 */
class RetentionPropertiesBindingTest {

    @Test
    void retentionAgePropertiesBindToTheProcessingNodeConfig() {
        var props = new ThreadmillProperties();
        props.setSucceededRetention(Duration.ofDays(3));
        props.setFailedRetention(Duration.ofDays(14));
        props.setDeletedRetention(Duration.ofDays(2));
        props.setQuarantinedRetention(Duration.ofDays(21));

        var config = new ThreadmillAutoConfiguration().threadmillProcessingNodeConfig(props);

        assertThat(config.succeededRetention()).isEqualTo(Duration.ofDays(3));
        assertThat(config.failedRetention()).isEqualTo(Duration.ofDays(14));
        assertThat(config.deletedRetention()).isEqualTo(Duration.ofDays(2));
        assertThat(config.quarantinedRetention()).isEqualTo(Duration.ofDays(21));
    }

    @Test
    void retentionAgeDefaultsMatchTheEngineDefaults() {
        var config = new ThreadmillAutoConfiguration().threadmillProcessingNodeConfig(new ThreadmillProperties());
        assertThat(config.succeededRetention()).isEqualTo(Duration.ofDays(7));
        assertThat(config.failedRetention()).isEqualTo(Duration.ofDays(30));
        assertThat(config.deletedRetention()).isEqualTo(Duration.ofDays(7));
        assertThat(config.quarantinedRetention()).isEqualTo(Duration.ofDays(30));
    }
}
