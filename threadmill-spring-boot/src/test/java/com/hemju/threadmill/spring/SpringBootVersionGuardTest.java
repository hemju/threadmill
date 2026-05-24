package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SpringBootVersionGuardTest {

    @Test
    void failsFastOnSpringBootThree() {
        assertThatThrownBy(() -> SpringBootVersionGuard.requireSpringBootFour("3.5.0"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("3.5.0")
                .hasMessageContaining("Spring Boot 4");
    }

    @Test
    void failsFastOnSpringBootTwo() {
        assertThatThrownBy(() -> SpringBootVersionGuard.requireSpringBootFour("2.7.18"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("2.7.18")
                .hasMessageContaining("Spring Boot 4");
    }

    @Test
    void failsFastOnUnparseableVersion() {
        assertThatThrownBy(() -> SpringBootVersionGuard.requireSpringBootFour("not-a-version"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("could not be identified")
                .hasMessageContaining("Spring Boot 4");
    }

    @Test
    void failsFastOnNullVersion() {
        assertThatThrownBy(() -> SpringBootVersionGuard.requireSpringBootFour(null))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void acceptsSpringBootFourReleases() {
        assertThatCode(() -> SpringBootVersionGuard.requireSpringBootFour("4.0.0"))
                .doesNotThrowAnyException();
        assertThatCode(() -> SpringBootVersionGuard.requireSpringBootFour("4.0.4"))
                .doesNotThrowAnyException();
        assertThatCode(() -> SpringBootVersionGuard.requireSpringBootFour("4.1.2"))
                .doesNotThrowAnyException();
    }

    @Test
    void acceptsFutureMajors() {
        assertThatCode(() -> SpringBootVersionGuard.requireSpringBootFour("5.0.0"))
                .doesNotThrowAnyException();
    }
}
