package com.hemju.threadmill.core.engine;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CircuitBreakerTest {

    @Test
    void countsConsecutiveFailures() {
        var cb = new CircuitBreaker(3);
        assertThat(cb.recordFailure()).isFalse();
        assertThat(cb.recordFailure()).isFalse();
        assertThat(cb.recordFailure()).isTrue();
    }

    @Test
    void successDecaysTheCounter() {
        var cb = new CircuitBreaker(3);
        cb.recordFailure();
        cb.recordFailure();
        cb.recordSuccess();
        assertThat(cb.current()).isEqualTo(1);
        cb.recordSuccess();
        cb.recordSuccess(); // floor at 0
        assertThat(cb.current()).isZero();
        // Three more failures needed to trip again.
        assertThat(cb.recordFailure()).isFalse();
        assertThat(cb.recordFailure()).isFalse();
        assertThat(cb.recordFailure()).isTrue();
    }
}
