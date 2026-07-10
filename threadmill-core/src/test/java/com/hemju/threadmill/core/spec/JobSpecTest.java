package com.hemju.threadmill.core.spec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

class JobSpecTest {

    @Test
    void legacyConstructorLeavesDedupUnset() {
        var spec = new JobSpec("com.example.Handler", List.of());

        assertThat(spec.dedupKeyValue()).isEmpty();
        assertThat(spec.dedupTtlValue()).isEmpty();
    }

    @Test
    void dedupRequiresKeyAndPositiveTtl() {
        assertThatThrownBy(() -> new JobSpec("com.example.Handler", List.of(), " ", Duration.ofMinutes(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dedupKey");

        assertThatThrownBy(() -> new JobSpec("com.example.Handler", List.of(), "key", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dedupTtl");

        assertThatThrownBy(() -> new JobSpec("com.example.Handler", List.of(), "key", Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dedupTtl");
    }

    @Test
    void dedupKeyIsLimitedByUtf8Bytes() {
        var oversized = "x".repeat(JobSpec.MAX_DEDUP_KEY_BYTES + 1);

        assertThatThrownBy(() -> new JobSpec("com.example.Handler", List.of(), oversized, Duration.ofMinutes(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dedupKey");
    }

    @Test
    void handlerTypeIsBoundedByUtf8BytesForEveryStore() {
        var oversized = "x".repeat(JobSpec.MAX_HANDLER_TYPE_BYTES + 1);

        assertThatThrownBy(() -> new JobSpec(oversized, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("handlerType");
        assertThatThrownBy(() -> new JobSpec("ü".repeat(JobSpec.MAX_HANDLER_TYPE_BYTES), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UTF-8");
    }
}
