package com.hemju.threadmill.core.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class QueueFamilyTest {

    @Test
    void patternMatchingIsAnchoredAndSupportsStarAndQuestion() {
        var family = new QueueFamily("project:*", QueueWeights.uniform());
        assertThat(family.matches("project:42")).isTrue();
        assertThat(family.matches("project:x")).isTrue();
        assertThat(family.matches("projectA:42")).isFalse();
        assertThat(family.matches("xproject:42")).isFalse();
        assertThat(family.matches("project:42:sub")).isFalse();

        var one = new QueueFamily("project:?", QueueWeights.uniform());
        assertThat(one.matches("project:x")).isTrue();
        assertThat(one.matches("project:42")).isFalse();
    }

    @Test
    void rejectsPatternFormsOutsideTheQueueFamilyGlobShape() {
        assertThatThrownBy(() -> new QueueFamily("project:**", QueueWeights.uniform()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new QueueFamily("project:[0-9]", QueueWeights.uniform()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new QueueFamily("project:%", QueueWeights.uniform()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void queueWeightsSupportUniformMapAndDynamicResolvers() {
        assertThat(QueueWeights.uniform().weightFor("any")).isEqualTo(1);
        var mapped = QueueWeights.fromMap(Map.of("project:42", 10, "project:43", 0));
        assertThat(mapped.weightFor("project:42")).isEqualTo(10);
        assertThat(mapped.weightFor("project:43")).isZero();
        assertThat(mapped.weightFor("project:44")).isEqualTo(1);

        var calls = new AtomicInteger();
        var dynamic = QueueWeights.from(queue -> {
            calls.incrementAndGet();
            return 3;
        });
        assertThat(dynamic.weightFor("project:x")).isEqualTo(3);
        assertThat(calls).hasValue(1);
    }

    @Test
    void negativeWeightsAreRejected() {
        assertThatThrownBy(() -> QueueWeights.fromMap(Map.of("project:42", -1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QueueWeights.from(queue -> -1).weightFor("project:42"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
