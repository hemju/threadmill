package com.hemju.threadmill.core.serialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import org.junit.jupiter.api.Test;

class DefaultTypingGuardTest {
    @Test
    void rejectsAMapperWithDefaultTypingEnabled() {
        var unsafe = new ObjectMapper()
                .activateDefaultTyping(
                        BasicPolymorphicTypeValidator.builder()
                                .allowIfBaseType(Object.class)
                                .build(),
                        DefaultTyping.NON_FINAL);
        assertThatThrownBy(() -> new JsonJobSerializer(unsafe))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("default typing");
    }

    @Test
    void acceptsTheDefaultMapper() {
        assertThat(new JsonJobSerializer(JsonJobSerializer.defaultMapper())).isNotNull();
    }
}
