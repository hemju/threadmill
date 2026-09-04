package com.hemju.threadmill.gradle

import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class ReleaseTagValidationTest {
    @Test
    fun `accepts an exact release tag`() {
        assertThatCode { ReleaseTagValidation.verify(listOf("1.2.3"), "v1.2.3") }
            .doesNotThrowAnyException()
    }

    @Test
    fun `rejects a snapshot before suggesting a tag`() {
        assertThatThrownBy { ReleaseTagValidation.verify(listOf("1.2.3-SNAPSHOT"), "v1.2.3") }
            .hasMessageContaining("Refusing to publish snapshot version '1.2.3-SNAPSHOT'")
            .hasMessageContaining("Set a release version")
            .hasMessageNotContaining("expected 'v1.2.3-SNAPSHOT'")
    }

    @Test
    fun `rejects a missing tag`() {
        assertThatThrownBy { ReleaseTagValidation.verify(listOf("1.2.3"), null) }
            .hasMessage("Release tag is unavailable; pass -PreleaseTag=v1.2.3.")
    }

    @Test
    fun `rejects a mismatched tag`() {
        assertThatThrownBy { ReleaseTagValidation.verify(listOf("1.2.3"), "v1.2.4") }
            .hasMessageContaining("does not match project version '1.2.3'")
            .hasMessageContaining("expected 'v1.2.3'")
    }

    @Test
    fun `rejects inconsistent published versions`() {
        assertThatThrownBy { ReleaseTagValidation.verify(listOf("1.2.3", "1.2.4"), "v1.2.3") }
            .hasMessageContaining("one consistent release version across all published modules")
            .hasMessageContaining("1.2.3")
            .hasMessageContaining("1.2.4")
    }
}
