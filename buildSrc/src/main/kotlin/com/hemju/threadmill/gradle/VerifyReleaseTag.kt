package com.hemju.threadmill.gradle

import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.TaskAction

/** Fails a publication build unless one release version exactly matches its Git tag. */
abstract class VerifyReleaseTag : DefaultTask() {
    @get:Input abstract val publishedVersions: ListProperty<String>

    @get:Input @get:Optional abstract val releaseTag: Property<String>

    init {
        group = "verification"
        description = "Require the release tag to exactly match the project version."
    }

    @TaskAction
    fun verify() {
        ReleaseTagValidation.verify(publishedVersions.get(), releaseTag.orNull)
    }
}

internal object ReleaseTagValidation {
    fun verify(publishedVersions: List<String>, releaseTag: String?) {
        val versions = publishedVersions.distinct()
        if (versions.size != 1 || versions.single() == "unspecified") {
            throw GradleException(
                "Expected one consistent release version across all published modules, found " +
                    versions.joinToString(prefix = "[", postfix = "]")
            )
        }
        val projectVersion = versions.single()
        if (projectVersion.endsWith("-SNAPSHOT")) {
            throw GradleException(
                "Refusing to publish snapshot version '$projectVersion'. Set a release version in " +
                    "buildSrc/src/main/kotlin/com/hemju/threadmill/gradle/ThreadmillVersion.kt and tag that commit."
            )
        }

        val expectedTag = "v$projectVersion"
        if (releaseTag == null) {
            throw GradleException("Release tag is unavailable; pass -PreleaseTag=$expectedTag.")
        }
        if (releaseTag != expectedTag) {
            throw GradleException(
                "Release tag '$releaseTag' does not match project version '$projectVersion'; " +
                    "expected '$expectedTag'."
            )
        }
    }
}
