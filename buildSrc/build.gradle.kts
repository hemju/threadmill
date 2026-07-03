plugins { `kotlin-dsl` }

repositories {
    gradlePluginPortal()
    mavenCentral()
}

dependencies {
    // Makes `id("com.gradleup.nmcp")` applicable from the `threadmill.publish`
    // precompiled convention plugin so every published module contributes its
    // files to the root nmcp aggregation. Keep this version in lock-step with
    // the `nmcp` version in gradle/libs.versions.toml.
    implementation("com.gradleup.nmcp:com.gradleup.nmcp.gradle.plugin:1.6.1")
}
