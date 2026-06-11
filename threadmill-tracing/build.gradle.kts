plugins {
    id("threadmill.java-module")
    id("threadmill.publish")
}

dependencies {
    api(project(":threadmill-core"))
    api(libs.opentelemetry.api)

    testImplementation(project(":threadmill-store-memory"))
    testImplementation(libs.opentelemetry.sdk)
    testImplementation(libs.opentelemetry.sdk.testing)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj.core)
}
