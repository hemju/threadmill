plugins {
    id("threadmill.java-module")
    id("threadmill.publish")
}

dependencies {
    api(project(":threadmill-core"))
    api(libs.micrometer.core)

    testImplementation(project(":threadmill-store-memory"))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj.core)
    testImplementation(libs.slf4j.simple)
    testImplementation(libs.awaitility)
}
