plugins {
    id("threadmill.java-module")
    id("threadmill.publish")
}

dependencies {
    api(project(":threadmill-core"))
    // Force Netty to the current security floor (4.1.137: SNI parsing and
    // routing fixes) regardless of Lettuce's transitive pin.
    api(platform(libs.netty.bom))
    api(libs.lettuce.core)

    testImplementation(project(":threadmill-test-support"))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj.core)
    testImplementation(libs.slf4j.simple)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation("org.testcontainers:testcontainers")
}
