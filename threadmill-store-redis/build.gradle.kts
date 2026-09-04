plugins {
    id("threadmill.java-module")
    id("threadmill.publish")
}

dependencies {
    api(project(":threadmill-core"))
    // Lettuce 6.8.2 still declares Netty 4.1.125. Force the 4.1.137 security
    // floor: GHSA-4g8c-wm8x-jfhw / GHSA-389x-839f-4rhx affect reachable TLS or
    // common code; GHSA-fccg-mwvh-qqg4 / GHSA-c4c3-7fpv-j4q5 are server-only.
    // See docs/dependency-security.md for the reachability decisions.
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
