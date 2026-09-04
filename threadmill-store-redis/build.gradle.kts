plugins {
    id("threadmill.java-module")
    id("threadmill.publish")
}

dependencies {
    api(project(":threadmill-core"))
    // Lettuce 6.8.2 declares Netty 4.1.125, which carries 10 known advisories.
    // Force 4.1.137 for Redis-reachable DNS and TLS fixes:
    // GHSA-5pvg-856g-cp85, GHSA-676x-f7gg-47vc, GHSA-xmv7-r254-6q78,
    // GHSA-c653-97m9-rcg9, GHSA-cm33-6792-r9fm, and GHSA-mfg7-5gfp-c4w3.
    // The same floor clears GHSA-x4gw-5cx5-pgmh, GHSA-3qp7-7mw8-wx86,
    // GHSA-558v-64gr-wgg4, and GHSA-mj4r-2hfc-f8p6 from unused surfaces.
    // Remove only when Lettuce's transitive Netty floor reaches 4.1.137. See
    // docs/dependency-security.md for the reachability decisions.
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
