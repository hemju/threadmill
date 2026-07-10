plugins {
    id("threadmill.java-module")
    id("threadmill.publish")
}

dependencies {
    api(project(":threadmill-core"))
    api(project(":threadmill-dashboard-api"))
    api(libs.spring.boot.autoconfigure)
    api(libs.spring.context)
    api(libs.spring.webmvc)
    api(libs.spring.security.config)
    api(libs.spring.security.core)
    api(libs.spring.security.web)
    api(libs.slf4j.api)
    compileOnly(libs.jakarta.servlet.api)

    testImplementation(project(":threadmill-store-memory"))
    testImplementation(libs.jakarta.servlet.api)
    testImplementation(libs.spring.boot.starter.webmvc)
    testImplementation(libs.spring.boot.starter.security)
    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.logback.classic)
    testImplementation(libs.spring.security.test)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj.core)
}
