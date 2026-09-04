plugins {
    id("threadmill.java-module")
    id("threadmill.publish")
}

dependencies {
    constraints {
        // Spring Boot 4.0.8's BOM resolves Tomcat 11.0.24, which remains below
        // the 11.0.25 security floor. These test-only starters still execute in
        // CI, so constrain the complete embedded-Tomcat set rather than ignore
        // the advisories.
        testImplementation(
            "org.apache.tomcat.embed:tomcat-embed-core:${libs.versions.tomcat.get()}"
        )
        testImplementation("org.apache.tomcat.embed:tomcat-embed-el:${libs.versions.tomcat.get()}")
        testImplementation(
            "org.apache.tomcat.embed:tomcat-embed-websocket:${libs.versions.tomcat.get()}"
        )

        // Keep the patched Jackson release until Spring Boot's BOM catches up.
        testImplementation(libs.jackson3.databind)
    }
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
