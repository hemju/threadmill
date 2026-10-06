import java.net.ServerSocket

plugins {
    id("threadmill.java-module")
    id("threadmill.publish")
}

val browserTestSourceSet =
    sourceSets.create("browserTest") {
        compileClasspath += sourceSets.main.get().output
        runtimeClasspath += output + compileClasspath
    }

configurations[browserTestSourceSet.implementationConfigurationName].extendsFrom(
    configurations.testImplementation.get()
)

configurations[browserTestSourceSet.runtimeOnlyConfigurationName].extendsFrom(
    configurations.testRuntimeOnly.get()
)

dependencies {
    constraints {
        // Spring Boot 4.0.8 resolves Tomcat 11.0.24, below the 11.0.25 floor for
        // GHSA-9xv2-5v5q-p794, GHSA-gcx9-497g-6cp6, and GHSA-h3x4-894j-xpx5.
        // Remove these constraints when Boot's BOM resolves Tomcat >= 11.0.25.
        testImplementation(
            "org.apache.tomcat.embed:tomcat-embed-core:${libs.versions.tomcat.get()}"
        )
        testImplementation("org.apache.tomcat.embed:tomcat-embed-el:${libs.versions.tomcat.get()}")
        testImplementation(
            "org.apache.tomcat.embed:tomcat-embed-websocket:${libs.versions.tomcat.get()}"
        )
        // Spring Boot 4.0.8 resolves Jackson 3.1.5, below the 3.1.7 floor for
        // GHSA-7hhh-6rmp-j9qf, GHSA-p6pp-m3f8-5c89, GHSA-cxp5-3px4-pw24,
        // GHSA-wv8q-qhhj-9h54, GHSA-gx83-3vf8-gh7j, GHSA-q4xh-88c3-wmh7, and
        // GHSA-wjgm-6hv5-3cvf. Remove these constraints when Boot's BOM resolves
        // Jackson 3 >= 3.1.7.
        testImplementation("tools.jackson.core:jackson-core:${libs.versions.jackson3.get()}")
        testImplementation("tools.jackson.core:jackson-databind:${libs.versions.jackson3.get()}")
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

    add(browserTestSourceSet.runtimeOnlyConfigurationName, project(":threadmill-dashboard-ui"))
}

val browserTest by
    tasks.registering(Exec::class) {
        group = "verification"
        description = "Run Playwright against the packaged UI mounted by the Spring dashboard."
        dependsOn(
            browserTestSourceSet.classesTaskName,
            ":threadmill-dashboard-ui:npmInstall",
            ":threadmill-dashboard-ui:npmBuild",
        )
        workingDir(rootProject.file("threadmill-dashboard-ui"))
        inputs.dir(rootProject.file("threadmill-dashboard-ui/browser-tests"))
        inputs.file(rootProject.file("threadmill-dashboard-ui/playwright.config.ts"))
        inputs.file(rootProject.file("threadmill-dashboard-ui/package.json"))
        inputs.file(rootProject.file("threadmill-dashboard-ui/package-lock.json"))
        inputs.files(browserTestSourceSet.runtimeClasspath)
        outputs.dir(rootProject.file("threadmill-dashboard-ui/build/playwright-report"))
        doFirst {
            val browserPort = ServerSocket(0).use { it.localPort.toString() }
            environment("THREADMILL_BROWSER_PORT", browserPort)
            environment(
                "THREADMILL_BROWSER_SERVER_CLASSPATH",
                browserTestSourceSet.runtimeClasspath.asPath,
            )
        }
        commandLine("npm", "run", "test:browser")
    }

tasks.named("check") { dependsOn(browserTestSourceSet.classesTaskName) }
