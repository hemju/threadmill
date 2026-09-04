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
        // Spring Boot 4.0.7's BOM resolves tools.jackson.core:jackson-databind
        // 3.1.4 (GHSA-5gvw-p9qm-jgwh) onto the test classpath; constrain the
        // patched release until a Boot patch carries it. Remove when the
        // springBoot pin advances past 4.0.7.
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
            environment(
                "THREADMILL_BROWSER_SERVER_CLASSPATH",
                browserTestSourceSet.runtimeClasspath.asPath,
            )
        }
        commandLine("npm", "run", "test:browser")
    }
