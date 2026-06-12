plugins {
    base
    alias(libs.plugins.spotless)
}

allprojects { tasks.withType<Test>().configureEach { systemProperty("file.encoding", "UTF-8") } }

// ---------------------------------------------------------------- Spotless
//
// Code formatting + hygiene enforced by `check`.
// Run `./gradlew spotlessApply` to auto-fix any violation locally.

spotless {
    val palantirVersion = libs.versions.palantirJavaFormat.get()
    val javaTargets = listOf("threadmill-*/src/**/*.java", "buildSrc/src/**/*.java")
    val excludedJavaPaths =
        listOf("**/build/**", "**/.gradle/**", "**/generated/**", ".local-reference/**")

    java {
        target(javaTargets)
        targetExclude(excludedJavaPaths)
        palantirJavaFormat(palantirVersion)
        importOrder("java", "javax", "jakarta", "", "com.hemju")
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()
    }

    kotlinGradle {
        target("*.gradle.kts", "**/*.gradle.kts")
        targetExclude("**/build/**", "**/.gradle/**", ".local-reference/**")
        ktfmt().kotlinlangStyle()
        trimTrailingWhitespace()
        endWithNewline()
    }

    kotlin {
        target("buildSrc/src/**/*.kt")
        targetExclude(".local-reference/**")
        ktfmt().kotlinlangStyle()
        trimTrailingWhitespace()
        endWithNewline()
    }

    format("misc") {
        target("*.md", ".gitignore")
        targetExclude("**/build/**", "**/.gradle/**", ".local-reference/**")
        trimTrailingWhitespace()
        endWithNewline()
    }
}

tasks.named("check") { dependsOn("spotlessCheck") }

tasks.register("dependencySecurityScan") {
    group = "verification"
    description = "Run an OSV dependency scan when osv-scanner is installed."
    doLast {
        val osv =
            providers
                .exec { commandLine("sh", "-c", "command -v osv-scanner || true") }
                .standardOutput
                .asText
                .get()
                .trim()
        if (osv.isBlank()) {
            // Fail-closed in CI, warn-and-pass locally: a release pipeline must
            // not silently skip the scan, but a developer's laptop without
            // osv-scanner should not be blocked from running productionCheck.
            // CI systems set CI=true; -PdependencyScanRequired=true|false forces
            // either behavior explicitly.
            val forced =
                (project.findProperty("dependencyScanRequired") as String?)?.toBooleanStrictOrNull()
            val required =
                forced ?: (System.getenv("CI")?.equals("true", ignoreCase = true) == true)
            if (!required) {
                logger.warn(
                    "Skipping dependencySecurityScan: osv-scanner is not installed. " +
                        "Install it (https://osv.dev) to scan dependencies for CVEs; this is enforced in CI."
                )
                return@doLast
            }
            throw GradleException(
                "dependencySecurityScan requires osv-scanner (https://osv.dev) in CI. Install it on the build " +
                    "agent, or pass -PdependencyScanRequired=false to skip (not recommended for releases)."
            )
        }
        val result =
            providers
                .exec { commandLine(osv, "scan", "--lockfile", "gradle/libs.versions.toml", ".") }
                .result
                .get()
        if (result.exitValue != 0) {
            throw GradleException(
                "dependencySecurityScan failed with exit code ${result.exitValue}"
            )
        }
    }
}

tasks.register("artifactInspection") {
    group = "verification"
    description = "Inspect release jars for private local material and test classes."
    dependsOn(subprojects.map { it.tasks.matching { task -> task.name == "jar" } })
    doLast {
        val problems = mutableListOf<String>()
        subprojects.forEach { project ->
            project.layout.buildDirectory
                .dir("libs")
                .get()
                .asFile
                .listFiles()
                ?.filter {
                    it.name == "${project.name}-${project.version}.jar" &&
                        !it.name.endsWith("-sources.jar") &&
                        !it.name.endsWith("-javadoc.jar")
                }
                ?.forEach { jar ->
                    zipTree(jar)
                        .matching { include("**/*") }
                        .files
                        .forEach { entry ->
                            val path = entry.invariantSeparatorsPath
                            if (path.contains(".local-reference"))
                                problems.add("${jar.name} contains private local material")
                            if (
                                project.name != "threadmill-test-support" &&
                                    (path.contains("/src/test/") || path.endsWith("Test.class"))
                            ) {
                                problems.add("${jar.name} appears to contain test material: $path")
                            }
                        }
                }
        }
        if (problems.isNotEmpty()) throw GradleException(problems.joinToString("\n"))
    }
}

tasks.register("productionCheck") {
    group = "verification"
    description = "Run the production-readiness validation gauntlet."
    dependsOn("clean", "check", "dependencySecurityScan", "artifactInspection")
    dependsOn(subprojects.map { it.tasks.matching { task -> task.name == "javadoc" } })
    dependsOn(
        ":threadmill-store-postgres:test",
        ":threadmill-store-redis:test",
        ":threadmill-soak:soakRegression",
    )
    dependsOn(":threadmill-example:run")
}

val cleanTask = tasks.named("clean")

tasks
    .matching { it.name in setOf("check", "artifactInspection", "dependencySecurityScan") }
    .configureEach { mustRunAfter(cleanTask) }

subprojects {
    tasks
        .matching { it.name in setOf("jar", "test", "javadoc", "soak", "run") }
        .configureEach { mustRunAfter(cleanTask) }
}
