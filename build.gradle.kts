plugins {
    base
    alias(libs.plugins.spotless)
    // Applied without a version: the nmcp plugin is on the build classpath via
    // buildSrc (so the per-module `com.gradleup.nmcp` plugin is applicable from
    // the threadmill.publish convention plugin). Version is pinned in buildSrc.
    id("com.gradleup.nmcp.aggregation")
}

// ---------------------------------------------------------------- Publishing
//
// Aggregates every published module's signed Maven publication into a single
// bundle and uploads it to the Sonatype Central Portal (the successor to the
// retired OSSRH staging API). Per-module POM + signing live in the
// `threadmill.publish` convention plugin; this block only wires the upload.
//
// Credentials come from Gradle properties `centralPortalUsername` /
// `centralPortalPassword` (a Central Portal *user token*, not the account
// login), which the release workflow injects via the ORG_GRADLE_PROJECT_* env
// vars. `AUTOMATIC` uploads, validates, and then publishes the bundle to Maven
// Central without a manual click — a tagged `v*` release goes live on its own.
// Switch back to `USER_MANAGED` if you want to inspect a deployment in the
// Central Portal UI and click "Publish" yourself before it goes public.
nmcpAggregation {
    centralPortal {
        username = providers.gradleProperty("centralPortalUsername")
        password = providers.gradleProperty("centralPortalPassword")
        publishingType = "AUTOMATIC"
    }
}

dependencies {
    nmcpAggregation(project(":threadmill-core"))
    nmcpAggregation(project(":threadmill-store-memory"))
    nmcpAggregation(project(":threadmill-store-postgres"))
    nmcpAggregation(project(":threadmill-store-redis"))
    nmcpAggregation(project(":threadmill-spring-boot"))
    nmcpAggregation(project(":threadmill-test-support"))
    nmcpAggregation(project(":threadmill-metrics"))
    nmcpAggregation(project(":threadmill-tracing"))
    nmcpAggregation(project(":threadmill-dashboard-api"))
    nmcpAggregation(project(":threadmill-dashboard-ui"))
    nmcpAggregation(project(":threadmill-dashboard-spring"))
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
        palantirJavaFormat(palantirVersion).style("GOOGLE")
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
    description = "Scan every Git-tracked Gradle and npm lockfile for known vulnerabilities."
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
        val trackedFiles =
            providers.exec {
                workingDir(rootDir)
                commandLine("git", "ls-files", "-z", "--", "*gradle.lockfile", "*package-lock.json")
                isIgnoreExitValue = true
            }
        if (trackedFiles.result.get().exitValue != 0) {
            throw GradleException(
                "dependencySecurityScan requires a Git checkout so it can scan exactly the Git-tracked " +
                    "lockfiles. Run it from a clone/worktree; exported source trees are not release evidence."
            )
        }
        val lockfiles =
            trackedFiles.standardOutput.asText
                .get()
                .split('\u0000')
                .filter(String::isNotBlank)
                .map(rootProject::file)
                .sortedBy { it.absolutePath }
        if (lockfiles.isEmpty()) {
            throw GradleException("dependencySecurityScan found no supported lockfiles")
        }
        lockfiles.forEach {
            if (!it.isFile) {
                throw GradleException("Git-tracked lockfile does not exist: $it")
            }
        }
        logger.lifecycle(
            "Scanning ${lockfiles.size} Git-tracked dependency lockfiles:\n" +
                lockfiles.joinToString("\n") { "- ${it.relativeTo(rootDir)}" }
        )

        val npmAudit =
            providers.exec {
                workingDir(rootProject.file("threadmill-dashboard-ui"))
                commandLine("npm", "audit", "--package-lock-only", "--audit-level=low")
                isIgnoreExitValue = true
            }
        val npmStandardOutput = npmAudit.standardOutput.asText.get().trim()
        val npmStandardError = npmAudit.standardError.asText.get().trim()
        if (npmStandardOutput.isNotEmpty()) logger.lifecycle(npmStandardOutput)
        if (npmStandardError.isNotEmpty()) logger.warn(npmStandardError)
        val npmResult = npmAudit.result.get()
        if (npmResult.exitValue != 0) {
            throw GradleException("npm audit failed with exit code ${npmResult.exitValue}")
        }

        val arguments =
            mutableListOf(
                "scan",
                "source",
                "--config",
                rootProject.file("osv-scanner.toml").absolutePath,
                "--experimental-no-default-plugins",
                "--experimental-plugins",
                "lockfile",
                "--format",
                "table",
            )
        lockfiles.forEach { lockfile ->
            arguments.add("--lockfile")
            arguments.add(
                if (lockfile.name.endsWith("gradle.lockfile")) {
                    "gradle.lockfile:${lockfile.absolutePath}"
                } else {
                    lockfile.absolutePath
                }
            )
        }
        val scan =
            providers.exec {
                commandLine(osv, *arguments.toTypedArray())
                isIgnoreExitValue = true
            }
        val standardOutput = scan.standardOutput.asText.get().trim()
        val standardError = scan.standardError.asText.get().trim()
        if (standardOutput.isNotEmpty()) logger.lifecycle(standardOutput)
        if (standardError.isNotEmpty()) logger.warn(standardError)
        val result = scan.result.get()
        if (result.exitValue != 0) {
            throw GradleException(
                "dependencySecurityScan failed with exit code ${result.exitValue}"
            )
        }
    }
}

tasks.register("artifactInspection") {
    group = "verification"
    description = "Inspect release jars for legal files, private local material, and test classes."
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
                    val entries = zipTree(jar).matching { include("**/*") }.files
                    var hasLicense = false
                    var hasNotice = false
                    entries.forEach { entry ->
                        val path = entry.invariantSeparatorsPath
                        hasLicense = hasLicense || path.endsWith("/META-INF/LICENSE")
                        hasNotice = hasNotice || path.endsWith("/META-INF/NOTICE")
                        if (path.contains(".local-reference"))
                            problems.add("${jar.name} contains private local material")
                        if (
                            project.name != "threadmill-test-support" &&
                                (path.contains("/src/test/") || path.endsWith("Test.class"))
                        ) {
                            problems.add("${jar.name} appears to contain test material: $path")
                        }
                    }
                    if (!hasLicense) problems.add("${jar.name} does not contain META-INF/LICENSE")
                    if (!hasNotice) problems.add("${jar.name} does not contain META-INF/NOTICE")
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
        // The correctness simulation is the gate that caught the C1
        // in-memory concurrency bypass — a release candidate must run it.
        ":threadmill-simulation:simulate",
        // The process-separated nudge simulation pins maintenance-leader
        // hard-kill handoff and the documented producer crash window.
        ":threadmill-simulation:simulateNudge",
    )
    dependsOn(":threadmill-example:run")
}

val cleanTask = tasks.named("clean")

tasks
    .matching { it.name in setOf("check", "artifactInspection", "dependencySecurityScan") }
    .configureEach { mustRunAfter(cleanTask) }

subprojects {
    tasks
        .matching {
            it.name in setOf("jar", "test", "javadoc", "soak", "run") ||
                it.name.startsWith("simulate")
        }
        .configureEach { mustRunAfter(cleanTask) }
}
