import com.hemju.threadmill.gradle.VerifyReleaseTag

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

val publishedProjects =
    listOf(
        project(":threadmill-core"),
        project(":threadmill-store-memory"),
        project(":threadmill-store-postgres"),
        project(":threadmill-store-redis"),
        project(":threadmill-spring-boot"),
        project(":threadmill-test-support"),
        project(":threadmill-metrics"),
        project(":threadmill-tracing"),
        project(":threadmill-dashboard-api"),
        project(":threadmill-dashboard-ui"),
        project(":threadmill-dashboard-spring"),
    )

dependencies { publishedProjects.forEach { add("nmcpAggregation", it) } }

allprojects { tasks.withType<Test>().configureEach { systemProperty("file.encoding", "UTF-8") } }

// --------------------------------------------------------- Aggregate lifecycle
//
// An unqualified task name on the command line fans out to every project, but
// dependsOn("clean") / dependsOn("check") resolves only the root task. Keep
// explicit aggregates so lifecycle dependencies cannot silently omit modules.

val cleanAll by
    tasks.registering {
        group = "build"
        description = "Delete build outputs for the root project and every subproject."
        dependsOn(":clean")
        dependsOn(subprojects.map { "${it.path}:clean" })
    }

val buildLogicTest by
    tasks.registering(Exec::class) {
        group = "verification"
        description = "Run the buildSrc build-logic regression tests."
        val wrapper =
            providers.systemProperty("os.name").map {
                if (it.startsWith("Windows")) "gradlew.bat" else "gradlew"
            }
        executable(wrapper.map { rootProject.file(it).absolutePath }.get())
        args("-p", rootProject.file("buildSrc").absolutePath, "test", "--no-configuration-cache")
        inputs.property("operatingSystem", providers.systemProperty("os.name"))
        inputs.files(
            fileTree("buildSrc") {
                include(
                    "build.gradle.kts",
                    "settings.gradle.kts",
                    "gradle.lockfile",
                    "settings-gradle.lockfile",
                    "src/**",
                )
            },
            file("build.gradle.kts"),
            file("settings.gradle.kts"),
            file("gradle.properties"),
            fileTree(rootDir) { include("threadmill-*/build.gradle.kts") },
        )
        // cleanAll deliberately cleans project outputs, not Gradle's separate
        // buildSrc build. Keep the success marker with buildSrc so an unchanged
        // productionCheck does not pay for a redundant nested build each time.
        val marker = layout.projectDirectory.file("buildSrc/build/build-logic-test/success.marker")
        outputs.file(marker)
        doLast {
            val markerFile = marker.asFile
            markerFile.parentFile.mkdirs()
            markerFile.writeText("verified\n")
        }
    }

tasks.named("check") {
    dependsOn(subprojects.map { "${it.path}:check" })
    dependsOn(buildLogicTest)
}

// A lifecycle task's ordering constraint does not extend to its dependencies.
// Order every non-clean task after cleanAll so compilation and resource work
// cannot consume stale outputs before a later clean deletes them.
allprojects {
    tasks.configureEach {
        if (
            name !in setOf("clean", "cleanAll", "verifyReleaseTag", "rejectDirectModulePublication")
        ) {
            mustRunAfter(cleanAll)
        }
    }
}

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
        val lockfiles =
            fileTree(rootDir) {
                    include("**/gradle.lockfile", "**/package-lock.json")
                    exclude(
                        "**/build/**",
                        "**/.gradle/**",
                        "**/node_modules/**",
                        "**/.worktrees/**",
                    )
                }
                .files
                .sortedBy { it.absolutePath }
        if (lockfiles.isEmpty()) {
            throw GradleException("dependencySecurityScan found no supported lockfiles")
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
        lockfiles.forEach {
            arguments.add("--lockfile")
            arguments.add(it.absolutePath)
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

val productionCheck by
    tasks.registering {
        group = "verification"
        description = "Run the production-readiness validation gauntlet."
        dependsOn(cleanAll, "check", "dependencySecurityScan", "artifactInspection")
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

val requestedReleaseTag =
    providers.gradleProperty("releaseTag").orElse(providers.environmentVariable("GITHUB_REF_NAME"))
val verifyReleaseTag by
    tasks.registering(VerifyReleaseTag::class) {
        publishedVersions.set(provider { publishedProjects.map { it.version.toString() } })
        releaseTag.set(requestedReleaseTag)
    }

// A release tag is cheap to validate. Run it before cleanAll, which in turn is
// ordered before every build and verification task in the publication graph.
cleanAll.configure { mustRunAfter(verifyReleaseTag) }

// One task graph builds, tests, inspects, signs, and uploads the same artifacts.
// Nmcp's non-prefixed task is a lifecycle alias; gate the real prefixed upload
// task as well, then stage and zip the already-verified artifacts afterwards.

listOf("nmcpCheckAggregationFiles", "nmcpZipAggregation").forEach {
    tasks.named(it) { mustRunAfter(productionCheck, verifyReleaseTag) }
}

val aggregationPublicationTasks =
    listOf(
        "publishAggregationToCentralPortal",
        "publishAggregationToCentralPortalSnapshots",
        "publishAggregationToCentralSnapshots",
        "nmcpPublishAggregationToCentralPortal",
        "nmcpPublishAggregationToCentralPortalSnapshots",
    )

aggregationPublicationTasks.forEach {
    tasks.named(it) { dependsOn(productionCheck, verifyReleaseTag) }
}

val modulePublicationTasks =
    listOf(
        "publishAllPublicationsToCentralPortal",
        "publishAllPublicationsToCentralPortalSnapshots",
        "publishAllPublicationsToCentralSnapshots",
        "nmcpPublishAllPublicationsToCentralPortal",
        "nmcpPublishAllPublicationsToCentralPortalSnapshots",
    )
val rejectDirectModulePublication by
    tasks.registering {
        description = "Reject unsupported per-module Central Portal publication."
        doLast {
            throw GradleException(
                "Threadmill publishes one atomic multi-module bundle; use " +
                    "publishAggregationToCentralPortal instead of a per-module publication task."
            )
        }
    }
val nmcpStagingTasks =
    setOf("publishMavenJavaPublicationToNmcpRepository", "publishAllPublicationsToNmcpRepository")

publishedProjects.forEach { publishedProject ->
    publishedProject.pluginManager.withPlugin("com.gradleup.nmcp") {
        modulePublicationTasks.forEach {
            // Threadmill releases one atomic multi-module aggregation. A direct
            // module upload could publish an incomplete release, so fail with an
            // actionable error before scheduling the expensive release gate.
            publishedProject.tasks.named(it) { dependsOn(rejectDirectModulePublication) }
        }
        // Nmcp creates repository staging tasks after its plugin callback starts.
        // An exact lazy match catches those later registrations without the
        // fail-open suffix matching that previously hid new/renamed task lanes.
        publishedProject.tasks
            .matching { it.name in nmcpStagingTasks }
            .configureEach {
                mustRunAfter(productionCheck, verifyReleaseTag, rejectDirectModulePublication)
            }
    }
}
