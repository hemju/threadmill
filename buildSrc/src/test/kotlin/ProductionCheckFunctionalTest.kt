import java.io.File
import org.assertj.core.api.Assertions.assertThat
import org.gradle.testkit.runner.GradleRunner
import org.junit.jupiter.api.Test

class ProductionCheckFunctionalTest {
    private val repositoryRoot = File("..").canonicalFile
    private val projectVersion =
        VERSION_DECLARATION.find(
                File(repositoryRoot, "buildSrc/src/main/kotlin/threadmill.java-base.gradle.kts")
                    .readText()
            )
            ?.groupValues
            ?.get(1) ?: error("Could not read the Threadmill project version")

    @Test
    fun `production check cleans and checks every subproject from a clean graph`() {
        val taskPaths = dryRun("productionCheck", "-PdependencyScanRequired=false")
        val subprojects =
            repositoryRoot
                .listFiles()
                .orEmpty()
                .filter { it.isDirectory && it.name.startsWith("threadmill-") }
                .filter { File(it, "build.gradle.kts").isFile }
                .map { it.name }
                .sorted()

        val cleanPaths = listOf(":clean") + subprojects.map { ":$it:clean" }
        val checkPaths = listOf(":check") + subprojects.map { ":$it:check" }

        assertThat(taskPaths).containsAll(cleanPaths)
        assertThat(taskPaths).containsAll(checkPaths)
        assertThat(taskPaths).contains(":buildLogicTest", ":cleanAll", ":productionCheck")

        val cleanAllIndex = taskPaths.indexOf(":cleanAll")
        assertThat(cleanAllIndex).isGreaterThanOrEqualTo(cleanPaths.size)
        assertThat(cleanPaths).allSatisfy { cleanPath ->
            assertThat(taskPaths.indexOf(cleanPath)).isBetween(0, cleanAllIndex - 1)
        }
        taskPaths
            .filterNot(cleanPaths::contains)
            .filterNot { it == ":cleanAll" }
            .forEach { taskPath ->
                assertThat(taskPaths.indexOf(taskPath))
                    .describedAs("$taskPath must run after :cleanAll")
                    .isGreaterThan(cleanAllIndex)
            }
    }

    @Test
    fun `central publication runs only after the production and tag gates`() {
        val taskPaths =
            dryRun(
                "publishAggregationToCentralPortal",
                "-PreleaseTag=v0.2.2-SNAPSHOT",
                "-PcentralPortalUsername=dummy",
                "-PcentralPortalPassword=dummy",
                "-PdependencyScanRequired=false",
            )

        val productionCheckIndex = taskPaths.indexOf(":productionCheck")
        val releaseTagIndex = taskPaths.indexOf(":verifyReleaseTag")
        val zipIndex = taskPaths.indexOf(":nmcpZipAggregation")
        val uploadIndex = taskPaths.indexOf(":nmcpPublishAggregationToCentralPortal")

        assertThat(productionCheckIndex).isGreaterThanOrEqualTo(0)
        assertThat(releaseTagIndex).isGreaterThanOrEqualTo(0)
        assertThat(zipIndex).isGreaterThan(productionCheckIndex)
        assertThat(zipIndex).isGreaterThan(releaseTagIndex)
        assertThat(uploadIndex).isGreaterThan(zipIndex)
        assertThat(taskPaths.indexOf(":publishAggregationToCentralPortal"))
            .isGreaterThan(uploadIndex)
        assertThat(taskPaths.filter { it.endsWith(":publishMavenJavaPublicationToNmcpRepository") })
            .isNotEmpty
            .allSatisfy { taskPath ->
                assertThat(taskPaths.indexOf(taskPath)).isGreaterThan(productionCheckIndex)
                assertThat(taskPaths.indexOf(taskPath)).isGreaterThan(releaseTagIndex)
            }
    }

    @Test
    fun `release tag must exactly match the project version`() {
        val result =
            GradleRunner.create()
                .withProjectDir(repositoryRoot)
                .withArguments(
                    "verifyReleaseTag",
                    "-PreleaseTag=v-not-the-project-version",
                    "--no-configuration-cache",
                )
                .buildAndFail()

        assertThat(result.output)
            .contains("does not match project version")
            .contains("expected 'v$projectVersion'")
    }

    @Test
    fun `matching release tag uses the shared subproject version`() {
        val runner =
            GradleRunner.create()
                .withProjectDir(repositoryRoot)
                .withArguments(
                    "verifyReleaseTag",
                    "-PreleaseTag=v$projectVersion",
                    "--no-configuration-cache",
                )

        if (projectVersion.endsWith("-SNAPSHOT")) {
            assertThat(runner.buildAndFail().output)
                .contains("Refusing to publish snapshot version '$projectVersion'")
        } else {
            assertThat(runner.build().task(":verifyReleaseTag")).isNotNull
        }
    }

    private fun dryRun(vararg tasksAndArguments: String): List<String> {
        val result =
            GradleRunner.create()
                .withProjectDir(repositoryRoot)
                .withArguments(*tasksAndArguments, "--dry-run", "--no-configuration-cache")
                .build()
        return result.output
            .lineSequence()
            .mapNotNull { DRY_RUN_TASK.matchEntire(it)?.groupValues?.get(1) }
            .toList()
    }

    private companion object {
        val DRY_RUN_TASK = Regex("^(:\\S+) SKIPPED$")
        val VERSION_DECLARATION = Regex("(?m)^version = \"([^\"]+)\"$")
    }
}
