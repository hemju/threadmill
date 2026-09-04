import java.io.File
import org.assertj.core.api.Assertions.assertThat
import org.gradle.testkit.runner.GradleRunner
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class ProductionCheckFunctionalTest {
    private val repositoryRoot =
        File(requireNotNull(System.getProperty("threadmill.repositoryRoot"))).canonicalFile

    @Test
    fun `production check cleans and checks every subproject from a clean graph`() {
        val taskPaths = dryRun("productionCheck", "-PdependencyScanRequired=false")
        val subprojects =
            INCLUDED_PROJECT.findAll(File(repositoryRoot, "settings.gradle.kts").readText())
                .map { it.groupValues[1] }
                .sorted()
                .toList()

        val cleanPaths = listOf(":clean") + subprojects.map { ":$it:clean" }
        val checkPaths = listOf(":check") + subprojects.map { ":$it:check" }

        assertThat(taskPaths).containsAll(cleanPaths)
        assertThat(taskPaths).containsAll(checkPaths)
        assertThat(taskPaths).contains(":buildLogicTest", ":cleanAll", ":productionCheck")

        val cleanAllIndex = taskPaths.indexOf(":cleanAll")
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
    fun `every supported central publication entry point runs after the production and tag gates`() {
        val aggregationTaskPaths = centralPublicationTaskPaths().filterNot(::isModuleTask)
        val publicationTaskPaths = aggregationTaskPaths + nmcpStagingTaskPaths()
        val taskPaths =
            dryRun(
                *publicationTaskPaths.toTypedArray(),
                "-PcentralPortalUsername=dummy",
                "-PcentralPortalPassword=dummy",
            )

        val cleanAllIndex = taskPaths.indexOf(":cleanAll")
        val productionCheckIndex = taskPaths.indexOf(":productionCheck")
        val releaseTagIndex = taskPaths.indexOf(":verifyReleaseTag")
        val zipIndex = taskPaths.indexOf(":nmcpZipAggregation")
        val uploadIndex = taskPaths.indexOf(":nmcpPublishAggregationToCentralPortal")

        assertThat(productionCheckIndex).isGreaterThanOrEqualTo(0)
        assertThat(releaseTagIndex).isGreaterThanOrEqualTo(0)
        assertThat(releaseTagIndex).isLessThan(cleanAllIndex)
        assertThat(releaseTagIndex).isLessThan(productionCheckIndex)
        assertThat(zipIndex).isGreaterThan(productionCheckIndex)
        assertThat(zipIndex).isGreaterThan(releaseTagIndex)
        assertThat(uploadIndex).isGreaterThan(zipIndex)
        assertThat(publicationTaskPaths).isNotEmpty.allSatisfy { taskPath ->
            assertThat(taskPaths.indexOf(taskPath))
                .describedAs("$taskPath must run after :productionCheck")
                .isGreaterThan(productionCheckIndex)
            assertThat(taskPaths.indexOf(taskPath))
                .describedAs("$taskPath must run after :verifyReleaseTag")
                .isGreaterThan(releaseTagIndex)
        }
        assertThat(taskPaths.filter { it.endsWith(":publishMavenJavaPublicationToNmcpRepository") })
            .isNotEmpty
            .allSatisfy { taskPath ->
                assertThat(taskPaths.indexOf(taskPath)).isGreaterThan(productionCheckIndex)
                assertThat(taskPaths.indexOf(taskPath)).isGreaterThan(releaseTagIndex)
            }
    }

    @Test
    fun `direct module central publication lanes refuse before the release gauntlet`() {
        val directModuleTaskPaths = centralPublicationTaskPaths().filter(::isModuleTask)
        val taskPaths = dryRun(*directModuleTaskPaths.toTypedArray())

        assertThat(directModuleTaskPaths).isNotEmpty
        assertThat(taskPaths)
            .contains(":rejectDirectModulePublication")
            .doesNotContain(":cleanAll", ":productionCheck", ":verifyReleaseTag")

        val rejectionIndex = taskPaths.indexOf(":rejectDirectModulePublication")
        taskPaths
            .filterNot { it == ":rejectDirectModulePublication" }
            .forEach { taskPath ->
                assertThat(taskPaths.indexOf(taskPath))
                    .describedAs("$taskPath must run after the direct-publication refusal")
                    .isGreaterThan(rejectionIndex)
            }
    }

    @Test
    fun `release tag reads each published project's actual version`(
        @TempDir temporaryDirectory: File
    ) {
        val initScript = File(temporaryDirectory, "override-version.init.gradle")
        initScript.writeText(
            """
            gradle.afterProject { project, state ->
                if (project.path == ':threadmill-core') {
                    project.version = 'test-inconsistent-version'
                }
            }
            """
                .trimIndent()
        )

        val result =
            GradleRunner.create()
                .withProjectDir(repositoryRoot)
                .withArguments(
                    "verifyReleaseTag",
                    "-I",
                    initScript.absolutePath,
                    "--no-configuration-cache",
                )
                .buildAndFail()

        assertThat(result.output)
            .contains("one consistent release version across all published modules")
            .contains("test-inconsistent-version")
    }

    private fun centralPublicationTaskPaths(): List<String> {
        val result =
            GradleRunner.create()
                .withProjectDir(repositoryRoot)
                .withArguments("tasks", "--all", "--no-configuration-cache")
                .build()
        val centralTasks =
            result.output
                .lineSequence()
                .mapNotNull { CENTRAL_PUBLICATION_TASK.matchEntire(it)?.groupValues?.get(1) }
                .map { ":$it" }
                .toList()
        return centralTasks.distinct()
    }

    private fun nmcpStagingTaskPaths(): List<String> {
        val publishedProjects =
            PUBLISHED_PROJECT.findAll(File(repositoryRoot, "build.gradle.kts").readText())
                .map { it.groupValues[1] }
                .toList()
        return publishedProjects.flatMap { project ->
            NMCP_STAGING_TASKS.map { task -> ":$project:$task" }
        }
    }

    private fun isModuleTask(taskPath: String): Boolean = taskPath.indexOf(':', startIndex = 1) >= 0

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
        val CENTRAL_PUBLICATION_TASK =
            Regex("^(\\S*(?:CentralPortal|CentralSnapshots)\\S*)(?:\\s+.*)?$")
        val DRY_RUN_TASK = Regex("^(:\\S+) SKIPPED$")
        val INCLUDED_PROJECT = Regex("\"(threadmill-[a-z-]+)\"")
        val PUBLISHED_PROJECT = Regex("project\\(\":(threadmill-[a-z-]+)\"\\)")
        val NMCP_STAGING_TASKS =
            listOf(
                "publishMavenJavaPublicationToNmcpRepository",
                "publishAllPublicationsToNmcpRepository",
            )
    }
}
