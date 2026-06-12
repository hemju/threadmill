plugins { id("threadmill.java-module") }

dependencies {
    implementation(project(":threadmill-core"))
    implementation(project(":threadmill-store-memory"))
    implementation(project(":threadmill-store-postgres"))
    implementation(project(":threadmill-store-redis"))
    implementation(libs.jackson.databind)
    implementation(libs.jackson.datatype.jsr310)
    implementation(libs.slf4j.api)
    implementation(libs.slf4j.simple)
    implementation(libs.postgresql.jdbc)
    implementation(libs.lettuce.core)
    implementation(libs.networknt.jsonschema)
    implementation(platform(libs.testcontainers.bom))
    implementation(libs.testcontainers.postgresql)
    implementation("org.testcontainers:testcontainers")

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj.core)
    testImplementation(libs.awaitility)
    testImplementation(libs.testcontainers.junit.jupiter)
}

// The soak regression suite is a tag-filtered JUnit test runner: fixed,
// production-oriented checks that remain separate from the tunable load harness.
val soakRegression by
    tasks.registering(Test::class) {
        description = "Run the fixed soak regression suite. Not part of `check`."
        group = "verification"
        useJUnitPlatform { includeTags("soak") }
        testClassesDirs = sourceSets["test"].output.classesDirs
        classpath = sourceSets["test"].runtimeClasspath
        // Long-running by intent; no time-out.
        forkEvery = 0
    }

tasks.named<Test>("test") { useJUnitPlatform { excludeTags("soak") } }

tasks.named("check") {
    // Explicitly do NOT depend on `soakRegression`.
}

// ---------------------------------------------------------------- load soak harness

val soakHarnessMainClass = "com.hemju.threadmill.soak.harness.SoakHarnessMain"

fun JavaExec.passSoakProps() {
    // Forward known -P props as system properties so SoakHarnessMain can read them.
    // Keeping the Main independent of Gradle lets the JUnit smoke tests invoke
    // SoakHarnessMain.main(...) directly with the same system-property contract.
    listOf(
            "scenario",
            "duration",
            "jobsPerSecond",
            "workerCount",
            "nodes",
            "outputDir",
            "runId",
            "failFast",
            "postgresUrl",
            "force",
            "redisTopology",
            "redisUrl",
            "progressInterval",
            "nodeChurn",
        )
        .forEach { name ->
            findProperty(name)?.toString()?.let { systemProperty("threadmill.soak.$name", it) }
        }
}

tasks.register<JavaExec>("soakMemory") {
    group = "verification"
    description = "Run a soak scenario against the in-memory store."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(soakHarnessMainClass)
    args = listOf("--backend", "memory")
    passSoakProps()
}

tasks.register<JavaExec>("soakPostgres") {
    group = "verification"
    description = "Run a soak scenario against PostgreSQL 18 via Testcontainers."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(soakHarnessMainClass)
    args = listOf("--backend", "postgres")
    passSoakProps()
}

tasks.register<JavaExec>("soakRedis") {
    group = "verification"
    description = "Run a soak scenario against Redis via Testcontainers."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(soakHarnessMainClass)
    args = listOf("--backend", "redis")
    passSoakProps()
}

tasks.register("soakAll") {
    group = "verification"
    description = "Run the chosen scenario against Postgres then Redis sequentially."
    dependsOn("soakPostgres", "soakRedis")
}
