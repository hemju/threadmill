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
    implementation(platform(libs.testcontainers.bom))
    implementation(libs.testcontainers.postgresql)
    implementation("org.testcontainers:testcontainers")

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj.core)
}

// Per-backend simulation tasks. The base `simulate` runs all three sequentially.
val simulationMainClass = "com.hemju.threadmill.simulation.SimulationMain"
val workerChurnMainClass = "com.hemju.threadmill.simulation.workerchurn.WorkerChurnSimulatorMain"

tasks.register<JavaExec>("simulateMemory") {
    group = "verification"
    description = "Run the simulation against the in-memory store (fast)."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(simulationMainClass)
    args = listOf("--backend", "memory")
}

tasks.register<JavaExec>("simulatePostgres") {
    group = "verification"
    description = "Run the simulation against PostgreSQL 18 via Testcontainers."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(simulationMainClass)
    args = listOf("--backend", "postgres")
}

tasks.register<JavaExec>("simulateRedis") {
    group = "verification"
    description = "Run the simulation against Redis via Testcontainers."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(simulationMainClass)
    args = listOf("--backend", "redis")
}

tasks.register("simulate") {
    group = "verification"
    description = "Run the full simulation against all three backends."
    dependsOn("simulateMemory", "simulatePostgres", "simulateRedis")
}

tasks.register<JavaExec>("simulateWorkerChurnPostgres") {
    group = "verification"
    description =
        "Run the multi-process worker-churn simulation against a shared PostgreSQL backend."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(workerChurnMainClass)
    args = listOf("--backend", "postgres")
}

tasks.register<JavaExec>("simulateWorkerChurnRedis") {
    group = "verification"
    description = "Run the multi-process worker-churn simulation against a shared Redis backend."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(workerChurnMainClass)
    args = listOf("--backend", "redis")
}
