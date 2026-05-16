plugins {
    id("threadmill.java-module")
    application
}

dependencies {
    implementation(project(":threadmill-core"))
    implementation(project(":threadmill-store-memory"))
    implementation(project(":threadmill-store-postgres"))
    implementation(project(":threadmill-store-redis"))
    implementation(libs.postgresql.jdbc)
    implementation(libs.lettuce.core)
    runtimeOnly(libs.slf4j.simple)
}

application { mainClass.set("com.example.threadmill.GettingStartedMain") }

tasks.named<JavaExec>("run") {
    // Friendlier console output for the demo.
    systemProperty("org.slf4j.simpleLogger.showDateTime", "true")
    systemProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss.SSS")
    systemProperty("org.slf4j.simpleLogger.showShortLogName", "true")
    systemProperty("org.slf4j.simpleLogger.defaultLogLevel", "info")
    standardInput = System.`in`
}

// Helper to apply the friendly console logging to all of the example's launches.
fun JavaExec.demoLogging() {
    systemProperty("org.slf4j.simpleLogger.showDateTime", "true")
    systemProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss.SSS")
    systemProperty("org.slf4j.simpleLogger.showShortLogName", "true")
    systemProperty("org.slf4j.simpleLogger.defaultLogLevel", "info")
}

fun JavaExec.exampleBackend(name: String) {
    systemProperty("threadmill.example.backend", name)
}

tasks.register<JavaExec>("runWorker") {
    description =
        "Start a long-running worker against the shared PostgreSQL backend. Pass a friendly label via --args=\"name\"."
    group = "application"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.example.threadmill.StoreWorkerMain")
    exampleBackend("postgres")
    standardInput = System.`in`
    demoLogging()
}

tasks.register<JavaExec>("runPostgresWorker") {
    description = "Start a long-running worker against the shared PostgreSQL backend."
    group = "application"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.example.threadmill.StoreWorkerMain")
    exampleBackend("postgres")
    standardInput = System.`in`
    demoLogging()
}

tasks.register<JavaExec>("runRedisWorker") {
    description = "Start a long-running worker against the shared Redis backend."
    group = "application"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.example.threadmill.StoreWorkerMain")
    exampleBackend("redis")
    standardInput = System.`in`
    demoLogging()
}

tasks.register<JavaExec>("submit") {
    description = "Enqueue example jobs against the shared PostgreSQL backend."
    group = "application"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.example.threadmill.StoreSubmitterMain")
    exampleBackend("postgres")
    demoLogging()
}

tasks.register<JavaExec>("submitPostgres") {
    description = "Enqueue example jobs against the shared PostgreSQL backend."
    group = "application"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.example.threadmill.StoreSubmitterMain")
    exampleBackend("postgres")
    demoLogging()
}

tasks.register<JavaExec>("submitRedis") {
    description = "Enqueue example jobs against the shared Redis backend."
    group = "application"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.example.threadmill.StoreSubmitterMain")
    exampleBackend("redis")
    demoLogging()
}
