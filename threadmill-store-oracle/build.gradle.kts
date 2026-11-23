plugins {
    id("threadmill.java-module")
    id("threadmill.publish")
}

dependencies {
    api(project(":threadmill-core"))
    // The store uses standard JDBC only. Applications supply the Oracle driver
    // (ojdbc11) themselves, so the published module carries no dependency on
    // Oracle-licensed artifacts; the tests pin the driver below.

    testImplementation(project(":threadmill-test-support"))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj.core)
    testImplementation(libs.slf4j.simple)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation("org.testcontainers:testcontainers")
    testImplementation(libs.oracle.jdbc)
    testImplementation(libs.hikaricp)
}

// Database selection for the integration tests (see OracleTestDatabase):
//   -PoracleImage=gvenzl/oracle-xe:21-slim-faststart   another gvenzl image (CI uses 21c XE)
//   -PoracleJdbcUrl=... -PoracleUser=... -PoraclePassword=...   an existing database, for
//       example a real 19c instance; its schema is reset by the tests
fun Test.passDatabaseSelection() {
    mapOf(
            "oracleImage" to "threadmill.oracle.image",
            "oracleJdbcUrl" to "threadmill.oracle.jdbcUrl",
            "oracleUser" to "threadmill.oracle.user",
            "oraclePassword" to "threadmill.oracle.password",
        )
        .forEach { (property, systemProperty) ->
            providers.gradleProperty(property).orNull?.let { systemProperty(systemProperty, it) }
        }
}

tasks.named<Test>("test") { passDatabaseSelection() }

// The whole suite again with the national text type (NVARCHAR2/NCLOB) on a
// WE8MSWIN1252 database: the container's pluggable database is relabelled, so a
// bind that bypasses the national character set loses characters and fails.
// -PoracleCharacterSet picks another set for the container; an existing
// database (-PoracleJdbcUrl) keeps its own character set.
val nationalTest by
    tasks.registering(Test::class) {
        description =
            "Runs the Oracle suite with the national text type on a WE8MSWIN1252 database."
        group = LifecycleBasePlugin.VERIFICATION_GROUP
        testClassesDirs = sourceSets.test.get().output.classesDirs
        classpath = sourceSets.test.get().runtimeClasspath
        passDatabaseSelection()
        systemProperty("threadmill.oracle.textType", "NATIONAL")
        systemProperty(
            "threadmill.oracle.characterSet",
            providers.gradleProperty("oracleCharacterSet").getOrElse("WE8MSWIN1252"),
        )
        shouldRunAfter(tasks.named("test"))
    }

tasks.named("check") { dependsOn(nationalTest) }
