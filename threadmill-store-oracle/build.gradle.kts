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
tasks.named<Test>("test") {
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
