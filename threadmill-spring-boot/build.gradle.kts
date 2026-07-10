plugins {
    id("threadmill.java-module")
    id("threadmill.publish")
}

val springBootVersion =
    rootProject.extensions
        .getByType(VersionCatalogsExtension::class)
        .named("libs")
        .findVersion("springBoot")
        .get()
        .toString()

dependencies {
    api(project(":threadmill-core"))
    api(project(":threadmill-store-memory"))
    // Redis store is compileOnly so apps that do not use Redis do not pull in
    // Lettuce/Netty; ThreadmillRedisAutoConfiguration is gated by
    // @ConditionalOnClass(RedisJobStore.class) so this is safe.
    compileOnly(project(":threadmill-store-redis"))

    // Postgres store is compileOnly so applications that don't need it (or
    // use Redis exclusively) don't pull in the JDBC driver. The auto-config
    // bean for Postgres is gated by @ConditionalOnClass so this is safe.
    compileOnly(project(":threadmill-store-postgres"))
    compileOnly(libs.spring.boot.autoconfigure)
    compileOnly(libs.spring.context)
    compileOnly(libs.spring.jdbc)
    compileOnly(libs.spring.tx)
    annotationProcessor(
        "org.springframework.boot:spring-boot-configuration-processor:$springBootVersion"
    )

    testImplementation(project(":threadmill-store-postgres"))
    testImplementation(project(":threadmill-store-redis"))
    testImplementation(project(":threadmill-test-support"))
    testImplementation(libs.spring.boot.autoconfigure)
    testImplementation(libs.spring.context)
    testImplementation(libs.spring.jdbc)
    testImplementation(libs.spring.tx)
    // The real SB4 DataSourceAutoConfiguration, so the ordering regression
    // can boot a property-configured DataSource instead of a user bean.
    testImplementation(libs.spring.boot.jdbc)
    // The pool the JDBC starter would bring; without one the real
    // DataSourceAutoConfiguration backs off and the ordering test is moot.
    testImplementation(libs.hikaricp)
    testImplementation(libs.postgresql.jdbc)
    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.logback.classic)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj.core)
    testImplementation(libs.awaitility)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.postgresql)
}
