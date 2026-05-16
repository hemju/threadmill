plugins { id("threadmill.java-module") }

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
    api(project(":threadmill-store-redis"))

    // Postgres store is compileOnly so applications that don't need it (or
    // use Redis exclusively) don't pull in the JDBC driver. The auto-config
    // bean for Postgres is gated by @ConditionalOnClass so this is safe.
    compileOnly(project(":threadmill-store-postgres"))
    compileOnly(libs.spring.boot.autoconfigure)
    compileOnly(libs.spring.context)
    compileOnly(libs.spring.tx)
    annotationProcessor(
        "org.springframework.boot:spring-boot-configuration-processor:$springBootVersion"
    )

    testImplementation(project(":threadmill-store-postgres"))
    testImplementation(libs.spring.boot.autoconfigure)
    testImplementation(libs.spring.context)
    testImplementation(libs.spring.tx)
    testImplementation(libs.spring.boot.starter.test)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj.core)
    testImplementation(libs.awaitility)
}
