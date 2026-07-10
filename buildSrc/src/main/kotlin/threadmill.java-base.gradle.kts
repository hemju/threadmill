plugins { `java-library` }

group = "com.hemju.threadmill"

version = "0.1.2"

java {
    toolchain { languageVersion.set(JavaLanguageVersion.of(25)) }
    withSourcesJar()
    withJavadocJar()
}

repositories { mavenCentral() }

dependencyLocking { lockAllConfigurations() }

tasks.named<Jar>("jar") {
    from(rootProject.file("LICENSE")) { into("META-INF") }
    from(rootProject.file("NOTICE")) { into("META-INF") }
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(25)
    options.encoding = "UTF-8"
    options.compilerArgs.addAll(
        listOf("-Xlint:all", "-Werror", "-Xlint:-serial", "-Xlint:-processing")
    )
}

tasks.withType<Javadoc>().configureEach {
    options.encoding = "UTF-8"
    isFailOnError = true
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:all,-missing", "-quiet")
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    testLogging {
        events("failed")
        showStandardStreams = false
        showStackTraces = true
        showExceptions = true
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
