// Publishing for the public library modules. Applied alongside
// `threadmill.java-module` by every artifact we ship to Maven Central; the
// internal modules (example, soak, simulation) deliberately do not apply it.
plugins {
    `java-library`
    `maven-publish`
    signing
}

// A stable JPMS module name so a later jar rename cannot break downstream
// `requires` clauses. Derived deterministically from the artifact id.
tasks.named<Jar>("jar") {
    manifest {
        attributes("Automatic-Module-Name" to "com.hemju." + project.name.replace('-', '.'))
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            pom {
                name.set(project.name)
                description.set(
                    project.description
                        ?: "Threadmill — a modern, lightweight background job-processing library for Java."
                )
                url.set("https://github.com/hemju/threadmill")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        id.set("hemju")
                        name.set("Helmut Michael Juskewycz")
                    }
                }
                scm {
                    connection.set("scm:git:https://github.com/hemju/threadmill.git")
                    developerConnection.set("scm:git:ssh://git@github.com/hemju/threadmill.git")
                    url.set("https://github.com/hemju/threadmill")
                }
            }
        }
    }
}

// Sign published artifacts only when a key is configured (CI / release), using
// an in-memory ASCII-armored key from Gradle properties or environment. Normal
// `./gradlew build` does not sign, so local and unsigned CI builds still work.
val signingKey =
    (findProperty("signingKey") as String?) ?: System.getenv("ORG_GRADLE_PROJECT_signingKey")
val signingPassword =
    (findProperty("signingPassword") as String?)
        ?: System.getenv("ORG_GRADLE_PROJECT_signingPassword")

signing {
    setRequired({
        signingKey != null && gradle.taskGraph.allTasks.any { it.name.startsWith("publish") }
    })
    if (signingKey != null) {
        useInMemoryPgpKeys(signingKey, signingPassword)
        sign(publishing.publications["mavenJava"])
    }
}
