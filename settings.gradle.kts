rootProject.name = "threadmill"

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement { repositories { mavenCentral() } }

include(
    "threadmill-core",
    "threadmill-store-memory",
    "threadmill-store-postgres",
    "threadmill-store-redis",
    "threadmill-spring-boot",
    "threadmill-test-support",
    "threadmill-metrics",
    "threadmill-tracing",
    "threadmill-dashboard",
    "threadmill-soak",
    "threadmill-simulation",
    "threadmill-example",
)
