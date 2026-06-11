plugins { id("threadmill.java-module") }

dependencies {
    api(project(":threadmill-core"))

    testImplementation(project(":threadmill-store-memory"))
    testImplementation(project(":threadmill-test-support"))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj.core)
}
