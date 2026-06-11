plugins {
    id("threadmill.java-module")
    id("threadmill.publish")
}

dependencies {
    api(project(":threadmill-core"))
    api(platform(libs.junit.bom))
    api(libs.junit.jupiter)
    api(libs.assertj.core)
    api(libs.awaitility)
}
