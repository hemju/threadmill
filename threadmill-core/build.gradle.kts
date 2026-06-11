plugins {
    id("threadmill.java-module")
    id("threadmill.publish")
}

dependencies {
    api(libs.slf4j.api)
    // JSON is the default serializer in this module; consumers may swap in
    // another mapper by providing their own JobSerializer.
    // Jackson is exposed on the public API (JsonJobSerializer(ObjectMapper), JobId/CronExpression
    // annotations), so it must be api, not implementation, or consumers cannot compile
    // against the documented "reuse your ObjectMapper" extension point.
    api(libs.jackson.databind)
    api(libs.jackson.datatype.jsr310)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj.core)
    testImplementation(libs.slf4j.simple)
}
