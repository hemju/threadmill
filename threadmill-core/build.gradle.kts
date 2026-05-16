plugins { id("threadmill.java-module") }

dependencies {
    api(libs.slf4j.api)
    // JSON is the default serializer in this module; consumers may swap in
    // another mapper by providing their own JobSerializer.
    implementation(libs.jackson.databind)
    implementation(libs.jackson.datatype.jsr310)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj.core)
    testImplementation(libs.slf4j.simple)
}
