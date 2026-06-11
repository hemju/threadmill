plugins {
    id("threadmill.java-base")
    id("threadmill.publish")
}

val npmInstall by
    tasks.registering(Exec::class) {
        inputs.file("package.json")
        inputs.file("package-lock.json")
        outputs.dir("node_modules")
        commandLine("npm", "ci")
    }

val npmTest by
    tasks.registering(Exec::class) {
        dependsOn(npmInstall)
        inputs.dir("src")
        inputs.file("package.json")
        inputs.file("package-lock.json")
        commandLine("npm", "run", "test")
    }

val npmBuild by
    tasks.registering(Exec::class) {
        dependsOn(npmInstall)
        inputs.dir("src")
        inputs.file("index.html")
        inputs.file("package.json")
        inputs.file("package-lock.json")
        inputs.file("tsconfig.json")
        inputs.file("vite.config.ts")
        outputs.dir("dist")
        commandLine("npm", "run", "build")
    }

tasks.named("check") { dependsOn(npmTest, npmBuild) }

tasks.named<ProcessResources>("processResources") {
    dependsOn(npmBuild)
    from(layout.projectDirectory.dir("dist")) { into("META-INF/resources/threadmill") }
}
