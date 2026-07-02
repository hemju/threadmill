# Release Checklist

Before publishing the repository:

- Confirm `LICENSE` is the Apache License 2.0 and the README license section
  points to it.
- Confirm `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`, and the
  `.github` issue / pull-request templates are present.
- Confirm the README still states the at-least-once delivery guarantee, Java 25
  requirement, PostgreSQL 18+ requirement, Redis AOF durability note, and
  Testcontainers requirement for real backend tests.

Run from a clean git tree:

```bash
./gradlew clean check
./gradlew :threadmill-store-postgres:test :threadmill-store-redis:test --rerun-tasks
./gradlew :threadmill-soak:soakRegression
./gradlew :threadmill-example:run
./gradlew javadoc
./gradlew productionCheck
```

If `productionCheck` (or any step above) fails, fix the cause and re-run the
whole sequence from a clean tree — no failure is acceptable for a release
build.

Then inspect artifacts:

```bash
jar tf threadmill-core/build/libs/threadmill-core-*.jar
```

Confirm no test classes, no local development material, no dynamic dependency
versions, no unresolved security findings, and a non-SNAPSHOT release version.

## Publish

Publishing is configured by the `threadmill.publish` convention plugin
(`buildSrc/src/main/kotlin/threadmill.publish.gradle.kts`). Every public
library module applies it; the internal modules (`threadmill-example`,
`threadmill-soak`, `threadmill-simulation`) deliberately do not, so they are
never shipped.

1. **Dry-run locally.** `./gradlew publishToMavenLocal` publishes every
   library module to `~/.m2/repository`. Inspect the POMs there (name,
   description, license, SCM, developer info are populated by the plugin)
   and the jars — each carries a stable `Automatic-Module-Name` derived from
   the artifact id.
2. **Configure signing.** Release artifacts are signed with an in-memory
   ASCII-armored GPG key supplied via the `signingKey` / `signingPassword`
   Gradle properties, or equivalently the `ORG_GRADLE_PROJECT_signingKey` /
   `ORG_GRADLE_PROJECT_signingPassword` environment variables. Signing is
   skipped automatically when no key is configured — that keeps local and
   unsigned CI builds working, but it also means an unsigned publish will not
   fail loudly. Verify the key is present in the release environment before
   publishing: Maven Central rejects unsigned deployments.
3. **Publish.** `./gradlew publish` publishes the signed `mavenJava`
   publication of every library module to the configured release repository.
4. **Stage and promote on Maven Central.** Releases go through the Central
   Portal (<https://central.sonatype.com>). After the upload, check that the
   staged deployment passes the portal's validation (signatures, POM
   completeness, sources / javadoc jars), then promote it. Nothing is public
   until the deployment is promoted; a failed validation is fixed locally and
   re-published — never patched in place.
5. **Tag the release.** Tag the exact commit the artifacts were built from
   with the published version, `v`-prefixed (`v0.1.0-rc.1` style), and push
   the tag:

   ```bash
   git tag -a v0.1.0-rc.1 -m "Threadmill 0.1.0-rc.1"
   git push origin v0.1.0-rc.1
   ```
