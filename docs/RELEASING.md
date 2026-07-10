# Releasing Threadmill

This is the maintainer runbook for cutting a public release and publishing
artifacts to Maven Central. It assumes the one-time setup in
[§1](#1-one-time-setup) is already done.

Artifacts are published to the **Sonatype Central Portal**
(<https://central.sonatype.com>) — the successor to the retired OSSRH staging
API — via the [`nmcp`](https://gradleup.com/nmcp/) aggregation plugin wired in
the root `build.gradle.kts`. Per-module POM metadata and PGP signing live in the
`threadmill.publish` convention plugin (`buildSrc/`).

---

## 1. One-time setup

### 1.1 Namespace verification (do this first — it can force a group-id change)

The published coordinate group is **`com.hemju.threadmill`** (see
`buildSrc/src/main/kotlin/threadmill.java-base.gradle.kts`). Central Portal will
not accept a bundle until the namespace is verified to you:

- **`com.hemju`** requires proving control of the domain **`hemju.com`** by
  adding a DNS `TXT` record that Central Portal generates. Use this only if you
  own `hemju.com`.
- **If you do not own `hemju.com`**, switch the group to **`io.github.hemju`**,
  which Central Portal verifies by having you create a throwaway public GitHub
  repo with a generated name. This changes only the Maven *coordinates*, not the
  Java package names (`com.hemju.threadmill.*` stay as-is). To switch:
  - edit `group = "com.hemju.threadmill"` → `group = "io.github.hemju"` in
    `buildSrc/src/main/kotlin/threadmill.java-base.gradle.kts`;
  - update the coordinates in `README.md`, `docs/quickstart.md`, and
    `threadmill-spring-boot/README.md` (search for `com.hemju.threadmill:`).

Register/verify the namespace at
<https://central.sonatype.com/publishing/namespaces>.

### 1.2 Central Portal user token

In Central Portal → **Account → Generate User Token**. This yields a
`username` / `password` pair (NOT your login). These map to the Gradle
properties `centralPortalUsername` / `centralPortalPassword`.

### 1.3 PGP signing key

Central Portal requires every artifact to be signed.

```sh
# Generate a key (RSA 4096, no expiry) if you don't have one:
gpg --full-generate-key
# Find its id and publish the public half to a keyserver Central Portal checks:
gpg --list-secret-keys --keyid-format=long
gpg --keyserver keyserver.ubuntu.com --send-keys <KEY_ID>
# Export the ASCII-armored PRIVATE key (this whole block is the secret):
gpg --armor --export-secret-keys <KEY_ID>
```

### 1.4 GitHub Actions secrets

In the repo → **Settings → Secrets and variables → Actions**, add:

| Secret | Value |
|---|---|
| `SIGNING_KEY` | the full ASCII-armored **private** key block from §1.3 |
| `SIGNING_PASSWORD` | passphrase for that key |
| `CENTRAL_PORTAL_USERNAME` | user-token name from §1.2 |
| `CENTRAL_PORTAL_PASSWORD` | user-token secret from §1.2 |

The `Release` workflow (`.github/workflows/release.yml`) reads these and passes
them to Gradle as `ORG_GRADLE_PROJECT_*` properties. Consider putting them in a
GitHub Environment named `release` with required reviewers for an approval gate.

---

## 2. Cut a release

1. Make sure `main` is green (the `CI` workflow runs `./gradlew check` on every
   push and PR) and `CHANGELOG.md` is updated.
2. Set the version in
   `buildSrc/src/main/kotlin/threadmill.java-base.gradle.kts`
   (e.g. `version = "0.1.0"`). Releases must not be `-SNAPSHOT`.
3. Commit, tag, and push the tag:
   ```sh
   git commit -am "release: v0.1.0"
   git tag v0.1.0
   git push origin main --tags
   ```
4. The tag push triggers `.github/workflows/release.yml`, which runs
   `./gradlew publishAggregationToCentralPortal`. This signs all 11 published
   modules, assembles one bundle, and uploads it to the Central Portal.
5. The build uses `publishingType = "AUTOMATIC"`, so the deployment is
   validated and then **published to Maven Central automatically** — no manual
   click. Sync to `repo.maven.apache.org` takes ~15–30 min; the search UI can
   lag a few hours. Track the deployment at
   <https://central.sonatype.com/publishing/deployments>.
   - To gate releases behind a manual review instead, change `publishingType`
     back to `"USER_MANAGED"` in the root `build.gradle.kts`; the deployment
     then waits for a **Publish** click in the Central Portal UI.
6. Bump the version back to the next `-SNAPSHOT`/`rc` on `main`.

### Local dry-run (optional)

You can exercise everything except the upload without credentials:

```sh
./gradlew publishToMavenLocal   # installs all modules to ~/.m2 (unsigned)
```

Dependency locks and SHA-256 verification metadata are enforced during this
build. Each binary JAR must contain `META-INF/LICENSE` and `META-INF/NOTICE`;
the release-candidate `artifactInspection` task checks both files.

To build the exact bundle that would be uploaded (needs a signing key + dummy
Central Portal props), run `./gradlew zipAggregation` and inspect the zip under
`build/`.

---

## 3. Making the repository public (first release only)

The repo starts private. Before flipping it public, scrub internal-only files
from history (see the pre-publication checklist handed off separately), then:

```sh
gh repo edit hemju/threadmill --visibility public --accept-visibility-change-consequences
```

Set the repo description and topics while you're there:

```sh
gh repo edit hemju/threadmill \
  --description "Modern, lightweight background job-processing library for Java 25" \
  --add-topic java --add-topic jobs --add-topic background-jobs \
  --add-topic postgresql --add-topic redis --add-topic scheduler
```
