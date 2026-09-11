# Releasing Threadmill

This runbook covers merging a qualified candidate, publishing the complete
Threadmill 1.0.0 artifact set to Maven Central, and creating its GitHub release.
A release tag triggers publication automatically; create it only after every
qualification and release check has passed.

## Publication prerequisites

The published namespace is `com.hemju.threadmill`. Keep it consistent with
existing releases and verify that the release account can publish to that
namespace in [Sonatype Central Portal](https://central.sonatype.com/publishing/namespaces).

The `Release` workflow in `.github/workflows/release.yml` uses the GitHub
`release` environment and these repository or environment secrets:

| Secret | Purpose |
|---|---|
| `SIGNING_KEY` | ASCII-armored private PGP signing key |
| `SIGNING_PASSWORD` | Signing-key passphrase |
| `CENTRAL_PORTAL_USERNAME` | Central Portal user-token name |
| `CENTRAL_PORTAL_PASSWORD` | Central Portal user-token secret |

Check secret availability and any environment approval requirements before
tagging. Do not print or copy secret values into logs or release notes. The
signing public key must be available to Central Portal. Credentials are passed
to Gradle as `ORG_GRADLE_PROJECT_*` properties; per-module POM metadata and
signing are configured by `threadmill.publish` in `buildSrc`.

## Qualify and merge the candidate

1. Complete the [1.0 soak plan](soak-plan-1.0.md) and review correctness,
   performance and stability separately. Preserve candidate/runtime hashes,
   baseline comparisons, fault recovery, raw counter reconciliation and final
   datastore snapshots. Interrupted runs are incomplete. Explain or fix every
   outlier and unexplained resource-growth trend before sign-off; do not weaken
   acceptance thresholds after a run. Record the exact qualified versions and
   topologies, and do not present a short topology test as hours-scale evidence.
2. Resolve release-blocking issues and review feedback, update documentation and
   the changelog, and run the complete production gate with dependency scanning
   required. Any correctness fix needs fresh relevant soak qualification.
3. Confirm that the PR head matches the reviewed candidate, all required CI
   checks pass, and there are no unresolved blocking reviews or merge conflicts.
   Merge through the PR. If the resulting source differs from the qualified
   source beyond reviewed documentation/version metadata, assess and rerun the
   affected qualification before proceeding.

## Prepare the release commit

1. On `main`, set `ThreadmillVersion.CURRENT` in
   `buildSrc/src/main/kotlin/com/hemju/threadmill/gradle/ThreadmillVersion.kt`
   to `"1.0.0"`. Every published module must use that same non-SNAPSHOT version.
2. Confirm the README, Spring quickstart and module installation examples all
   use `1.0.0`. Preserve historical versions in the changelog and frozen 0.3.0
   migration fixtures. Finalize the compatibility guide, commercial-support
   wording and project status. Change the candidate notices to release wording
   and date the 1.0.0 changelog entry when cutting the release.
3. Run formatting, then the complete gate and tag/version validation:

   ```sh
   ./gradlew spotlessApply
   ./gradlew productionCheck verifyReleaseTag \
     -PreleaseTag=v1.0.0 -PdependencyScanRequired=true
   ```

   Review the reports for failures or skipped real-store tests. Confirm the
   example, browser tests, simulations, dependency scans, Javadoc and artifact
   inspection passed. Check all eleven published modules, their POMs and
   intra-Threadmill dependency versions. Binary JARs must contain
   `META-INF/LICENSE` and `META-INF/NOTICE`, with no test or private local files.
4. Commit any remaining release preparation using a Conventional Commit such
   as `chore(release): prepare 1.0.0`. Require a clean working tree and record the
   final commit and the relationship to the qualified runtime. Keep qualification
   artifacts outside build directories because `productionCheck` cleans outputs.

## Tag and publish

Confirm `v1.0.0` does not already exist locally or remotely. Tag the verified
commit, then push only `main` and the intended release tag:

```sh
git tag -a v1.0.0 -m "Threadmill 1.0.0"
git push origin main
git push origin refs/tags/v1.0.0
```

The tag push triggers the `Release` workflow. It validates that the tag equals
`v` plus every published module's version, runs `productionCheck` from clean
outputs, and signs and uploads the same verified artifacts as one aggregated
bundle through `publishAggregationToCentralPortal`. Central Portal validates
and publishes automatically because `publishingType` is `AUTOMATIC`.

Watch the workflow to completion and verify the deployment in
[Central Portal](https://central.sonatype.com/publishing/deployments). Confirm all
eleven modules at version 1.0.0 are retrievable from Maven Central, including
POMs, binary/source/Javadoc JARs and signatures. Resolve the README's installation
coordinates from a fresh consumer project on Java 25.

The workflow publishes Maven artifacts; it does not create the GitHub release.
After verifying publication, create the GitHub release for the existing
`v1.0.0` tag with reviewed notes based on `CHANGELOG.md`, a prominent
[0.3.0 upgrade guide](compatibility.md#upgrade-from-v030), supported platform
requirements, the at-least-once guarantee, and the commercial-support contact.
Use a notes file with actual newlines. Keep private operational evidence and
credentials out of public notes.

Confirm GitHub and Maven Central reference the intended version before closing
release issues and cleaning up merged branches/worktrees. Preserve all soak
artifacts, backups and frozen runtimes. Do not automatically invent a next
version or move an existing release tag.

## Failure and local inspection

If publication fails, determine whether any version became public before
retrying. Never overwrite published coordinates or retarget a released tag;
fix source changes in a new version. Do not use direct per-module Central tasks
or the obsolete unconfigured `./gradlew publish` path.

`./gradlew publishToMavenLocal` can inspect unsigned artifacts in a local Maven
repository. This does not run the complete release gate and does not establish
public availability. To inspect aggregation locally, use the configured
`nmcpZipAggregation` task and inspect its output under `build/`; it is not
release qualification or an upload.
