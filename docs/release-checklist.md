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
./gradlew productionCheck
```

`productionCheck` owns the clean-all-projects boundary, every subproject check,
real-store tests, soak and correctness simulations, Javadoc, the example,
dependency scanning, and artifact inspection. If it fails, fix the cause and
re-run it from a clean tree — no failure is acceptable for a release build.

Then inspect artifacts:

```bash
jar tf threadmill-core/build/libs/threadmill-core-*.jar
```

Confirm no test classes, no local development material, no dynamic dependency
versions, no unresolved security findings, and a non-SNAPSHOT release version.
The pull-request, nightly, and tag-release workflows install pinned Node/npm
and the pinned, checksum-verified OSV Scanner, then run
`dependencySecurityScan`. The task scans every Git-tracked Gradle/npm lockfile
and runs `npm audit` at the `low` threshold; either scanner fails the build on
any known advisory. See
[Dependency security](dependency-security.md) for the exact policy,
reachability record, and the strict process for temporary exceptions.

## Publish

[`RELEASING.md`](RELEASING.md) is the canonical publishing runbook. In short:

1. Set and commit the release version.
2. Tag that exact commit with the matching `v<version>` tag.
3. Push `main` and the tag.
4. The GitHub Release workflow validates the tag against the project version,
   runs `productionCheck`, signs the same verified artifacts from that task
   graph, and submits one aggregated bundle through
   `publishAggregationToCentralPortal`.
5. Central Portal validates and publishes the bundle automatically because
   `publishingType` is `AUTOMATIC`.

Do not run the obsolete unconfigured `./gradlew publish` path and do not wait
for a manual Central Portal promotion. If the automated deployment fails,
correct the source and cut a new version; never patch a published release.
