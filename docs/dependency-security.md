# Dependency Security

Threadmill fails closed on known dependency vulnerabilities in pull requests,
nightly scans of `main`, and tagged releases. The Gradle
`dependencySecurityScan` task is the single entry point for this gate.

## Scan inputs and thresholds

The task asks Git for every tracked `*gradle.lockfile` and
`*package-lock.json`, then passes each file explicitly to OSV Scanner. A Git
checkout is required: untracked local lockfiles are intentionally excluded,
and exported source trees fail with an actionable error because they cannot
prove which inputs are release evidence. Git-add any new lockfile so the local
gate includes it; a release run additionally requires the clean, committed tree
specified by the release checklist. Modifications to an already tracked
lockfile are scanned from the working tree.

The input set includes `buildSrc/gradle.lockfile`, which carries the resolved
build-classpath graph. It also includes `settings-gradle.lockfile`; that file
currently contains only Gradle's empty catalog marker, but keeping it in the
enumeration prevents future settings dependencies from escaping the gate. The
task deliberately excludes the version catalog and historical entries in
Gradle checksum-verification metadata.

OSV Scanner fails the build on any advisory, regardless of severity. The same
task runs `npm audit --package-lock-only --audit-level=low`, so every npm
advisory at `low` or above also fails. CI and the tag-release workflow use Node
24.20.0/npm 11.19.0, install OSV Scanner 2.5.0 from its architecture-matched
release binary, verify its SHA-256 checksum, and run the task with scanning
required before testing or publishing.

## Exceptions

There are no exceptions at present. If an advisory cannot immediately be
removed, an exception may be added to `osv-scanner.toml` only after its exact
version and reachable call paths have been reviewed. It must use one
`[[IgnoredVulns]]` entry naming one advisory ID, a concrete reason, and a
near-term `ignoreUntil` date. Package-wide overrides and permanent ignores are
not allowed. The exception must be removed on expiry or as soon as a compatible
fix exists, whichever comes first.

`npm audit` has no ignore mechanism in this repository. A vulnerable npm
dependency must be upgraded, removed, or temporarily pinned to a patched
compatible release through the `overrides` field in `package.json`. An override
must name the affected package and patched version, explain its advisory in the
pull request, and be removed when the direct dependency adopts the fix.

## Checksum metadata

`gradle/verification-metadata.xml` is intentionally additive when dependencies
change. Historical checksums authenticate previously resolved artifact bytes;
they do not approve a version and are not vulnerability-scan inputs. The
committed lockfiles define the current graph, and OSV Scanner evaluates that
graph on every gated run. Regenerating verification metadata from scratch is a
separate trust-policy change because it creates broad, unrelated checksum
churn; do not mix it into a routine dependency upgrade.

## Reachability record

The following assessment was refreshed on 2026-09-04. Re-run it whenever the
dependency graph or the relevant code paths change.

| Dependency surface | Advisories assessed | Reachability and decision |
| --- | --- | --- |
| Lettuce and Netty | `GHSA-4g8c-wm8x-jfhw` (`CVE-2025-24970`), `GHSA-389x-839f-4rhx` (`CVE-2025-25193`), `GHSA-fccg-mwvh-qqg4`, `GHSA-c4c3-7fpv-j4q5` | Lettuce is the Redis backend's runtime client. Netty transport, buffer, resolver, handler, and TLS code are therefore runtime-reachable. In particular, `SslHandler` is reachable for TLS Redis connections, so a malicious or compromised endpoint could exercise TLS parser defects. Windows-local environment-file defects may also be reachable when running on Windows. Server-only SNI, HTTP, CORS, MQTT, SOCKS, and SCTP entry points are not used by Threadmill, but the Netty BOM is still upgraded globally instead of ignoring those advisories. The resolved floor is Lettuce 6.8.2.RELEASE plus Netty 4.1.137.Final. |
| Dashboard JavaScript toolchain | `GHSA-73wf-gq98-2v4g`, `GHSA-c83g-rgw3-j3cx`, `GHSA-w9m9-85wc-3x92` | Vite 7.3.6, Babel 7.29.7, and esbuild 0.28.1 were already patched on `main` before this change. This change removes the remaining audit findings by moving Browserslist to 4.28.9 and PostCSS Selector Parser to 6.1.4. These packages run only during local or CI dashboard builds; the published JAR contains compiled static assets, not the toolchain. Build-time execution is still trusted code execution, so vulnerable transitives are upgraded rather than ignored. |
| Spring test stack | `GHSA-qv9r-c865-cp47`, `GHSA-9xv2-5v5q-p794`, `GHSA-gcx9-497g-6cp6`, `GHSA-h3x4-894j-xpx5`, `GHSA-5gvw-p9qm-jgwh` | Embedded Tomcat and Log4j appear only in test compile/runtime lock entries and are not shipped in Threadmill artifacts. They still execute during CI integration tests. Spring Boot is upgraded to 4.0.8, its Spring Framework/Security pins are aligned, and Tomcat is constrained to the patched 11.0.25 floor rather than treating test scope as an exception. Boot 4.0.8 now resolves Jackson 3.1.5 itself, so the earlier explicit Jackson constraint is removed. |

When evaluating a new report, confirm the resolved version in the committed
lockfile, the affected module and configuration, whether application or build
inputs can reach the vulnerable behavior, and the earliest compatible upstream
fix. Lack of reachability may justify only a short-lived, advisory-specific
exception; it does not justify a package-wide suppression.
