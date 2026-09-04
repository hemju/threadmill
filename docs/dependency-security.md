# Dependency Security

Threadmill fails closed on known dependency vulnerabilities in pull requests
and tagged releases. The Gradle `dependencySecurityScan` task is the single
entry point for this gate.

## Scan inputs and thresholds

The task asks Git for every tracked `*gradle.lockfile` and
`*package-lock.json`, then passes each file explicitly to OSV Scanner. This
includes `settings-gradle.lockfile`; it deliberately excludes the version
catalog and historical entries in Gradle checksum-verification metadata. Only
the resolved, committed dependency graph is release evidence.

OSV Scanner fails the build on any advisory, regardless of severity. The same
task runs `npm audit --package-lock-only --audit-level=low`, so every npm
advisory at `low` or above also fails. CI and the tag-release workflow install
OSV Scanner 2.5.0 from its release binary, verify its SHA-256 checksum, and run
the task with scanning required before testing or publishing.

## Exceptions

There are no exceptions at present. If an advisory cannot immediately be
removed, an exception may be added to `osv-scanner.toml` only after its exact
version and reachable call paths have been reviewed. It must use one
`[[IgnoredVulns]]` entry naming one advisory ID, a concrete reason, and a
near-term `ignoreUntil` date. Package-wide overrides and permanent ignores are
not allowed. The exception must be removed on expiry or as soon as a compatible
fix exists, whichever comes first.

`npm audit` has no ignore mechanism in this repository. A vulnerable npm
dependency must be upgraded, overridden to a patched compatible version, or
removed.

## Reachability record

The following assessment was refreshed on 2026-09-04. Re-run it whenever the
dependency graph or the relevant code paths change.

| Dependency surface | Reachability and decision |
| --- | --- |
| Lettuce and Netty | Lettuce is the Redis backend's runtime client. Netty transport, buffer, resolver, handler, and TLS code are therefore runtime-reachable. In particular, `SslHandler` is reachable for TLS Redis connections, so a malicious or compromised endpoint could exercise TLS parser defects. Windows-local environment-file defects may also be reachable when running on Windows. Server-only SNI, HTTP, CORS, MQTT, SOCKS, and SCTP entry points are not used by Threadmill, but the Netty BOM is still upgraded globally instead of ignoring those advisories. The resolved floor is Lettuce 6.8.2.RELEASE plus Netty 4.1.137.Final. |
| Dashboard JavaScript toolchain | Vite 7.3.6, Babel 7.29.7, esbuild 0.28.1, Browserslist 4.28.9, PostCSS Selector Parser 6.1.4, and related packages run only during local or CI dashboard builds; the published JAR contains compiled static assets, not the toolchain. Build-time execution is still trusted code execution, so vulnerable transitive packages are upgraded rather than ignored. |
| Spring test stack | Embedded Tomcat and Log4j appear only in test compile/runtime lock entries and are not shipped in Threadmill artifacts. They still execute during CI integration tests. Spring Boot is upgraded to 4.0.8, its Spring Framework/Security pins are aligned, and Tomcat is constrained to the patched 11.0.25 floor rather than treating test scope as an exception. |

When evaluating a new report, confirm the resolved version in the committed
lockfile, the affected module and configuration, whether application or build
inputs can reach the vulnerable behavior, and the earliest compatible upstream
fix. Lack of reachability may justify only a short-lived, advisory-specific
exception; it does not justify a package-wide suppression.
