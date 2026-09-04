# Contributing to Threadmill

Thanks for taking the time to improve Threadmill.

Threadmill is a Java 25 background-job-processing library with an
at-least-once delivery model. Contributions should preserve the core
invariants documented in [AGENTS.md](AGENTS.md): one central state machine,
store-backed behavior that is covered by the shared contract suite, and no
storage or framework code in `threadmill-core`.

## Development Requirements

- Use Java 25.
- Use Node.js 24.20.0 and npm 11.19.0 when changing the dashboard, matching CI.
- Use the committed Gradle wrapper: `./gradlew`.
- When changing a Gradle dependency, regenerate and commit dependency locks and
  checksum verification metadata with
  `./gradlew dependencies --write-locks --write-verification-metadata sha256`.
- Keep handlers, docs, and examples clear that jobs can run more than once and
  handlers must be idempotent.
- Do not commit private local notes, generated build output, IDE metadata, or
  datastore artifacts.

## Before Opening a Pull Request

Run:

```bash
./gradlew check
```

For changes touching PostgreSQL, Redis, store contracts, scheduling, claiming,
or maintenance behavior, also run the relevant real-backend tests:

```bash
./gradlew :threadmill-store-postgres:test :threadmill-store-redis:test --rerun-tasks
```

These tests use Testcontainers and require a working container runtime.

For dependency changes, install OSV Scanner and run the same fail-closed gate
as CI:

```bash
./gradlew dependencySecurityScan -PdependencyScanRequired=true
```

If the gate reports an advisory, follow the thresholds, reachability review,
and temporary-exception rules in
[Dependency security](docs/dependency-security.md). Never add a package-wide or
permanent ignore to make a pull request green.

For release-sensitive or production-behavior changes, run:

```bash
./gradlew productionCheck
```

## Pull Request Expectations

- Include tests for behavioral changes and permanent regression tests for bugs.
- Update docs and Javadoc for user-facing API or operational changes.
- Keep changes focused; avoid unrelated refactors in the same PR.
- Use the project vocabulary from [AGENTS.md](AGENTS.md).
- Follow Conventional Commits for commit messages when practical, for example
  `fix(core): preserve job version after stale save`.

## License

By contributing, you agree that your contributions are licensed under the
Apache License 2.0, the same license as this repository.
