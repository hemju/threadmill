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

Then inspect artifacts:

```bash
jar tf threadmill-core/build/libs/threadmill-core-*.jar
```

Confirm no test classes, no local development material, no dynamic dependency
versions, no unresolved security findings, and a non-SNAPSHOT release version.
