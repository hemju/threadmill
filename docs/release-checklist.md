# Release Checklist

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
