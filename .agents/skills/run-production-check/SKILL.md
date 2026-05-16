# Run Production Check

Use this when validating a Threadmill release candidate.

1. Confirm a container runtime is running.
2. Run `./gradlew clean check`.
3. Run `./gradlew :threadmill-store-postgres:test :threadmill-store-redis:test --rerun-tasks`.
4. Run `./gradlew :threadmill-soak:soakRegression`.
5. Run `./gradlew :threadmill-example:run`.
6. Run `./gradlew javadoc productionCheck`.
7. Inspect JUnit XML for skipped Postgres/Redis tests.
8. Inspect main jars with `jar tf`; no test classes or local development files may appear.
