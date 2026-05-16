# Release Readiness

Use this when preparing a Threadmill release candidate.

1. Confirm the version is non-SNAPSHOT.
2. Confirm `LICENSE`, README, docs, example, and `AGENTS.md` are current.
3. Run `./gradlew productionCheck`.
4. Verify dependency scan output or record why the scanner was unavailable.
5. Confirm the git tree is clean before tagging.
6. Use Conventional Commit messages and Threadmill vocabulary.
