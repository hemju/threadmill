---
name: release-readiness
description: Prepare and verify a Threadmill release candidate before tagging and publication.
---

# Release Readiness

Use this when preparing a Threadmill release candidate.

1. Set `ThreadmillVersion.CURRENT` to a non-SNAPSHOT release version. The build
   rejects snapshots and requires the Git tag to be exactly `v<version>`.
2. Confirm `LICENSE`, README, docs, example, and `AGENTS.md` are current.
3. Run `./gradlew productionCheck`.
4. Verify that the required dependency scan completed with no unresolved finding.
5. Confirm the git tree is clean before tagging.
6. Use Conventional Commit messages and Threadmill vocabulary.
