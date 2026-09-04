---
name: run-production-check
description: Run and assess Threadmill's complete production-readiness validation gate.
---

# Run Production Check

Use this when validating a Threadmill release candidate.

1. Confirm a container runtime is running.
2. Run `./gradlew productionCheck`. This task owns the clean-all-projects
   boundary, every subproject check, Javadoc, real-store tests, the fixed soak
   suite, correctness and nudge simulations, the example, dependency scanning,
   and artifact inspection.
3. Inspect JUnit XML for skipped Postgres/Redis tests.
4. Inspect main jars with `jar tf`; no test classes or local development files may appear.
5. Treat every failure as release-blocking. Fix the cause and rerun the complete
   `productionCheck`; do not replace it with an itemized subset.
