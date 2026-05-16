# threadmill-test-support

The abstract `JobStore` contract test plus shared fixtures. Every storage
backend extends `AbstractJobStoreContractTest` and is held to the same
61-test suite — that's the only thing guaranteeing all three backends
behave identically.

## How to add a new backend

1. Implement `JobStore` in your new module.
2. Add an integration test class in your module's test source set:

   ```java
   class XJobStoreContractTest extends AbstractJobStoreContractTest {
       @Override protected JobStore createStore() {
           return new XJobStore(/* … */);
       }

       @Override protected void tearDownStore() {
           // optional: release resources between tests
       }
   }
   ```

3. Run `./gradlew :threadmill-store-x:test` and pass every test (currently 61)
   before adding any backend-specific tests.
4. Add backend-specific tests in `XJobStoreRegressionTest`. For every
   correctness lesson learned during development, add a named regression
   test and a row to `AGENTS.md` §11.

## What's in here

- `AbstractJobStoreContractTest` — the contract suite (insert, saveAtomic,
  claim, dedup, concurrency groups, workflow inheritance, replacement,
  mutexes, queue pauses, bulk insert, …).
- `Jobs` — tiny factory for the jobs the contract tests build. Keeps the
  tests focused on the contract rather than on how to construct a job.

## Build

This module has no production sources — it ships the abstract base for the
backend modules' test source sets to extend. Built as part of every other
module's test compile.
