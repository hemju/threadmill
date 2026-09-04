# threadmill-test-support

The abstract `JobStore` contract test plus shared fixtures. Every storage
backend extends `AbstractJobStoreContractTest` and is held to the same
76-test suite — that's the only thing guaranteeing all three backends
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

3. Run `./gradlew :threadmill-store-x:test` and pass every test (currently 76)
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
- `JobStoreDecoratorContract` — reflective check for `JobStore` decorators.
  `assertForwardsEveryOperation(decorate)` wraps a recording proxy and
  requires every `JobStore` method — the interface's `default` methods
  included, and any method added to the SPI later — to reach the delegate
  exactly once with the caller's arguments and to return the delegate's
  result. Pair it with the contract suite: the suite proves a decorator does
  not break the store it wraps, this proves it forwards everything.
- `ForwardingJobStore` — deprecated alias for
  `com.hemju.threadmill.core.store.ForwardingJobStore`, the forwarding base
  every decorator (tracing, metrics, test fault injection) now extends.
  Extend the core class directly in new code.

## Build

This module ships test infrastructure only — the abstract contract base,
fixtures, and the decorator contract for the backend and decorator modules'
test source sets. Built as part of every other module's test compile.
