# Add Store Regression

Use this when turning a store bug into permanent coverage.

1. If the behavior is required of every backend, add the test to
   `threadmill-test-support`'s abstract `JobStore` contract.
2. If the behavior is backend-specific, add a named regression in that store's
   real integration test class.
3. Run memory, Postgres, and Redis store tests when the contract changes.
4. Keep the test name tied to the failure mode, not the implementation detail.
