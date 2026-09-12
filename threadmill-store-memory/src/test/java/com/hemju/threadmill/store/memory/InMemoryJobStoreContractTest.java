package com.hemju.threadmill.store.memory;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.test.AbstractJobStoreContractTest;
import com.hemju.threadmill.test.ClaimPoisonRegression;

/**
 * Runs the full {@link AbstractJobStoreContractTest} against the in-memory
 * store. No backend-specific behaviour is exercised here; if a test in this
 * class fails, the contract or the store is wrong, never the test.
 */
class InMemoryJobStoreContractTest extends AbstractJobStoreContractTest {

  @Test
  void poisonSerializationDoesNotDiscardEarlierClaims() {
    ClaimPoisonRegression.verify(
        new InMemoryJobStore(ClaimPoisonRegression.serializer(), store.capabilities()));
  }

  @Override
  protected JobStore createStore() {
    return new InMemoryJobStore();
  }
}
