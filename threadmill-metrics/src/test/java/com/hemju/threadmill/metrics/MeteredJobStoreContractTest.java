package com.hemju.threadmill.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.memory.InMemoryJobStore;
import com.hemju.threadmill.test.AbstractJobStoreContractTest;

/** Runs the shared store contract through the operational metrics decorator. */
class MeteredJobStoreContractTest extends AbstractJobStoreContractTest {

  @Override
  protected JobStore createStore() {
    return new ThreadmillMetrics(new SimpleMeterRegistry(), new InMemoryJobStore()).meteredStore();
  }
}
