package com.hemju.threadmill.tracing;

import io.opentelemetry.api.OpenTelemetry;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.memory.InMemoryJobStore;
import com.hemju.threadmill.test.AbstractJobStoreContractTest;

/** Runs the shared store contract through the OpenTelemetry tracing decorator. */
class TracingJobStoreContractTest extends AbstractJobStoreContractTest {

  @Override
  protected JobStore createStore() {
    return ThreadmillTracing.of(OpenTelemetry.noop()).wrapStore(new InMemoryJobStore());
  }
}
