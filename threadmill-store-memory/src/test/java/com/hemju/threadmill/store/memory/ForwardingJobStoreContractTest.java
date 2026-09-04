package com.hemju.threadmill.store.memory;

import com.hemju.threadmill.core.store.ForwardingJobStore;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.test.AbstractJobStoreContractTest;

/** Runs the shared store contract through the plain forwarding decorator base. */
class ForwardingJobStoreContractTest extends AbstractJobStoreContractTest {

  @Override
  protected JobStore createStore() {
    return new ForwardingJobStore(new InMemoryJobStore());
  }
}
