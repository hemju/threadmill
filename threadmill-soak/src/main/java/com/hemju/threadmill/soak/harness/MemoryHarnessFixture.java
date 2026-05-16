package com.hemju.threadmill.soak.harness;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

/** Trivial in-memory fixture — no lifecycle, no external resources. */
public final class MemoryHarnessFixture implements BackendFixture {

    private final InMemoryJobStore store = new InMemoryJobStore();

    @Override
    public JobStore store() {
        return store;
    }

    @Override
    public void close() {
        // nothing to release
    }
}
