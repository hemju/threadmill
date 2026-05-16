package com.hemju.threadmill.soak.harness;

import com.hemju.threadmill.core.store.JobStore;

/**
 * Lifecycle wrapper for a backend used by one harness run. The runner asks
 * the fixture for a {@link JobStore}, runs the scenario, and closes the
 * fixture in a finally block — which stops Testcontainers, closes JDBC
 * pools, and shuts down Lettuce clients.
 */
public interface BackendFixture extends AutoCloseable {

    JobStore store();

    @Override
    void close();
}
