package com.hemju.threadmill.soak.harness;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.redis.RedisJobStore;

/**
 * Boots a {@code redis:7-alpine} Testcontainer with AOF on, matches the
 * durability posture documented for production deployments. The threadmill
 * keyspace is flushed before the run.
 *
 * <p>Topology: standalone only in v1. The {@code -PredisTopology=sentinel|cluster}
 * knob is rejected with a clear "not yet implemented" error so the contract
 * surface is visible from day one — the wiring for those topologies is a
 * follow-up.
 */
public final class RedisHarnessFixture implements BackendFixture {

    @SuppressWarnings("resource")
    private final GenericContainer<?> container;

    private final RedisURI uri;
    private final RedisClient adminClient;
    private final StatefulRedisConnection<String, String> adminConnection;
    private final RedisJobStore store;

    @SuppressWarnings("resource")
    public RedisHarnessFixture(String topology) {
        if (!"standalone".equalsIgnoreCase(topology)) {
            throw new IllegalArgumentException("Redis topology '" + topology
                    + "' is not yet implemented in the soak harness. " + "Only 'standalone' is supported in v1.");
        }
        this.container = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                .withExposedPorts(6379)
                .withCommand("redis-server", "--appendonly", "yes")
                .waitingFor(Wait.forListeningPort());
        container.start();
        this.uri = RedisURI.create("redis://" + container.getHost() + ":" + container.getMappedPort(6379));
        this.adminClient = RedisClient.create(uri);
        this.adminConnection = adminClient.connect();
        adminConnection.sync().flushdb();
        this.store = new RedisJobStore(uri);
    }

    @Override
    public JobStore store() {
        return store;
    }

    @Override
    public void close() {
        try {
            store.close();
        } catch (RuntimeException ignore) {
            // best-effort cleanup
        }
        if (adminConnection != null) adminConnection.close();
        if (adminClient != null) adminClient.shutdown();
        if (container != null && container.isRunning()) container.stop();
    }
}
