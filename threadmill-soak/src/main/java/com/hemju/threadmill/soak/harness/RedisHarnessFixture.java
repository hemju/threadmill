package com.hemju.threadmill.soak.harness;

import java.util.Optional;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.redis.RedisJobStore;

/**
 * Boots a {@code redis:7-alpine} Testcontainer with AOF on, matching the
 * durability posture documented for production deployments — or, if
 * {@code -PredisUrl=redis://host:port} is given, points at an external
 * instance (the docker-compose file shipped with the module, or any
 * long-lived Redis an operator controls). External instances are preferred
 * for endurance runs: they outlive the harness process, so the datastore can
 * be inspected after a failure.
 *
 * <p>The reset semantics differ deliberately: a container this fixture owns
 * is wiped with {@code FLUSHDB}, but an external instance only has the
 * Threadmill namespace removed ({@code {threadmill}:*}) — never a flush that
 * could destroy unrelated keys on shared infrastructure.
 *
 * <p>Topology: standalone only in v1. The {@code -PredisTopology=sentinel|cluster}
 * knob is rejected with a clear "not yet implemented" error so the contract
 * surface is visible from day one — the wiring for those topologies is a
 * follow-up.
 */
public final class RedisHarnessFixture implements BackendFixture {

    @SuppressWarnings("resource")
    private final GenericContainer<?> container;

    private final RedisClient adminClient;
    private final StatefulRedisConnection<String, String> adminConnection;
    private final RedisJobStore store;

    public RedisHarnessFixture(String topology) {
        this(topology, Optional.empty());
    }

    @SuppressWarnings("resource")
    public RedisHarnessFixture(String topology, Optional<String> externalUrl) {
        if (!"standalone".equalsIgnoreCase(topology)) {
            throw new IllegalArgumentException("Redis topology '" + topology
                    + "' is not yet implemented in the soak harness. " + "Only 'standalone' is supported in v1.");
        }
        if (externalUrl.isPresent()) {
            this.container = null;
            this.adminClient = null;
            this.adminConnection = null;
            this.store = new RedisJobStore(RedisURI.create(externalUrl.get()));
            store.dropThreadmillKeys();
        } else {
            this.container = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                    .withExposedPorts(6379)
                    .withCommand("redis-server", "--appendonly", "yes")
                    .waitingFor(Wait.forListeningPort());
            container.start();
            var uri = RedisURI.create("redis://" + container.getHost() + ":" + container.getMappedPort(6379));
            this.adminClient = RedisClient.create(uri);
            this.adminConnection = adminClient.connect();
            adminConnection.sync().flushdb();
            this.store = new RedisJobStore(uri);
        }
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
