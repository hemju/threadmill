package com.hemju.threadmill.soak.harness;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.redis.RedisJobStore;
import com.hemju.threadmill.store.redis.RedisStoreConfig;

/**
 * Boots a {@code redis:7.4-alpine} Testcontainer with AOF on, matching the
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
 * <p>Sentinel and Cluster use externally provisioned disposable topologies.
 * Sentinel accepts a Lettuce Sentinel URI; Cluster accepts comma-separated
 * Redis seed URIs with identical credentials and TLS settings.
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
    if (!List.of("standalone", "sentinel", "cluster").contains(topology)) {
      throw new IllegalArgumentException("Redis topology must be standalone, sentinel, or cluster");
    }
    if (externalUrl.isEmpty() && !"standalone".equals(topology)) {
      throw new IllegalArgumentException(
          "Sentinel/Cluster soak requires -PredisUrl for a disposable external topology");
    }
    if (externalUrl.isPresent()) {
      this.container = null;
      this.adminClient = null;
      this.adminConnection = null;
      this.store = new RedisJobStore(externalConfig(topology, externalUrl.get()));
      store.dropThreadmillKeys();
    } else {
      this.container = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
          .withExposedPorts(6379)
          .withCommand("redis-server", "--appendonly", "yes")
          .waitingFor(Wait.forListeningPort());
      container.start();
      var uri =
          RedisURI.create("redis://" + container.getHost() + ":" + container.getMappedPort(6379));
      this.adminClient = RedisClient.create(uri);
      this.adminConnection = adminClient.connect();
      adminConnection.sync().flushdb();
      this.store = new RedisJobStore(uri);
    }
  }

  static RedisStoreConfig externalConfig(String topology, String url) {
    if (!"cluster".equals(topology)) {
      var uri = RedisURI.create(url);
      if ("sentinel".equals(topology) != !uri.getSentinels().isEmpty()) {
        throw new IllegalArgumentException(
            "Sentinel topology requires a redis-sentinel URI; standalone requires a data-node URI");
      }
      return new RedisStoreConfig.Standalone(uri);
    }
    var seeds =
        Arrays.stream(url.split(",")).map(String::trim).map(RedisURI::create).toList();
    var first = seeds.getFirst();
    var firstCredentials =
        first.getCredentialsProvider().resolveCredentials().block(Duration.ofSeconds(1));
    for (var seed : seeds) {
      var seedCredentials =
          seed.getCredentialsProvider().resolveCredentials().block(Duration.ofSeconds(1));
      if (seed.getDatabase() != 0
          || !seed.getSentinels().isEmpty()
          || seed.isSsl() != first.isSsl()
          || !Objects.equals(seedCredentials.getUsername(), firstCredentials.getUsername())
          || !Arrays.equals(seedCredentials.getPassword(), firstCredentials.getPassword())) {
        throw new IllegalArgumentException(
            "Cluster seeds must use database 0, identical credentials and TLS, and data-node URIs");
      }
    }
    var credentials = new RedisStoreConfig.Credentials(
        firstCredentials.getUsername(),
        firstCredentials.getPassword() == null ? null : new String(firstCredentials.getPassword()));
    return new RedisStoreConfig.Cluster(
        seeds.stream()
            .map(seed -> new RedisStoreConfig.HostAndPort(seed.getHost(), seed.getPort()))
            .toList(),
        "master",
        credentials,
        first.isSsl() ? RedisStoreConfig.Tls.verified() : RedisStoreConfig.Tls.disabled());
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
