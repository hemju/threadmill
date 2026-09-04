package com.hemju.threadmill.store.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.test.AbstractJobStoreContractTest;

/** Runs the complete shared job-store contract against an actual Redis Cluster connection. */
@ResourceLock("redis-cluster-fixed-ports")
class RedisClusterJobStoreContractTest extends AbstractJobStoreContractTest {

  private static GenericContainer<?> redis;
  private static RedisClusterClient adminClient;
  private static StatefulRedisClusterConnection<String, String> adminConnection;
  private static RedisStoreConfig.Cluster config;

  @BeforeAll
  static void startCluster() throws Exception {
    int hostPort = startContainerWithFreshPort();
    awaitClusterReady();

    var seed = RedisURI.create("redis://localhost:" + hostPort);
    adminClient = RedisClusterClient.create(seed);
    adminConnection = adminClient.connect();
    config = new RedisStoreConfig.Cluster(
        List.of(new RedisStoreConfig.HostAndPort("localhost", hostPort)), "master");
  }

  private static int startContainerWithFreshPort() throws Exception {
    RuntimeException lastFailure = null;
    for (int attempt = 0; attempt < 3; attempt++) {
      int hostPort = availablePort();
      redis = newClusterContainer(hostPort);
      try {
        redis.start();
        var addSlots = redis.execInContainer("redis-cli", "cluster", "addslotsrange", "0", "16383");
        assertThat(addSlots.getExitCode()).as(addSlots.getStderr()).isZero();
        return hostPort;
      } catch (RuntimeException startFailure) {
        lastFailure = startFailure;
        try {
          redis.close();
        } catch (RuntimeException closeFailure) {
          startFailure.addSuppressed(closeFailure);
        }
      }
    }
    throw lastFailure;
  }

  private static GenericContainer<?> newClusterContainer(int hostPort) {
    var redisConfig = """
        bind 0.0.0.0
        protected-mode no
        port 6379
        cluster-enabled yes
        cluster-config-file /tmp/nodes.conf
        cluster-node-timeout 1000
        cluster-announce-ip 127.0.0.1
        cluster-announce-port %d
        cluster-announce-bus-port 16379
        appendonly yes
        maxmemory-policy noeviction
        """.formatted(hostPort);
    return new FixedPortRedisContainer(hostPort)
        .withCopyToContainer(Transferable.of(redisConfig), "/tmp/redis.conf")
        .withCommand("redis-server", "/tmp/redis.conf")
        .waitingFor(Wait.forListeningPort());
  }

  @AfterAll
  static void stopCluster() {
    if (adminConnection != null) adminConnection.close();
    if (adminClient != null) adminClient.shutdown();
    if (redis != null && redis.isRunning()) redis.stop();
  }

  @BeforeEach
  void flushBetweenTests() {
    adminConnection.sync().flushall();
  }

  @AfterEach
  void closeStore() {
    try {
      assertThat(adminConnection.sync().exists(RedisKeys.NO_KEY))
          .as("the optional-key sentinel must never become a real Redis key")
          .isZero();
    } finally {
      if (store instanceof RedisJobStore redisStore) redisStore.close();
    }
  }

  @Override
  protected JobStore createStore() {
    return new RedisJobStore(config);
  }

  private static int availablePort() throws IOException {
    // Cluster announces a concrete reachable port before Testcontainers can map
    // a random one, so this suite serializes its narrow close-to-bind window.
    try (var socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static void awaitClusterReady() throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    String lastInfo = "";
    do {
      var info = redis.execInContainer("redis-cli", "cluster", "info");
      assertThat(info.getExitCode()).as(info.getStderr()).isZero();
      lastInfo = info.getStdout();
      if (lastInfo.contains("cluster_state:ok")) return;
      Thread.sleep(100);
    } while (System.nanoTime() < deadline);
    throw new AssertionError("Cluster did not become ready: " + lastInfo);
  }

  private static final class FixedPortRedisContainer
      extends GenericContainer<FixedPortRedisContainer> {

    private FixedPortRedisContainer(int hostPort) {
      super(DockerImageName.parse("redis:7-alpine"));
      addFixedExposedPort(hostPort, 6379);
    }
  }
}
