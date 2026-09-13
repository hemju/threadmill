package com.hemju.threadmill.store.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.test.AbstractJobStoreContractTest;
import com.hemju.threadmill.test.ClaimPoisonRegression;

/**
 * Runs the {@link AbstractJobStoreContractTest} against real Redis via
 * Testcontainers. The exact same contract tests the in-memory and PostgreSQL
 * stores pass must also pass here.
 */
class RedisJobStoreContractTest extends AbstractJobStoreContractTest {

  @SuppressWarnings("resource")
  private static final GenericContainer<?> REDIS = new GenericContainer<>(
          DockerImageName.parse("redis:7.4-alpine"))
      .withExposedPorts(6379)
      .withCommand("redis-server", "--appendonly", "yes")
      .waitingFor(Wait.forListeningPort());

  private static RedisURI uri;
  private static RedisClient adminClient;
  private static StatefulRedisConnection<String, String> adminConnection;

  @BeforeAll
  static void startContainer() {
    REDIS.start();
    uri = RedisURI.create("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
    adminClient = RedisClient.create(uri);
    adminConnection = adminClient.connect();
  }

  @AfterAll
  static void stopContainer() {
    if (adminConnection != null) adminConnection.close();
    if (adminClient != null) adminClient.shutdown();
    if (REDIS.isRunning()) REDIS.stop();
  }

  @BeforeEach
  void flushBetweenTests() {
    adminConnection.sync().flushdb();
  }

  @Test
  void poisonSerializationDoesNotDiscardEarlierClaims() {
    try (var faulty =
        new RedisJobStore(adminClient, ClaimPoisonRegression.serializer(), store.capabilities())) {
      ClaimPoisonRegression.verify(faulty);
    }
  }

  @Override
  protected JobStore createStore() {
    return new RedisJobStore(uri);
  }
}
