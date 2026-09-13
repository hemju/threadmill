package com.hemju.threadmill.store.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.ResourceLock;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.test.AbstractJobStoreContractTest;

/** Full shared contract through a real three-Sentinel, primary/replica topology. */
@ResourceLock("redis-failover-fixed-ports")
class RedisSentinelJobStoreContractTest extends AbstractJobStoreContractTest {
  private static RedisFailoverTopology topology;
  private static RedisClient client;
  private static StatefulRedisConnection<String, String> admin;

  @BeforeAll
  static void startTopology() throws Exception {
    topology = RedisFailoverTopology.start("7.4-alpine", false);
    client = RedisClient.create(
        RedisConnectionConfig.sentinelUri((RedisStoreConfig.Sentinel) topology.config()));
    client.setOptions(RedisClusterOptions.standaloneOptions());
    admin = client.connect();
  }

  @BeforeEach
  void clearNamespace() {
    admin.sync().flushall();
  }

  @AfterAll
  static void stopTopology() {
    if (admin != null) admin.close();
    if (client != null) client.shutdown();
    if (topology != null) topology.close();
  }

  @Override
  protected JobStore createStore() {
    return new RedisJobStore(client);
  }
}
