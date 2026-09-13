package com.hemju.threadmill.store.redis;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lettuce.core.RedisURI;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.JobEngineFatalException;

class RedisVersionGateTest {
  @Test
  void redisBefore74FailsAtStartupInsteadOfFailingTheFirstClaim() {
    try (var redis =
        new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine")).withExposedPorts(6379)) {
      redis.start();
      var uri = RedisURI.create("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
      assertThatThrownBy(() -> {
            try (var store = new RedisJobStore(uri)) {
              store.describe();
            }
          })
          .isInstanceOf(JobEngineFatalException.class)
          .hasMessageContaining("Redis 7.4 or later");
    }
  }
}
