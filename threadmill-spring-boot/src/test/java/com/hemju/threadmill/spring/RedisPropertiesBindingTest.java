package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import io.lettuce.core.SslVerifyMode;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import com.hemju.threadmill.store.redis.RedisStoreConfig;

class RedisPropertiesBindingTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner().withUserConfiguration(PropertiesConfiguration.class);

  @Test
  void clusterAclAndTlsPropertiesBindIntoTheStoreConfiguration() {
    runner
        .withPropertyValues(
            "threadmill.store.redis.mode=cluster",
            "threadmill.store.redis.cluster.nodes[0]=redis.example:6380",
            "threadmill.store.redis.cluster.username=cluster-user",
            "threadmill.store.redis.cluster.password=cluster-password",
            "threadmill.store.redis.cluster.tls=true",
            "threadmill.store.redis.cluster.verify-mode=none")
        .run(context -> {
          assertThat(context).hasNotFailed();
          var redis = context.getBean(ThreadmillProperties.class).getStore().getRedis();

          assertThat(ThreadmillRedisAutoConfiguration.redisStoreConfig(redis))
              .isEqualTo(new RedisStoreConfig.Cluster(
                  List.of(new RedisStoreConfig.HostAndPort("redis.example", 6380)),
                  "master",
                  new RedisStoreConfig.Credentials("cluster-user", "cluster-password"),
                  RedisStoreConfig.Tls.unverified()));
        });
  }

  @Test
  void sentinelDataAndControlPlaneSecurityPropertiesRemainDistinct() {
    runner
        .withPropertyValues(
            "threadmill.store.redis.mode=sentinel",
            "threadmill.store.redis.sentinel.master-name=threadmill-master",
            "threadmill.store.redis.sentinel.nodes[0]=sentinel.example:26380",
            "threadmill.store.redis.sentinel.data-node-username=data-user",
            "threadmill.store.redis.sentinel.data-node-password=data-password",
            "threadmill.store.redis.sentinel.sentinel-username=sentinel-user",
            "threadmill.store.redis.sentinel.sentinel-password=sentinel-password",
            "threadmill.store.redis.sentinel.tls=true",
            "threadmill.store.redis.sentinel.verify-mode=full")
        .run(context -> {
          assertThat(context).hasNotFailed();
          var redis = context.getBean(ThreadmillProperties.class).getStore().getRedis();

          assertThat(ThreadmillRedisAutoConfiguration.redisStoreConfig(redis))
              .isEqualTo(new RedisStoreConfig.Sentinel(
                  "threadmill-master",
                  List.of(new RedisStoreConfig.HostAndPort("sentinel.example", 26380)),
                  new RedisStoreConfig.Credentials("data-user", "data-password"),
                  new RedisStoreConfig.Credentials("sentinel-user", "sentinel-password"),
                  RedisStoreConfig.Tls.verified()));
        });
  }

  @Test
  void caOnlyVerificationModeBindsForClusterTls() {
    runner
        .withPropertyValues(
            "threadmill.store.redis.mode=cluster",
            "threadmill.store.redis.cluster.nodes[0]=redis.example:6380",
            "threadmill.store.redis.cluster.tls=true",
            "threadmill.store.redis.cluster.verify-mode=ca")
        .run(context -> {
          assertThat(context).hasNotFailed();
          var redis = context.getBean(ThreadmillProperties.class).getStore().getRedis();
          var config =
              (RedisStoreConfig.Cluster) ThreadmillRedisAutoConfiguration.redisStoreConfig(redis);

          assertThat(config.tls().verifyMode()).isEqualTo(SslVerifyMode.CA);
        });
  }

  @Test
  void nonDefaultVerificationWithoutTlsFailsValidation() {
    runner
        .withPropertyValues(
            "threadmill.store.redis.mode=cluster",
            "threadmill.store.redis.cluster.nodes[0]=redis.example:6379",
            "threadmill.store.redis.cluster.verify-mode=none")
        .run(context -> {
          var redis = context.getBean(ThreadmillProperties.class).getStore().getRedis();
          assertThatThrownBy(() -> ThreadmillRedisAutoConfiguration.redisStoreConfig(redis))
              .isInstanceOf(IllegalArgumentException.class)
              .hasMessage("non-default TLS verification requires TLS to be enabled");
        });
  }

  @Configuration(proxyBeanMethods = false)
  @EnableConfigurationProperties(ThreadmillProperties.class)
  static class PropertiesConfiguration {}
}
