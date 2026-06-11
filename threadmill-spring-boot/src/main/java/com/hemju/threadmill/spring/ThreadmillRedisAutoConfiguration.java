package com.hemju.threadmill.spring;

import java.util.List;
import java.util.Locale;

import io.lettuce.core.RedisURI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;

import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.redis.RedisJobStore;
import com.hemju.threadmill.store.redis.RedisStoreConfig;

/**
 * Redis {@link JobStore} auto-configuration, contributed only when the Redis
 * store is on the classpath ({@code @ConditionalOnClass(RedisJobStore.class)})
 * and {@code threadmill.store.redis.*} is configured. Kept in its own
 * configuration — mirroring {@link ThreadmillPostgresAutoConfiguration} — so
 * the Redis store module (and its Lettuce/Netty transitive dependencies) is a
 * {@code compileOnly} dependency of {@code threadmill-spring-boot}: applications
 * that don't use Redis don't pull it onto their runtime classpath.
 *
 * <p>Sorts before {@link ThreadmillAutoConfiguration} so the Redis store bean
 * wins over the in-memory fallback; Postgres defers to Redis via
 * {@link OnRedisStoreNotConfigured}, so the two store configs are mutually
 * exclusive on the redis-configured property.
 */
@AutoConfiguration
@AutoConfigureBefore(ThreadmillAutoConfiguration.class)
@ConditionalOnClass(RedisJobStore.class)
public class ThreadmillRedisAutoConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadmillRedisAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean(JobStore.class)
    @Conditional(OnRedisStoreConfigured.class)
    public JobStore threadmillJobStore(ThreadmillProperties properties) {
        var redis = properties.getStore().getRedis();
        if (redis.isResetOnStart() && !redis.isAllowDestructiveReset()) {
            throw new IllegalStateException("threadmill.store.redis.reset-on-start=true requires"
                    + " threadmill.store.redis.allow-destructive-reset=true");
        }
        LOG.info("Threadmill: using Redis store");
        var store = new RedisJobStore(redisStoreConfig(redis));
        if (redis.isResetOnStart()) {
            store.dropThreadmillKeys();
        }
        return store;
    }

    private static RedisStoreConfig redisStoreConfig(ThreadmillProperties.RedisProperties redis) {
        var safety = redis.isNoEvictionExternallyValidated()
                ? RedisStoreConfig.RedisSafetyValidation.externallyValidatedMode()
                : RedisStoreConfig.RedisSafetyValidation.strict();
        return switch (redis.getMode().toLowerCase(Locale.ROOT)) {
            case "standalone" -> {
                if (redis.getUri() == null || redis.getUri().isBlank()) {
                    throw new IllegalArgumentException("threadmill.store.redis.uri must be set for standalone Redis");
                }
                yield new RedisStoreConfig.Standalone(RedisURI.create(redis.getUri()), safety);
            }
            case "sentinel" -> {
                var sentinel = redis.getSentinel();
                yield new RedisStoreConfig.Sentinel(
                        sentinel.getMasterName(), parseRedisNodes(sentinel.getNodes()), sentinel.getPassword(), safety);
            }
            case "cluster" -> {
                var cluster = redis.getCluster();
                yield new RedisStoreConfig.Cluster(
                        parseRedisNodes(cluster.getNodes()), cluster.getReadPolicy(), safety);
            }
            default ->
                throw new IllegalArgumentException(
                        "threadmill.store.redis.mode must be standalone, sentinel, or cluster");
        };
    }

    private static List<RedisStoreConfig.HostAndPort> parseRedisNodes(List<String> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("threadmill.store.redis nodes must not be empty");
        }
        return nodes.stream()
                .map(ThreadmillRedisAutoConfiguration::parseRedisNode)
                .toList();
    }

    private static RedisStoreConfig.HostAndPort parseRedisNode(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Redis node must not be blank");
        }
        int colon = value.lastIndexOf(':');
        if (colon < 1 || colon == value.length() - 1) {
            throw new IllegalArgumentException("Redis node must use host:port format: " + value);
        }
        return new RedisStoreConfig.HostAndPort(
                value.substring(0, colon), Integer.parseInt(value.substring(colon + 1)));
    }
}
