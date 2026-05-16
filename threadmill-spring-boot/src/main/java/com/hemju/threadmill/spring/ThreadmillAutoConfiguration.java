package com.hemju.threadmill.spring;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

import javax.sql.DataSource;

import io.lettuce.core.RedisURI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.handler.JobHandlerResolver;
import com.hemju.threadmill.core.schedule.Scheduler;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.memory.InMemoryJobStore;
import com.hemju.threadmill.store.postgres.PostgresJobStore;
import com.hemju.threadmill.store.redis.RedisJobStore;
import com.hemju.threadmill.store.redis.RedisStoreConfig;

/**
 * Spring Boot auto-configuration for Threadmill.
 *
 * <p>Wires the following beans (each conditional on the application not already
 * defining its own):
 * <ul>
 *   <li>A {@link JobStore} — defaults to {@link InMemoryJobStore}. Applications
 *       that want Postgres or Redis define their own bean and this default is
 *       skipped.</li>
 *   <li>A {@link JobSerializer} — defaults to {@link JsonJobSerializer} backed
 *       by Threadmill's mapper; applications may override with their own
 *       Jackson-backed serializer to share the host application's mapper.</li>
 *   <li>A {@link JobHandlerResolver} — defaults to
 *       {@link SpringJobHandlerResolver}, looking handlers up as Spring beans.</li>
 *   <li>A {@link Scheduler} — the user-facing API for enqueuing.</li>
 *   <li>A {@link ProcessingNode} — started with the context, stopped when the
 *       context closes.</li>
 * </ul>
 *
 * <p>Set {@code threadmill.enabled=false} to wire the {@code Scheduler} bean
 * without starting a {@code ProcessingNode} (submitting-only mode).
 */
@AutoConfiguration
@AutoConfigureAfter(
        name = {
            "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
            "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration"
        })
@EnableConfigurationProperties(ThreadmillProperties.class)
public class ThreadmillAutoConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadmillAutoConfiguration.class);

    /**
     * Resolve the {@link JobStore} by classpath and configuration precedence:
     * <ol>
     *   <li>If {@code threadmill.store.redis.*} is explicitly configured, use Redis.</li>
     *   <li>Else, if a {@link DataSource} bean is present and the Postgres store class is
     *       on the classpath, use {@link PostgresJobStore}.</li>
     *   <li>Else, fall back to {@link InMemoryJobStore} with a loud warning.</li>
     * </ol>
     *
     * <p>Applications wanting full control define their own {@code JobStore} bean and this
     * default is skipped.
     */
    @Bean
    @ConditionalOnMissingBean
    public JobStore threadmillJobStore(ThreadmillProperties properties, ApplicationContext context) {
        if (properties.getStore().getRedis().isConfigured()) {
            return new RedisJobStore(redisStoreConfig(properties.getStore().getRedis()));
        }
        DataSource dataSource = lookupOptionalBean(context, DataSource.class);
        if (dataSource != null && isPostgresOnClasspath()) {
            LOG.info("Threadmill: using Postgres store wired from the application's DataSource");
            return new PostgresJobStore(dataSource);
        }
        LOG.warn("Threadmill: using in-memory store — jobs will not survive restart. Configure"
                + " threadmill.store.redis.* or define a DataSource bean for durable storage.");
        return new InMemoryJobStore();
    }

    private static <T> T lookupOptionalBean(ApplicationContext context, Class<T> type) {
        try {
            return context.getBean(type);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static boolean isPostgresOnClasspath() {
        try {
            Class.forName(
                    "com.hemju.threadmill.store.postgres.PostgresJobStore",
                    false,
                    ThreadmillAutoConfiguration.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException notFound) {
            return false;
        }
    }

    @Bean
    @ConditionalOnMissingBean
    public JobSerializer threadmillJobSerializer() {
        return new JsonJobSerializer();
    }

    @Bean
    @ConditionalOnMissingBean
    public JobHandlerResolver threadmillJobHandlerResolver(ApplicationContext context) {
        return new SpringJobHandlerResolver(context);
    }

    @Bean
    @ConditionalOnMissingBean
    public Scheduler threadmillScheduler(JobStore store, JobSerializer serializer) {
        return new Scheduler(store, serializer);
    }

    @Bean
    @ConditionalOnMissingBean
    public ProcessingNodeConfig threadmillProcessingNodeConfig(ThreadmillProperties properties) {
        return ProcessingNodeConfig.builder()
                .workerCount(properties.getWorkerCount())
                .pollInterval(properties.getPollInterval())
                .claimHeartbeat(properties.getClaimHeartbeat())
                .heartbeatTimeout(properties.getHeartbeatTimeout())
                .maintenanceLeaseDuration(properties.getMaintenanceLeaseDuration())
                .nodeHeartbeatRetention(properties.getNodeHeartbeatRetention())
                .checkInMinInterval(properties.getCheckInMinInterval())
                .noProgressTimeout(properties.getNoProgressTimeout())
                .queueFamilyDiscoveryInterval(properties.getQueueFamily().getDiscoveryInterval())
                .queueFamilyRetentionAfterEmpty(properties.getQueueFamily().getRetentionAfterEmpty())
                .logMaxRatePerSecond(properties.getLogMaxRatePerSecond())
                .logMaxEntries(properties.getLogMaxEntries())
                .logMaxBytes(properties.getLogMaxBytes())
                .maxDedupTtl(properties.getMaxDedupTtl())
                .defaultMaxAttempts(properties.getDefaultMaxAttempts())
                .retryInitialBackoff(properties.getRetryInitialBackoff())
                .jobTimeout(properties.getJobTimeout())
                .maxConsecutiveDispatcherFailures(properties.getMaxConsecutiveDispatcherFailures())
                .claimBatchSize(properties.getClaimBatchSize())
                .storeOutagePollInterval(properties.getStoreOutagePollInterval())
                .shutdownGracePeriod(properties.getShutdownGracePeriod())
                .defaultQueue(properties.getDefaultQueue())
                .maintenancePollInterval(properties.getMaintenancePollInterval())
                .retentionInterval(properties.getRetentionInterval())
                .build();
    }

    @Bean
    @ConditionalOnMissingBean
    public ThreadmillJobRegistry threadmillJobRegistry(ApplicationContext context, ThreadmillProperties properties) {
        return new ThreadmillJobRegistry(context, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public ThreadmillRecurringRegistrar threadmillRecurringRegistrar(
            Scheduler scheduler, ThreadmillJobRegistry registry) {
        var registrar = new ThreadmillRecurringRegistrar(scheduler, registry);
        registrar.registerAll();
        return registrar;
    }

    @Bean
    @ConditionalOnMissingBean
    public JobScheduler threadmillJobScheduler(
            JobStore store,
            JobSerializer serializer,
            ThreadmillJobRegistry registry,
            ProcessingNodeConfig config,
            ThreadmillProperties properties) {
        if (properties.getSpring().isEnqueueAfterCommit()) {
            return new TransactionAwareJobScheduler(store, serializer, registry, config);
        }
        return new JobScheduler(store, serializer, registry, config);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "threadmill", name = "enabled", havingValue = "true", matchIfMissing = true)
    public ProcessingNode threadmillProcessingNode(
            JobStore store,
            JobSerializer serializer,
            JobHandlerResolver resolver,
            ProcessingNodeConfig config,
            ThreadmillJobRegistry registry) {
        List<String> queues = resolveQueues(config, registry);
        var builder = ProcessingNode.builder(store)
                .config(config)
                .serializer(serializer)
                .handlerResolver(resolver);
        for (String queue : queues) {
            builder.lane(queue, config.workerCount());
        }
        ProcessingNode node = builder.build();
        for (var registration : registry.registrations()) {
            LOG.info(
                    "Threadmill: registered handler {} for payload {} on queue '{}' (timeout={}, maxRetries={})",
                    registration.handlerType().getName(),
                    registration.payloadType().getName(),
                    registration.queue(),
                    registration.timeout(),
                    registration.maxRetries());
        }
        LOG.info(
                "Threadmill: {} handlers registered; polling lanes={} with {} workers each; store={}; node={}",
                registry.registrations().size(),
                queues,
                config.workerCount(),
                store.getClass().getSimpleName(),
                node.nodeId());
        return node;
    }

    /**
     * Resolve the queue lanes the {@link ProcessingNode} will poll.
     *
     * <p>Starts with the configured default queue so it is always served, then
     * adds every distinct queue declared by an {@code @Job} handler.
     * Without this, a handler annotated with a non-default {@code queue} would
     * accept enqueues but never run — its jobs would sit in the store with no
     * dispatcher polling them.
     *
     * <p>Order is insertion order, with the default queue first, so it is
     * deterministic across restarts and easy to read in startup logs.
     */
    private static List<String> resolveQueues(ProcessingNodeConfig config, ThreadmillJobRegistry registry) {
        var queues = new LinkedHashSet<String>();
        queues.add(config.defaultQueue());
        for (var registration : registry.registrations()) {
            queues.add(registration.queue());
        }
        return List.copyOf(queues);
    }

    /**
     * Wraps the {@link ProcessingNode} in a {@link ThreadmillLifecycle} so Spring drives
     * start/stop through its lifecycle protocol — guaranteeing the engine starts after
     * the DataSource / RedisConnectionFactory is ready and stops before they are torn
     * down on graceful shutdown.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "threadmill", name = "enabled", havingValue = "true", matchIfMissing = true)
    public ThreadmillLifecycle threadmillLifecycle(ProcessingNode node) {
        return new ThreadmillLifecycle(node);
    }

    private static RedisStoreConfig redisStoreConfig(ThreadmillProperties.RedisProperties redis) {
        return switch (redis.getMode().toLowerCase(Locale.ROOT)) {
            case "standalone" -> {
                if (redis.getUri() == null || redis.getUri().isBlank()) {
                    throw new IllegalArgumentException("threadmill.store.redis.uri must be set for standalone Redis");
                }
                yield new RedisStoreConfig.Standalone(RedisURI.create(redis.getUri()));
            }
            case "sentinel" -> {
                var sentinel = redis.getSentinel();
                yield new RedisStoreConfig.Sentinel(
                        sentinel.getMasterName(), parseRedisNodes(sentinel.getNodes()), sentinel.getPassword());
            }
            case "cluster" -> {
                var cluster = redis.getCluster();
                yield new RedisStoreConfig.Cluster(parseRedisNodes(cluster.getNodes()), cluster.getReadPolicy());
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
        return nodes.stream().map(ThreadmillAutoConfiguration::parseRedisNode).toList();
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
