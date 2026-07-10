package com.hemju.threadmill.spring;

import java.util.LinkedHashSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import com.hemju.threadmill.core.Names;
import com.hemju.threadmill.core.engine.JobInterceptor;
import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.engine.RemoteWakeChannel;
import com.hemju.threadmill.core.handler.JobHandlerResolver;
import com.hemju.threadmill.core.schedule.Scheduler;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.serialization.JobSerializers;
import com.hemju.threadmill.core.serialization.PayloadMigrations;
import com.hemju.threadmill.core.serialization.TypeNameAliases;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

/**
 * Spring Boot auto-configuration for Threadmill.
 *
 * <p>Wires the following beans (each conditional on the application not already
 * defining its own):
 * <ul>
 *   <li>A {@link JobStore}. Applications provide a durable store or explicitly opt
 *       into the volatile {@link InMemoryJobStore} for development and tests.</li>
 *   <li>A {@link JobSerializer} — defaults to Threadmill JSON serialization backed
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
// String-form FQCNs keep the constant pool free of optional Spring modules.
// These are the Spring Boot 4 locations (the SB3-era
// org.springframework.boot.autoconfigure.* names no longer exist, and
// AutoConfigurationSorter silently ignores unknown names).
@AutoConfigureAfter(
        name = {
            "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
            "org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration"
        })
@EnableConfigurationProperties(ThreadmillProperties.class)
public class ThreadmillAutoConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadmillAutoConfiguration.class);

    public ThreadmillAutoConfiguration() {
        SpringBootVersionGuard.requireSpringBootFour();
    }

    /**
     * In-memory {@link JobStore} fallback when nothing else has provided one.
     *
     * <p>The durable stores live in their own gated configurations, each ordered
     * {@code @AutoConfigureBefore} this class so they win the
     * {@link ConditionalOnMissingBean} race when their conditions match:
     * {@link ThreadmillRedisAutoConfiguration} ({@code @ConditionalOnClass(RedisJobStore.class)}
     * + {@code threadmill.store.redis.*} configured) and
     * {@link ThreadmillPostgresAutoConfiguration}
     * ({@code @ConditionalOnClass(PostgresJobStore.class)} + a {@code DataSource},
     * deferring to Redis). Keeping this class free of any
     * {@code threadmill-store-redis}/{@code -postgres} class references means the
     * optional store modules can be {@code compileOnly} dependencies and excluded
     * at runtime without breaking this configuration.
     *
     * <p>Applications wanting full control define their own {@code JobStore} bean
     * and this default is skipped.
     */
    @Bean
    @ConditionalOnMissingBean
    public JobStore threadmillJobStore(ThreadmillProperties properties) {
        // Durable stores are contributed by ThreadmillRedisAutoConfiguration and
        // ThreadmillPostgresAutoConfiguration, both @AutoConfigureBefore this one
        // and gated on their store class + configuration; this is the in-memory
        // fallback when neither matched.
        if (!properties.getStore().getMemory().isEnabled()) {
            throw new IllegalStateException("Threadmill has no durable JobStore. Configure threadmill.store.redis.*,"
                    + " provide a DataSource or JobStore bean, or explicitly opt into volatile development storage"
                    + " with threadmill.store.memory.enabled=true.");
        }
        LOG.warn("Threadmill: using explicitly enabled in-memory store — jobs will not survive restart.");
        return new InMemoryJobStore();
    }

    @Bean
    @ConditionalOnMissingBean
    public PayloadMigrations threadmillPayloadMigrations() {
        return PayloadMigrations.empty();
    }

    @Bean
    @ConditionalOnMissingBean
    public TypeNameAliases threadmillTypeNameAliases() {
        return TypeNameAliases.empty();
    }

    @Bean
    @ConditionalOnMissingBean
    public JobSerializer threadmillJobSerializer(TypeNameAliases aliases, PayloadMigrations payloadMigrations) {
        return JobSerializers.json(aliases, payloadMigrations);
    }

    @Bean
    @ConditionalOnMissingBean
    public JobHandlerResolver threadmillJobHandlerResolver(ApplicationContext context, TypeNameAliases aliases) {
        return new SpringJobHandlerResolver(context, aliases);
    }

    @Bean
    @ConditionalOnMissingBean
    public LocalWakeBus threadmillLocalWakeBus() {
        return new LocalWakeBus();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(
            prefix = "threadmill.remote-wake",
            name = "enabled",
            havingValue = "true",
            matchIfMissing = true)
    public ThreadmillRemoteWakeChannels threadmillRemoteWakeChannels(
            ThreadmillProperties properties, JobStore store, ObjectProvider<RemoteWakeChannel> customChannel) {
        RemoteWakeChannel provided = customChannel.getIfAvailable();
        if (provided != null) {
            return ThreadmillRemoteWakeChannels.of(provided);
        }
        JobStore concreteStore = unwrapStore(store);
        String channel = remoteWakeChannel(properties);
        // Every store with a native notification path (Postgres LISTEN/NOTIFY,
        // Redis Pub/Sub) exposes it through the JobStore SPI hook — no
        // concrete store class references are required here, and user-defined
        // store beans get the same wiring as property-configured ones.
        return concreteStore
                .createRemoteWakeChannel(channel)
                .map(ThreadmillRemoteWakeChannels::ofManaged)
                .orElseGet(ThreadmillRemoteWakeChannels::none);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(ThreadmillRemoteWakeChannels.class)
    public ThreadmillRemoteWakePublisher threadmillRemoteWakePublisher(
            LocalWakeBus wakeBus, ThreadmillRemoteWakeChannels remoteWakeChannels) {
        return new ThreadmillRemoteWakePublisher(wakeBus, remoteWakeChannels);
    }

    @Bean
    @ConditionalOnMissingBean
    public Scheduler threadmillScheduler(JobStore store, JobSerializer serializer, LocalWakeBus wakeBus) {
        return new Scheduler(store, serializer, wakeBus);
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
                .succeededRetention(properties.getSucceededRetention())
                .failedRetention(properties.getFailedRetention())
                .deletedRetention(properties.getDeletedRetention())
                .quarantinedRetention(properties.getQuarantinedRetention())
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
            Scheduler scheduler,
            ThreadmillJobRegistry registry,
            JobSerializer serializer,
            ThreadmillProperties properties,
            ApplicationContext context) {
        var registrar = new ThreadmillRecurringRegistrar(
                scheduler, registry, serializer, recurringNamespace(properties, context));
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
            ThreadmillProperties properties,
            LocalWakeBus wakeBus) {
        return switch (properties.getSpring().getEnqueueMode()) {
            case AFTER_COMMIT -> new TransactionAwareJobScheduler(store, serializer, registry, config, wakeBus);
            case IMMEDIATE -> new JobScheduler(store, serializer, registry, config, wakeBus);
            case JOIN_TRANSACTION -> {
                // Routed through the generic JobStore SPI flag so this method does not
                // need to reference any postgres-store class — keeping the auto-config
                // loadable even when threadmill-store-postgres is not on the classpath.
                if (!unwrapStore(store).supportsExternalTransactions()) {
                    throw new IllegalStateException(
                            "threadmill.spring.enqueue-mode=join_transaction requires a JobStore that supports"
                                    + " external transactions (today: the Spring auto-configured PostgresJobStore"
                                    + " using the same DataSource as the caller's transaction)");
                }
                yield new TransactionJoinedJobScheduler(store, serializer, registry, config, wakeBus);
            }
        };
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "threadmill", name = "enabled", havingValue = "true", matchIfMissing = true)
    public ProcessingNode threadmillProcessingNode(
            JobStore store,
            JobSerializer serializer,
            JobHandlerResolver resolver,
            ProcessingNodeConfig config,
            ThreadmillJobRegistry registry,
            LocalWakeBus wakeBus,
            List<JobInterceptor> interceptors) {
        List<String> queues = resolveQueues(config, registry);
        var builder = ProcessingNode.builder(store)
                .config(config)
                .serializer(serializer)
                .handlerResolver(resolver)
                .wakeBus(wakeBus);
        for (JobInterceptor interceptor : interceptors) {
            builder.interceptor(interceptor);
        }
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
     * start/stop through its lifecycle protocol. Remote-wake subscriptions are folded
     * into the same lifecycle to guarantee node-before-subscription startup and
     * subscription-before-node shutdown.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "threadmill", name = "enabled", havingValue = "true", matchIfMissing = true)
    public ThreadmillLifecycle threadmillLifecycle(
            ProcessingNode node, ObjectProvider<ThreadmillRemoteWakeChannels> remoteWakeChannels) {
        return new ThreadmillLifecycle(node, remoteWakeChannels.getIfAvailable());
    }

    private static String recurringNamespace(ThreadmillProperties properties, ApplicationContext context) {
        String configured = properties.getSpring().getRecurringNamespace();
        if (configured != null && !configured.isBlank()) {
            return configured;
        }
        return context.getEnvironment().getProperty("spring.application.name");
    }

    private static JobStore unwrapStore(JobStore store) {
        JobStore current = store;
        while (true) {
            JobStore next = current.delegate();
            if (next == current) return current;
            current = next;
        }
    }

    private static String remoteWakeChannel(ThreadmillProperties properties) {
        String configured = properties.getRemoteWake().getChannel();
        if (configured == null || configured.isBlank()) return null;
        return Names.requireName("threadmill.remote-wake.channel", configured);
    }
}
