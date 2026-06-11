package com.hemju.threadmill.spring;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties bound from {@code threadmill.*}. Subset of
 * {@link com.hemju.threadmill.core.engine.ProcessingNodeConfig}; values
 * not bound fall back to the engine's defaults.
 */
@ConfigurationProperties(prefix = "threadmill")
public class ThreadmillProperties {

    /** Whether the auto-configured ProcessingNode should start with the Spring context. */
    private boolean enabled = true;

    /** Number of worker permits per default lane. */
    private int workerCount = 10;

    /** How often the dispatcher polls for new work. */
    private Duration pollInterval = Duration.ofMillis(500);

    /** How often owned jobs and the node registry heartbeat are refreshed. */
    private Duration claimHeartbeat = Duration.ofSeconds(15);

    /**
     * How often the master-only maintenance loop ticks. This bounds the latency
     * of materializing due recurring jobs, promoting due scheduled jobs to
     * {@code ENQUEUED}, and reclaiming orphans. Defaults to 1 s.
     */
    private Duration maintenancePollInterval = Duration.ofSeconds(1);

    /**
     * How often retention sweeps run on the master node — old succeeded jobs,
     * expired dedup keys, and stale node-heartbeat rows. Deletion is not
     * time-sensitive, so the default is generous. Defaults to 1 h.
     */
    private Duration retentionInterval = Duration.ofHours(1);

    /** How long before a heartbeat is considered expired. */
    private Duration heartbeatTimeout = Duration.ofSeconds(60);

    /** How long the maintenance leadership lease lasts. */
    private Duration maintenanceLeaseDuration = Duration.ofSeconds(60);

    /** How long old node heartbeat records are kept for observability. */
    private Duration nodeHeartbeatRetention = Duration.ofMinutes(10);

    /** How long SUCCEEDED jobs are kept before retention hard-deletes them. */
    private Duration succeededRetention = Duration.ofDays(7);

    /** How long FAILED jobs are kept before retention hard-deletes them. */
    private Duration failedRetention = Duration.ofDays(30);

    /** How long DELETED (soft-deleted) jobs are kept before retention hard-deletes them. */
    private Duration deletedRetention = Duration.ofDays(7);

    /** How long QUARANTINED jobs are kept before retention hard-deletes them. */
    private Duration quarantinedRetention = Duration.ofDays(30);

    /** Maximum attempts (including the first) before a failing job stays in FAILED. */
    private int defaultMaxAttempts = 5;

    /** Initial backoff before the first retry. */
    private Duration retryInitialBackoff = Duration.ofSeconds(5);

    /** Per-job timeout. */
    private Duration jobTimeout = Duration.ofMinutes(5);

    /** Maximum consecutive dispatcher store failures before pausing. */
    private int maxConsecutiveDispatcherFailures = 10;

    /** Maximum number of jobs claimed in one poll. */
    private int claimBatchSize = 10;

    /** How often a paused dispatcher probes the store for recovery. */
    private Duration storeOutagePollInterval = Duration.ofSeconds(5);

    /** Grace period for in-flight jobs during node shutdown. */
    private Duration shutdownGracePeriod = Duration.ofSeconds(10);

    private Duration checkInMinInterval = Duration.ofSeconds(5);
    private Duration noProgressTimeout = Duration.ofMinutes(15);
    private int logMaxRatePerSecond = 100;
    private int logMaxEntries = 1000;
    private int logMaxBytes = 256 * 1024;
    private Duration maxDedupTtl = Duration.ofDays(30);

    /** Name of the default queue. */
    private String defaultQueue = "default";

    /** Store-specific configuration. */
    private StoreProperties store = new StoreProperties();

    /** Queue-family lane discovery configuration. */
    private QueueFamilyProperties queueFamily = new QueueFamilyProperties();

    /** Cross-node wake notification configuration. */
    private RemoteWakeProperties remoteWake = new RemoteWakeProperties();

    /** Spring-specific configuration (transaction integration). */
    private SpringProperties spring = new SpringProperties();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getWorkerCount() {
        return workerCount;
    }

    public void setWorkerCount(int workerCount) {
        this.workerCount = workerCount;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public void setPollInterval(Duration pollInterval) {
        this.pollInterval = pollInterval;
    }

    public Duration getClaimHeartbeat() {
        return claimHeartbeat;
    }

    public void setClaimHeartbeat(Duration claimHeartbeat) {
        this.claimHeartbeat = claimHeartbeat;
    }

    public Duration getMaintenancePollInterval() {
        return maintenancePollInterval;
    }

    public void setMaintenancePollInterval(Duration maintenancePollInterval) {
        this.maintenancePollInterval = maintenancePollInterval;
    }

    public Duration getRetentionInterval() {
        return retentionInterval;
    }

    public void setRetentionInterval(Duration retentionInterval) {
        this.retentionInterval = retentionInterval;
    }

    public Duration getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    public void setHeartbeatTimeout(Duration heartbeatTimeout) {
        this.heartbeatTimeout = heartbeatTimeout;
    }

    public Duration getMaintenanceLeaseDuration() {
        return maintenanceLeaseDuration;
    }

    public void setMaintenanceLeaseDuration(Duration maintenanceLeaseDuration) {
        this.maintenanceLeaseDuration = maintenanceLeaseDuration;
    }

    public Duration getSucceededRetention() {
        return succeededRetention;
    }

    public void setSucceededRetention(Duration succeededRetention) {
        this.succeededRetention = succeededRetention;
    }

    public Duration getFailedRetention() {
        return failedRetention;
    }

    public void setFailedRetention(Duration failedRetention) {
        this.failedRetention = failedRetention;
    }

    public Duration getDeletedRetention() {
        return deletedRetention;
    }

    public void setDeletedRetention(Duration deletedRetention) {
        this.deletedRetention = deletedRetention;
    }

    public Duration getQuarantinedRetention() {
        return quarantinedRetention;
    }

    public void setQuarantinedRetention(Duration quarantinedRetention) {
        this.quarantinedRetention = quarantinedRetention;
    }

    public Duration getNodeHeartbeatRetention() {
        return nodeHeartbeatRetention;
    }

    public void setNodeHeartbeatRetention(Duration nodeHeartbeatRetention) {
        this.nodeHeartbeatRetention = nodeHeartbeatRetention;
    }

    public int getDefaultMaxAttempts() {
        return defaultMaxAttempts;
    }

    public void setDefaultMaxAttempts(int defaultMaxAttempts) {
        this.defaultMaxAttempts = defaultMaxAttempts;
    }

    public Duration getRetryInitialBackoff() {
        return retryInitialBackoff;
    }

    public void setRetryInitialBackoff(Duration retryInitialBackoff) {
        this.retryInitialBackoff = retryInitialBackoff;
    }

    public Duration getJobTimeout() {
        return jobTimeout;
    }

    public void setJobTimeout(Duration jobTimeout) {
        this.jobTimeout = jobTimeout;
    }

    public int getMaxConsecutiveDispatcherFailures() {
        return maxConsecutiveDispatcherFailures;
    }

    public void setMaxConsecutiveDispatcherFailures(int maxConsecutiveDispatcherFailures) {
        this.maxConsecutiveDispatcherFailures = maxConsecutiveDispatcherFailures;
    }

    public int getClaimBatchSize() {
        return claimBatchSize;
    }

    public void setClaimBatchSize(int claimBatchSize) {
        this.claimBatchSize = claimBatchSize;
    }

    public Duration getStoreOutagePollInterval() {
        return storeOutagePollInterval;
    }

    public void setStoreOutagePollInterval(Duration storeOutagePollInterval) {
        this.storeOutagePollInterval = storeOutagePollInterval;
    }

    public Duration getShutdownGracePeriod() {
        return shutdownGracePeriod;
    }

    public void setShutdownGracePeriod(Duration shutdownGracePeriod) {
        this.shutdownGracePeriod = shutdownGracePeriod;
    }

    public Duration getCheckInMinInterval() {
        return checkInMinInterval;
    }

    public void setCheckInMinInterval(Duration checkInMinInterval) {
        this.checkInMinInterval = checkInMinInterval;
    }

    public Duration getNoProgressTimeout() {
        return noProgressTimeout;
    }

    public void setNoProgressTimeout(Duration noProgressTimeout) {
        this.noProgressTimeout = noProgressTimeout;
    }

    public int getLogMaxRatePerSecond() {
        return logMaxRatePerSecond;
    }

    public void setLogMaxRatePerSecond(int logMaxRatePerSecond) {
        this.logMaxRatePerSecond = logMaxRatePerSecond;
    }

    public int getLogMaxEntries() {
        return logMaxEntries;
    }

    public void setLogMaxEntries(int logMaxEntries) {
        this.logMaxEntries = logMaxEntries;
    }

    public int getLogMaxBytes() {
        return logMaxBytes;
    }

    public void setLogMaxBytes(int logMaxBytes) {
        this.logMaxBytes = logMaxBytes;
    }

    public Duration getMaxDedupTtl() {
        return maxDedupTtl;
    }

    public void setMaxDedupTtl(Duration maxDedupTtl) {
        this.maxDedupTtl = maxDedupTtl;
    }

    public String getDefaultQueue() {
        return defaultQueue;
    }

    public void setDefaultQueue(String defaultQueue) {
        this.defaultQueue = defaultQueue;
    }

    public StoreProperties getStore() {
        return store;
    }

    public void setStore(StoreProperties store) {
        this.store = store;
    }

    public QueueFamilyProperties getQueueFamily() {
        return queueFamily;
    }

    public void setQueueFamily(QueueFamilyProperties queueFamily) {
        this.queueFamily = queueFamily;
    }

    public RemoteWakeProperties getRemoteWake() {
        return remoteWake;
    }

    public void setRemoteWake(RemoteWakeProperties remoteWake) {
        this.remoteWake = remoteWake;
    }

    public SpringProperties getSpring() {
        return spring;
    }

    public void setSpring(SpringProperties spring) {
        this.spring = spring;
    }

    /** Spring-specific options: transaction integration etc. */
    public static final class SpringProperties {
        /** How enqueue calls interact with active Spring transactions. */
        private SpringEnqueueMode enqueueMode = SpringEnqueueMode.AFTER_COMMIT;

        private String recurringNamespace;

        public SpringEnqueueMode getEnqueueMode() {
            return enqueueMode;
        }

        public void setEnqueueMode(SpringEnqueueMode enqueueMode) {
            this.enqueueMode = enqueueMode;
        }

        public String getRecurringNamespace() {
            return recurringNamespace;
        }

        public void setRecurringNamespace(String recurringNamespace) {
            this.recurringNamespace = recurringNamespace;
        }
    }

    public static final class QueueFamilyProperties {
        private Duration discoveryInterval = Duration.ofSeconds(1);
        private Duration retentionAfterEmpty = Duration.ofSeconds(30);

        public Duration getDiscoveryInterval() {
            return discoveryInterval;
        }

        public void setDiscoveryInterval(Duration discoveryInterval) {
            this.discoveryInterval = discoveryInterval;
        }

        public Duration getRetentionAfterEmpty() {
            return retentionAfterEmpty;
        }

        public void setRetentionAfterEmpty(Duration retentionAfterEmpty) {
            this.retentionAfterEmpty = retentionAfterEmpty;
        }
    }

    public static final class RemoteWakeProperties {
        private boolean enabled = true;
        private String channel;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getChannel() {
            return channel;
        }

        public void setChannel(String channel) {
            this.channel = channel;
        }
    }

    public static final class StoreProperties {
        private PostgresProperties postgres = new PostgresProperties();
        private RedisProperties redis = new RedisProperties();

        public PostgresProperties getPostgres() {
            return postgres;
        }

        public void setPostgres(PostgresProperties postgres) {
            this.postgres = postgres;
        }

        public RedisProperties getRedis() {
            return redis;
        }

        public void setRedis(RedisProperties redis) {
            this.redis = redis;
        }
    }

    public static final class PostgresProperties {
        private PostgresSchemaMode schemaMode = PostgresSchemaMode.MIGRATE;
        private boolean allowDestructiveSchemaReset;

        public PostgresSchemaMode getSchemaMode() {
            return schemaMode;
        }

        public void setSchemaMode(PostgresSchemaMode schemaMode) {
            this.schemaMode = schemaMode;
        }

        public boolean isAllowDestructiveSchemaReset() {
            return allowDestructiveSchemaReset;
        }

        public void setAllowDestructiveSchemaReset(boolean allowDestructiveSchemaReset) {
            this.allowDestructiveSchemaReset = allowDestructiveSchemaReset;
        }
    }

    public static final class RedisProperties {
        private String mode = "standalone";
        private String uri;
        private boolean noEvictionExternallyValidated;
        private boolean resetOnStart;
        private boolean allowDestructiveReset;
        private SentinelProperties sentinel = new SentinelProperties();
        private ClusterProperties cluster = new ClusterProperties();

        public boolean isConfigured() {
            return (uri != null && !uri.isBlank())
                    || (sentinel.masterName != null && !sentinel.masterName.isBlank())
                    || !cluster.nodes.isEmpty();
        }

        public String getMode() {
            return mode;
        }

        public void setMode(String mode) {
            this.mode = mode;
        }

        public String getUri() {
            return uri;
        }

        public void setUri(String uri) {
            this.uri = uri;
        }

        public boolean isNoEvictionExternallyValidated() {
            return noEvictionExternallyValidated;
        }

        public void setNoEvictionExternallyValidated(boolean noEvictionExternallyValidated) {
            this.noEvictionExternallyValidated = noEvictionExternallyValidated;
        }

        public boolean isResetOnStart() {
            return resetOnStart;
        }

        public void setResetOnStart(boolean resetOnStart) {
            this.resetOnStart = resetOnStart;
        }

        public boolean isAllowDestructiveReset() {
            return allowDestructiveReset;
        }

        public void setAllowDestructiveReset(boolean allowDestructiveReset) {
            this.allowDestructiveReset = allowDestructiveReset;
        }

        public SentinelProperties getSentinel() {
            return sentinel;
        }

        public void setSentinel(SentinelProperties sentinel) {
            this.sentinel = sentinel;
        }

        public ClusterProperties getCluster() {
            return cluster;
        }

        public void setCluster(ClusterProperties cluster) {
            this.cluster = cluster;
        }
    }

    public static final class SentinelProperties {
        private String masterName;
        private List<String> nodes = new ArrayList<>();
        private String password;

        public String getMasterName() {
            return masterName;
        }

        public void setMasterName(String masterName) {
            this.masterName = masterName;
        }

        public List<String> getNodes() {
            return nodes;
        }

        public void setNodes(List<String> nodes) {
            this.nodes = nodes;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }

    public static final class ClusterProperties {
        private List<String> nodes = new ArrayList<>();
        private String readPolicy = "master";

        public List<String> getNodes() {
            return nodes;
        }

        public void setNodes(List<String> nodes) {
            this.nodes = nodes;
        }

        public String getReadPolicy() {
            return readPolicy;
        }

        public void setReadPolicy(String readPolicy) {
            this.readPolicy = readPolicy;
        }
    }
}
