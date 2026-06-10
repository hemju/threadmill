package com.hemju.threadmill.core.engine;

import java.time.Duration;
import java.util.Objects;

import com.hemju.threadmill.core.Names;

/**
 * Runtime configuration for a {@link ProcessingNode}. Reasonable defaults
 * are provided; every parameter is overridable.
 */
public record ProcessingNodeConfig(
        int workerCount,
        Duration pollInterval,
        Duration claimHeartbeat,
        Duration heartbeatTimeout,
        Duration jobTimeout,
        int maxConsecutiveDispatcherFailures,
        Duration retryInitialBackoff,
        int defaultMaxAttempts,
        int claimBatchSize,
        String defaultQueue,
        Duration storeOutagePollInterval,
        Duration maintenanceLeaseDuration,
        Duration nodeHeartbeatRetention,
        Duration checkInMinInterval,
        Duration noProgressTimeout,
        Duration queueFamilyDiscoveryInterval,
        Duration queueFamilyRetentionAfterEmpty,
        int logMaxRatePerSecond,
        int logMaxEntries,
        int logMaxBytes,
        Duration maxDedupTtl,
        Duration shutdownGracePeriod,
        Duration maintenancePollInterval,
        Duration retentionInterval,
        Duration succeededRetention,
        Duration failedRetention,
        Duration deletedRetention,
        Duration quarantinedRetention) {

    public ProcessingNodeConfig {
        if (workerCount <= 0) throw new IllegalArgumentException("workerCount must be positive");
        if (claimBatchSize <= 0) throw new IllegalArgumentException("claimBatchSize must be positive");
        if (maxConsecutiveDispatcherFailures <= 0)
            throw new IllegalArgumentException("maxConsecutiveDispatcherFailures must be positive");
        if (defaultMaxAttempts <= 0) throw new IllegalArgumentException("defaultMaxAttempts must be positive");
        if (logMaxRatePerSecond <= 0) throw new IllegalArgumentException("logMaxRatePerSecond must be positive");
        if (logMaxEntries <= 0) throw new IllegalArgumentException("logMaxEntries must be positive");
        if (logMaxBytes <= 0) throw new IllegalArgumentException("logMaxBytes must be positive");
        Objects.requireNonNull(pollInterval, "pollInterval");
        Objects.requireNonNull(claimHeartbeat, "claimHeartbeat");
        Objects.requireNonNull(heartbeatTimeout, "heartbeatTimeout");
        Objects.requireNonNull(jobTimeout, "jobTimeout");
        Objects.requireNonNull(retryInitialBackoff, "retryInitialBackoff");
        Objects.requireNonNull(defaultQueue, "defaultQueue");
        Objects.requireNonNull(storeOutagePollInterval, "storeOutagePollInterval");
        Objects.requireNonNull(maintenanceLeaseDuration, "maintenanceLeaseDuration");
        Objects.requireNonNull(nodeHeartbeatRetention, "nodeHeartbeatRetention");
        Objects.requireNonNull(checkInMinInterval, "checkInMinInterval");
        Objects.requireNonNull(noProgressTimeout, "noProgressTimeout");
        Objects.requireNonNull(queueFamilyDiscoveryInterval, "queueFamilyDiscoveryInterval");
        Objects.requireNonNull(queueFamilyRetentionAfterEmpty, "queueFamilyRetentionAfterEmpty");
        Objects.requireNonNull(maxDedupTtl, "maxDedupTtl");
        Objects.requireNonNull(shutdownGracePeriod, "shutdownGracePeriod");
        Objects.requireNonNull(maintenancePollInterval, "maintenancePollInterval");
        Objects.requireNonNull(retentionInterval, "retentionInterval");
        Objects.requireNonNull(succeededRetention, "succeededRetention");
        Objects.requireNonNull(failedRetention, "failedRetention");
        Objects.requireNonNull(deletedRetention, "deletedRetention");
        Objects.requireNonNull(quarantinedRetention, "quarantinedRetention");
        requirePositive("pollInterval", pollInterval);
        requirePositive("claimHeartbeat", claimHeartbeat);
        requirePositive("heartbeatTimeout", heartbeatTimeout);
        requirePositive("jobTimeout", jobTimeout);
        requirePositive("retryInitialBackoff", retryInitialBackoff);
        requirePositive("storeOutagePollInterval", storeOutagePollInterval);
        requirePositive("maintenanceLeaseDuration", maintenanceLeaseDuration);
        requirePositive("nodeHeartbeatRetention", nodeHeartbeatRetention);
        requirePositive("checkInMinInterval", checkInMinInterval);
        requirePositive("noProgressTimeout", noProgressTimeout);
        requirePositive("queueFamilyDiscoveryInterval", queueFamilyDiscoveryInterval);
        requirePositive("queueFamilyRetentionAfterEmpty", queueFamilyRetentionAfterEmpty);
        requirePositive("maxDedupTtl", maxDedupTtl);
        requirePositive("shutdownGracePeriod", shutdownGracePeriod);
        requirePositive("maintenancePollInterval", maintenancePollInterval);
        requirePositive("retentionInterval", retentionInterval);
        requirePositive("succeededRetention", succeededRetention);
        requirePositive("failedRetention", failedRetention);
        requirePositive("deletedRetention", deletedRetention);
        requirePositive("quarantinedRetention", quarantinedRetention);
        if (!maintenanceLeaseDuration.minus(claimHeartbeat).isPositive()) {
            throw new IllegalArgumentException("maintenanceLeaseDuration must be greater than claimHeartbeat");
        }
        if (!nodeHeartbeatRetention.minus(heartbeatTimeout).isPositive()) {
            throw new IllegalArgumentException("nodeHeartbeatRetention must be greater than heartbeatTimeout");
        }
        Names.requireName("defaultQueue", defaultQueue);
    }

    public static ProcessingNodeConfig defaults() {
        return new ProcessingNodeConfig(
                10,
                Duration.ofMillis(500),
                Duration.ofSeconds(15),
                Duration.ofSeconds(60),
                Duration.ofMinutes(5),
                10,
                Duration.ofSeconds(5),
                5,
                10,
                "default",
                Duration.ofSeconds(5),
                Duration.ofSeconds(60),
                Duration.ofMinutes(10),
                Duration.ofSeconds(5),
                Duration.ofMinutes(15),
                Duration.ofSeconds(1),
                Duration.ofSeconds(30),
                100,
                1000,
                256 * 1024,
                Duration.ofDays(30),
                Duration.ofSeconds(10),
                Duration.ofSeconds(1),
                Duration.ofHours(1),
                Duration.ofDays(7),
                Duration.ofDays(30),
                Duration.ofDays(7),
                Duration.ofDays(30));
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public static Builder builder() {
        return new Builder(defaults());
    }

    public static final class Builder {
        private int workerCount;
        private Duration pollInterval;
        private Duration claimHeartbeat;
        private Duration heartbeatTimeout;
        private Duration jobTimeout;
        private int maxConsecutiveDispatcherFailures;
        private Duration retryInitialBackoff;
        private int defaultMaxAttempts;
        private int claimBatchSize;
        private String defaultQueue;
        private Duration storeOutagePollInterval;
        private Duration maintenanceLeaseDuration;
        private Duration nodeHeartbeatRetention;
        private Duration checkInMinInterval;
        private Duration noProgressTimeout;
        private Duration queueFamilyDiscoveryInterval;
        private Duration queueFamilyRetentionAfterEmpty;
        private int logMaxRatePerSecond;
        private int logMaxEntries;
        private int logMaxBytes;
        private Duration maxDedupTtl;
        private Duration shutdownGracePeriod;
        private Duration maintenancePollInterval;
        private Duration retentionInterval;
        private Duration succeededRetention;
        private Duration failedRetention;
        private Duration deletedRetention;
        private Duration quarantinedRetention;

        private Builder(ProcessingNodeConfig c) {
            this.workerCount = c.workerCount;
            this.pollInterval = c.pollInterval;
            this.claimHeartbeat = c.claimHeartbeat;
            this.heartbeatTimeout = c.heartbeatTimeout;
            this.jobTimeout = c.jobTimeout;
            this.maxConsecutiveDispatcherFailures = c.maxConsecutiveDispatcherFailures;
            this.retryInitialBackoff = c.retryInitialBackoff;
            this.defaultMaxAttempts = c.defaultMaxAttempts;
            this.claimBatchSize = c.claimBatchSize;
            this.defaultQueue = c.defaultQueue;
            this.storeOutagePollInterval = c.storeOutagePollInterval;
            this.maintenanceLeaseDuration = c.maintenanceLeaseDuration;
            this.nodeHeartbeatRetention = c.nodeHeartbeatRetention;
            this.checkInMinInterval = c.checkInMinInterval;
            this.noProgressTimeout = c.noProgressTimeout;
            this.queueFamilyDiscoveryInterval = c.queueFamilyDiscoveryInterval;
            this.queueFamilyRetentionAfterEmpty = c.queueFamilyRetentionAfterEmpty;
            this.logMaxRatePerSecond = c.logMaxRatePerSecond;
            this.logMaxEntries = c.logMaxEntries;
            this.logMaxBytes = c.logMaxBytes;
            this.maxDedupTtl = c.maxDedupTtl;
            this.shutdownGracePeriod = c.shutdownGracePeriod;
            this.maintenancePollInterval = c.maintenancePollInterval;
            this.retentionInterval = c.retentionInterval;
            this.succeededRetention = c.succeededRetention;
            this.failedRetention = c.failedRetention;
            this.deletedRetention = c.deletedRetention;
            this.quarantinedRetention = c.quarantinedRetention;
        }

        public Builder workerCount(int v) {
            this.workerCount = v;
            return this;
        }

        public Builder pollInterval(Duration v) {
            this.pollInterval = v;
            return this;
        }

        public Builder claimHeartbeat(Duration v) {
            this.claimHeartbeat = v;
            return this;
        }

        public Builder heartbeatTimeout(Duration v) {
            this.heartbeatTimeout = v;
            return this;
        }

        public Builder jobTimeout(Duration v) {
            this.jobTimeout = v;
            return this;
        }

        public Builder maxConsecutiveDispatcherFailures(int v) {
            this.maxConsecutiveDispatcherFailures = v;
            return this;
        }

        public Builder retryInitialBackoff(Duration v) {
            this.retryInitialBackoff = v;
            return this;
        }

        public Builder defaultMaxAttempts(int v) {
            this.defaultMaxAttempts = v;
            return this;
        }

        public Builder claimBatchSize(int v) {
            this.claimBatchSize = v;
            return this;
        }

        public Builder defaultQueue(String v) {
            this.defaultQueue = v;
            return this;
        }

        public Builder storeOutagePollInterval(Duration v) {
            this.storeOutagePollInterval = v;
            return this;
        }

        public Builder maintenanceLeaseDuration(Duration v) {
            this.maintenanceLeaseDuration = v;
            return this;
        }

        public Builder nodeHeartbeatRetention(Duration v) {
            this.nodeHeartbeatRetention = v;
            return this;
        }

        public Builder checkInMinInterval(Duration v) {
            this.checkInMinInterval = v;
            return this;
        }

        public Builder noProgressTimeout(Duration v) {
            this.noProgressTimeout = v;
            return this;
        }

        public Builder queueFamilyDiscoveryInterval(Duration v) {
            this.queueFamilyDiscoveryInterval = v;
            return this;
        }

        public Builder queueFamilyRetentionAfterEmpty(Duration v) {
            this.queueFamilyRetentionAfterEmpty = v;
            return this;
        }

        public Builder logMaxRatePerSecond(int v) {
            this.logMaxRatePerSecond = v;
            return this;
        }

        public Builder logMaxEntries(int v) {
            this.logMaxEntries = v;
            return this;
        }

        public Builder logMaxBytes(int v) {
            this.logMaxBytes = v;
            return this;
        }

        public Builder maxDedupTtl(Duration v) {
            this.maxDedupTtl = v;
            return this;
        }

        public Builder shutdownGracePeriod(Duration v) {
            this.shutdownGracePeriod = v;
            return this;
        }

        public Builder maintenancePollInterval(Duration v) {
            this.maintenancePollInterval = v;
            return this;
        }

        public Builder retentionInterval(Duration v) {
            this.retentionInterval = v;
            return this;
        }

        public Builder succeededRetention(Duration v) {
            this.succeededRetention = v;
            return this;
        }

        public Builder failedRetention(Duration v) {
            this.failedRetention = v;
            return this;
        }

        public Builder deletedRetention(Duration v) {
            this.deletedRetention = v;
            return this;
        }

        public Builder quarantinedRetention(Duration v) {
            this.quarantinedRetention = v;
            return this;
        }

        public ProcessingNodeConfig build() {
            return new ProcessingNodeConfig(
                    workerCount,
                    pollInterval,
                    claimHeartbeat,
                    heartbeatTimeout,
                    jobTimeout,
                    maxConsecutiveDispatcherFailures,
                    retryInitialBackoff,
                    defaultMaxAttempts,
                    claimBatchSize,
                    defaultQueue,
                    storeOutagePollInterval,
                    maintenanceLeaseDuration,
                    nodeHeartbeatRetention,
                    checkInMinInterval,
                    noProgressTimeout,
                    queueFamilyDiscoveryInterval,
                    queueFamilyRetentionAfterEmpty,
                    logMaxRatePerSecond,
                    logMaxEntries,
                    logMaxBytes,
                    maxDedupTtl,
                    shutdownGracePeriod,
                    maintenancePollInterval,
                    retentionInterval,
                    succeededRetention,
                    failedRetention,
                    deletedRetention,
                    quarantinedRetention);
        }
    }

    private static void requirePositive(String name, Duration value) {
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }
}
