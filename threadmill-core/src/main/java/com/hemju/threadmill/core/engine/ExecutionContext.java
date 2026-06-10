package com.hemju.threadmill.core.engine;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobLog;
import com.hemju.threadmill.core.JobMetadata;
import com.hemju.threadmill.core.JobProgress;
import com.hemju.threadmill.core.JobResult;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.store.JobStore;

/**
 * The engine's concrete {@link JobExecutionContext}. Carries the running
 * job's identity, the owning node, the attempt number, the start time, and
 * the user-touchable surfaces (log, progress, metadata, result).
 *
 * <p>This is published to handler code via {@link EngineScopedValues} so
 * code running on a virtual thread the handler spawns can still resolve
 * the current job's id and attempt.
 */
public final class ExecutionContext implements JobExecutionContext {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutionContext.class);

    private final Job job;
    private final JobStore store;
    private final JobId jobId;
    private final NodeId nodeId;
    private final int attempt;
    private final Instant claimedAt;
    private final JobLog log;
    private final JobProgress progress;
    private final JobMetadata metadata;
    private final JobSerializer serializer;
    private final ProcessingNodeConfig config;
    private final AtomicReference<JobResult> result = new AtomicReference<>();
    private final AtomicReference<Object> resultObject = new AtomicReference<>();
    private final AtomicReference<Instant> lastCheckIn = new AtomicReference<>();
    private volatile Instant lastPersistedAt = Instant.EPOCH;
    private volatile long logWindowSecond = -1L;
    private volatile int acceptedLogCount;
    private final AtomicLong droppedLogCount = new AtomicLong();

    public ExecutionContext(
            Job job,
            JobStore store,
            JobId jobId,
            NodeId nodeId,
            int attempt,
            Instant claimedAt,
            JobLog log,
            JobProgress progress,
            JobMetadata metadata,
            JobSerializer serializer,
            ProcessingNodeConfig config) {
        this.job = Objects.requireNonNull(job, "job");
        this.store = Objects.requireNonNull(store, "store");
        this.jobId = jobId;
        this.nodeId = nodeId;
        this.attempt = attempt;
        this.claimedAt = claimedAt;
        this.log = log;
        this.progress = progress;
        this.metadata = metadata;
        this.serializer = serializer;
        this.config = Objects.requireNonNull(config, "config");
        this.log.configureBounds(config.logMaxEntries(), config.logMaxBytes());
    }

    @Override
    public JobId jobId() {
        return jobId;
    }

    @Override
    public NodeId nodeId() {
        return nodeId;
    }

    @Override
    public int attempt() {
        return attempt;
    }

    @Override
    public Instant claimedAt() {
        return claimedAt;
    }

    @Override
    public JobLog log() {
        return log;
    }

    @Override
    public JobProgress progress() {
        return progress;
    }

    @Override
    public JobMetadata metadata() {
        return metadata;
    }

    @Override
    public void setResult(Object value) {
        if (value == null) {
            result.set(null);
            resultObject.set(null);
            return;
        }
        JobArgument arg = serializer.serializeArgument(value);
        result.set(new JobResult(arg.typeTag(), arg.serialized()));
        resultObject.set(value);
    }

    @Override
    public Optional<Object> readResult() {
        return Optional.ofNullable(resultObject.get());
    }

    /** Engine-internal accessor for the {@link JobResult} produced by the handler. */
    public JobResult capturedResult() {
        return result.get();
    }

    @Override
    public void checkIn() {
        var now = Instant.now();
        job.checkIn(now);
        lastCheckIn.set(now);
        flushIfDue(now);
    }

    @Override
    public void checkIn(String message) {
        checkIn();
        log(message);
    }

    @Override
    public void updateProgress(double fractionComplete) {
        progress.update(fractionComplete);
        flushIfDue(Instant.now());
    }

    @Override
    public void log(String message) {
        var now = Instant.now();
        if (acceptLog(now)) {
            log.info(message);
            flushIfDue(now);
        } else {
            droppedLogCount.incrementAndGet();
        }
    }

    public Optional<Instant> lastCheckInAt() {
        return Optional.ofNullable(lastCheckIn.get());
    }

    public long droppedLogCount() {
        return droppedLogCount.get();
    }

    public void flushBestEffort() {
        try {
            store.saveExecutionUpdate(job, nodeId);
            lastPersistedAt = Instant.now();
        } catch (Throwable t) {
            LOG.debug("Threadmill check-in flush failed for job {}", jobId, t);
        }
    }

    private void flushIfDue(Instant now) {
        if (!lastPersistedAt.plus(config.checkInMinInterval()).isAfter(now)) {
            flushBestEffort();
        }
    }

    private synchronized boolean acceptLog(Instant now) {
        long second = now.getEpochSecond();
        if (second != logWindowSecond) {
            logWindowSecond = second;
            acceptedLogCount = 0;
        }
        if (acceptedLogCount >= config.logMaxRatePerSecond()) return false;
        acceptedLogCount++;
        return true;
    }
}
