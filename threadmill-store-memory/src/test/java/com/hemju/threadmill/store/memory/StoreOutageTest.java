package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobReplacement;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.OversizedJobException;
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.core.store.JobStoreCapabilities;
import com.hemju.threadmill.core.store.NodeHeartbeat;

/**
 * Wraps the in-memory store with a fault-injecting delegate so the engine
 * sees a real store-outage signature, then asserts:
 * <ul>
 *   <li>the engine pauses processing during the outage,</li>
 *   <li>the engine resumes once the outage clears,</li>
 *   <li>no node crash,</li>
 *   <li>previously-enqueued jobs are still processed afterward.</li>
 * </ul>
 */
class StoreOutageTest {

    private ProcessingNode node;
    private final JsonJobSerializer serializer = new JsonJobSerializer();

    @AfterEach
    void tearDown() {
        if (node != null) node.close();
    }

    @Test
    void clusterPausesAndResumesAroundAStoreOutage() {
        EngineTestHandlers.reset();
        var outage = new AtomicBoolean(false);
        JobStore inner = new InMemoryJobStore();
        JobStore flaky = new FlakyStore(inner, outage);

        // Enqueue a job BEFORE starting the engine.
        JobArgument arg = serializer.serializePayload(new EngineTestHandlers.HelloPayload("x"));
        Job job = Job.builder()
                .spec(new JobSpec(EngineTestHandlers.CountingHandler.class.getName(), List.of(arg)))
                .build();
        inner.insert(job);

        ProcessingNodeConfig fast = ProcessingNodeConfig.builder()
                .workerCount(2)
                .pollInterval(Duration.ofMillis(50))
                .claimHeartbeat(Duration.ofMillis(100))
                .heartbeatTimeout(Duration.ofMillis(500))
                .jobTimeout(Duration.ofSeconds(2))
                .defaultMaxAttempts(2)
                .retryInitialBackoff(Duration.ofMillis(50))
                .storeOutagePollInterval(Duration.ofMillis(100))
                .maxConsecutiveDispatcherFailures(2)
                .build();
        node = ProcessingNode.builder(flaky).config(fast).build();

        // Trigger outage BEFORE starting; the dispatcher's first ticks fail
        // and trip the breaker, pausing the loop.
        outage.set(true);
        node.start();

        // While outage is on, the job should not be processed.
        try {
            Thread.sleep(500);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        Job stillEnqueued = inner.findById(job.id()).orElseThrow();
        assertThat(stillEnqueued.currentState()).isEqualTo(JobState.ENQUEUED);

        // Clear the outage. The loop must auto-resume on the next probe.
        outage.set(false);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Job loaded = inner.findById(job.id()).orElseThrow();
            assertThat(loaded.currentState()).isEqualTo(JobState.SUCCEEDED);
        });
    }

    /** Wraps a real store and throws on every call while {@code outage} is true. */
    private static final class FlakyStore implements JobStore {
        private final JobStore delegate;
        private final AtomicBoolean outage;

        FlakyStore(JobStore delegate, AtomicBoolean outage) {
            this.delegate = delegate;
            this.outage = outage;
        }

        private void check() {
            if (outage.get()) throw new RuntimeException("store unreachable");
        }

        @Override
        public JobStoreCapabilities capabilities() {
            check();
            return delegate.capabilities();
        }

        @Override
        public void insert(Job job) {
            check();
            delegate.insert(job);
        }

        @Override
        public List<JobId> insertAll(List<Job> jobs) {
            check();
            return delegate.insertAll(jobs);
        }

        @Override
        public EnqueueResult enqueueIfAbsent(Job job, String dedupKey, Duration ttl, Instant now) {
            check();
            return delegate.enqueueIfAbsent(job, dedupKey, ttl, now);
        }

        @Override
        public Optional<Job> findById(JobId id) {
            check();
            return delegate.findById(id);
        }

        @Override
        public void saveAtomic(Job job, long expectedVersion) throws StaleJobException, OversizedJobException {
            check();
            delegate.saveAtomic(job, expectedVersion);
        }

        @Override
        public boolean softDelete(JobId id) {
            check();
            return delegate.softDelete(id);
        }

        @Override
        public List<Job> claimReady(NodeId nodeId, String queue, int max, Instant heartbeatAt) {
            check();
            return delegate.claimReady(nodeId, queue, max, heartbeatAt);
        }

        @Override
        public void pauseQueue(String queue, String reason) {
            check();
            delegate.pauseQueue(queue, reason);
        }

        @Override
        public void resumeQueue(String queue) {
            check();
            delegate.resumeQueue(queue);
        }

        @Override
        public Set<String> listPausedQueues() {
            check();
            return delegate.listPausedQueues();
        }

        @Override
        public void touchOwnerHeartbeat(NodeId nodeId, Instant now) {
            check();
            delegate.touchOwnerHeartbeat(nodeId, now);
        }

        @Override
        public boolean saveExecutionUpdate(Job job, NodeId nodeId) {
            check();
            return delegate.saveExecutionUpdate(job, nodeId);
        }

        @Override
        public void recordNodeHeartbeat(NodeId nodeId, Instant now) {
            check();
            delegate.recordNodeHeartbeat(nodeId, now);
        }

        @Override
        public Optional<Instant> readNodeHeartbeat(NodeId nodeId) {
            check();
            return delegate.readNodeHeartbeat(nodeId);
        }

        @Override
        public boolean acquireOrRenewMaintenanceLease(NodeId nodeId, Duration leaseDuration) {
            check();
            return delegate.acquireOrRenewMaintenanceLease(nodeId, leaseDuration);
        }

        @Override
        public void releaseMaintenanceLease(NodeId nodeId) {
            check();
            delegate.releaseMaintenanceLease(nodeId);
        }

        @Override
        public Optional<NodeId> readMaintenanceLeaseOwner() {
            check();
            return delegate.readMaintenanceLeaseOwner();
        }

        @Override
        public List<Job> findDueForPromotion(Instant now, int max) {
            check();
            return delegate.findDueForPromotion(now, max);
        }

        @Override
        public List<Job> findOrphaned(Instant heartbeatExpiry, int max) {
            check();
            return delegate.findOrphaned(heartbeatExpiry, max);
        }

        @Override
        public Map<JobState, Long> countsByState() {
            check();
            return delegate.countsByState();
        }

        @Override
        public Map<String, Long> queueDepths() {
            check();
            return delegate.queueDepths();
        }

        @Override
        public List<String> listEnqueuedQueues() {
            check();
            return delegate.listEnqueuedQueues();
        }

        @Override
        public Optional<Instant> oldestEnqueuedAt(String queue) {
            check();
            return delegate.oldestEnqueuedAt(queue);
        }

        @Override
        public Optional<Instant> oldestProcessingHeartbeat() {
            check();
            return delegate.oldestProcessingHeartbeat();
        }

        @Override
        public List<NodeHeartbeat> listNodeHeartbeats() {
            check();
            return delegate.listNodeHeartbeats();
        }

        @Override
        public long deleteNodeHeartbeatsOlderThan(Instant cutoff) {
            check();
            return delegate.deleteNodeHeartbeatsOlderThan(cutoff);
        }

        @Override
        public long deleteExpiredDedupKeys(Instant now, int max) {
            check();
            return delegate.deleteExpiredDedupKeys(now, max);
        }

        @Override
        public List<Job> findByHandlerSignature(String handlerType, int max) {
            check();
            return delegate.findByHandlerSignature(handlerType, max);
        }

        @Override
        public long deleteFinishedOlderThan(Instant cutoff, JobState state, int max) {
            check();
            return delegate.deleteFinishedOlderThan(cutoff, state, max);
        }

        @Override
        public void upsertCronTask(com.hemju.threadmill.core.schedule.CronTask t) {
            check();
            delegate.upsertCronTask(t);
        }

        @Override
        public Optional<com.hemju.threadmill.core.schedule.CronTask> findCronTask(String name) {
            check();
            return delegate.findCronTask(name);
        }

        @Override
        public List<com.hemju.threadmill.core.schedule.CronTask> listCronTasks() {
            check();
            return delegate.listCronTasks();
        }

        @Override
        public void deleteCronTask(String name) {
            check();
            delegate.deleteCronTask(name);
        }

        @Override
        public void upsertCronTaskState(com.hemju.threadmill.core.schedule.CronTaskScheduleState s) {
            check();
            delegate.upsertCronTaskState(s);
        }

        @Override
        public Optional<com.hemju.threadmill.core.schedule.CronTaskScheduleState> findCronTaskState(String n) {
            check();
            return delegate.findCronTaskState(n);
        }

        @Override
        public List<Job> findAwaitingByParent(JobId parentId, int max) {
            check();
            return delegate.findAwaitingByParent(parentId, max);
        }

        @Override
        public boolean tryAcquireMutex(String name, String holder, Duration lease) {
            check();
            return delegate.tryAcquireMutex(name, holder, lease);
        }

        @Override
        public void releaseMutex(String name, String holder) {
            check();
            delegate.releaseMutex(name, holder);
        }

        @Override
        public boolean replaceJob(JobId id, long ev, JobReplacement r) {
            check();
            return delegate.replaceJob(id, ev, r);
        }
    }
}
