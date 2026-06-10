package com.hemju.threadmill.core.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.store.JobStore;

/**
 * The per-node dispatcher loop: figures out how many workers are free,
 * claims that many ready jobs from the {@link JobStore}, and submits them
 * to the worker pool.
 *
 * <p>The loop is robust against transient store outages: a thrown
 * exception trips the circuit breaker, the loop pauses, and a probe
 * thread re-attempts {@link JobStore#verifyWritable()} until it succeeds —
 * at which point processing resumes automatically.
 */
public final class Dispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(Dispatcher.class);

    /** Metadata key that carries a job's required-node-tag set as a comma-separated list. */
    public static final String REQUIRED_TAGS_META = "threadmill.tags.required";

    private final JobStore store;
    private final NodeId nodeId;
    private final JobRunner runner;
    private final ExecutorService workerPool;
    private final Semaphore workerCapacity;
    private final ProcessingNodeConfig config;
    private final Set<String> nodeTags;
    private final QueueFamily queueFamily;
    private final Map<String, FamilyQueue> familyQueues = new HashMap<>();
    private Instant nextDiscoveryAt = Instant.EPOCH;
    private Set<String> pausedQueues = Set.of();
    private final CircuitBreaker breaker;
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<Thread> loopThread = new AtomicReference<>();
    private final WakeSignal wakeSignal = new WakeSignal();

    public Dispatcher(
            JobStore store,
            NodeId nodeId,
            JobRunner runner,
            ExecutorService workerPool,
            Semaphore workerCapacity,
            ProcessingNodeConfig config) {
        this(store, nodeId, runner, workerPool, workerCapacity, config, Set.of());
    }

    public Dispatcher(
            JobStore store,
            NodeId nodeId,
            JobRunner runner,
            ExecutorService workerPool,
            Semaphore workerCapacity,
            ProcessingNodeConfig config,
            Set<String> nodeTags) {
        this(store, nodeId, runner, workerPool, workerCapacity, config, nodeTags, null);
    }

    public Dispatcher(
            JobStore store,
            NodeId nodeId,
            JobRunner runner,
            ExecutorService workerPool,
            Semaphore workerCapacity,
            ProcessingNodeConfig config,
            Set<String> nodeTags,
            QueueFamily queueFamily) {
        this.store = Objects.requireNonNull(store, "store");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.runner = Objects.requireNonNull(runner, "runner");
        this.workerPool = Objects.requireNonNull(workerPool, "workerPool");
        this.workerCapacity = Objects.requireNonNull(workerCapacity, "workerCapacity");
        this.config = Objects.requireNonNull(config, "config");
        this.nodeTags = Set.copyOf(Objects.requireNonNull(nodeTags, "nodeTags"));
        this.queueFamily = queueFamily;
        this.breaker = new CircuitBreaker(config.maxConsecutiveDispatcherFailures());
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;
        Thread t = Thread.ofPlatform()
                .name("threadmill-dispatcher-" + nodeId)
                .daemon(true)
                .start(this::loop);
        loopThread.set(t);
    }

    public void stop() {
        running.set(false);
        wakeSignal.signal();
        Thread t = loopThread.getAndSet(null);
        if (t != null) t.interrupt();
    }

    public boolean isPaused() {
        return paused.get();
    }

    /**
     * Returns {@code true} if this dispatcher handles {@code queue} — either as
     * its fixed queue or as a match against its queue-family pattern. Used by
     * {@link ProcessingNode#wake(String)} to route a producer-side wake to the
     * right lane.
     */
    boolean matches(String queue) {
        if (queueFamily != null) return queueFamily.matches(queue);
        return config.defaultQueue().equals(queue);
    }

    /**
     * Wake this dispatcher out of its poll-interval sleep. Opportunistic —
     * coalesced by {@link WakeSignal}'s single-permit cap; safe to call from
     * any thread on every enqueue.
     */
    void wake() {
        wakeSignal.signal();
    }

    private void loop() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                if (paused.get()) {
                    if (probeStore()) {
                        LOG.info("Store reachable again — resuming dispatcher");
                        paused.set(false);
                        breaker.reset();
                    } else {
                        sleep(config.storeOutagePollInterval());
                        continue;
                    }
                }
                doOnePoll();
                breaker.recordSuccess();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Throwable t) {
                if (!running.get()) {
                    break;
                }
                LOG.warn("Dispatcher poll failed", t);
                if (breaker.recordFailure()) {
                    LOG.error(
                            "Dispatcher hit its failure threshold ({}) — pausing until the store is reachable",
                            config.maxConsecutiveDispatcherFailures());
                    paused.set(true);
                }
                sleep(config.pollInterval());
            }
        }
    }

    private void doOnePoll() throws InterruptedException {
        int budget = workerCapacity.availablePermits();
        if (budget <= 0) {
            wakeSignal.awaitFor(config.pollInterval());
            return;
        }
        pausedQueues = store.listPausedQueues();
        int max = Math.min(budget, config.claimBatchSize());
        List<Job> claimed = queueFamily == null ? claimFixedQueue(max) : claimQueueFamily(max);
        if (claimed.isEmpty()) {
            wakeSignal.awaitFor(config.pollInterval());
            return;
        }
        dispatchClaimed(claimed);
    }

    /**
     * Submit each claimed job to the worker pool with per-job error
     * isolation. A failure dispatching one job must not abandon the
     * remaining claimed jobs in PROCESSING — the node-wide owner heartbeat
     * would shield them from orphan reclaim indefinitely. A job that cannot
     * be dispatched is released back via the SCHEDULED-with-short-backoff
     * path, and a submit failure after the permit was acquired releases the
     * permit so lane capacity cannot leak.
     */
    private void dispatchClaimed(List<Job> claimed) throws InterruptedException {
        for (int i = 0; i < claimed.size(); i++) {
            Job job = claimed.get(i);
            try {
                if (!eligibleByTags(job)) {
                    releaseClaimed(job, "engine.tag-mismatch");
                    continue;
                }
                try {
                    workerCapacity.acquire();
                } catch (InterruptedException interrupted) {
                    releaseRemaining(claimed, i);
                    throw interrupted;
                }
                boolean submitted = false;
                try {
                    workerPool.submit(() -> {
                        try {
                            runner.run(job);
                        } finally {
                            workerCapacity.release();
                            // Signal only on the transition from "all busy" to
                            // "at least one idle" — otherwise high-churn steady-state
                            // load would burn CPU on permits-already-pending releases.
                            // The single-permit cap inside WakeSignal coalesces races
                            // between concurrent finishers.
                            if (workerCapacity.availablePermits() == 1) {
                                wakeSignal.signal();
                            }
                        }
                    });
                    submitted = true;
                } finally {
                    if (!submitted) {
                        workerCapacity.release();
                    }
                }
            } catch (InterruptedException interrupted) {
                throw interrupted;
            } catch (Throwable t) {
                LOG.warn("Failed to dispatch claimed job {} — releasing it for redelivery", job.id(), t);
                releaseClaimed(job, "engine.dispatch-failure");
            }
        }
    }

    private void releaseRemaining(List<Job> claimed, int fromIndex) {
        for (int i = fromIndex; i < claimed.size(); i++) {
            releaseClaimed(claimed.get(i), "engine.dispatch-failure");
        }
    }

    private List<Job> claimFixedQueue(int max) {
        if (pausedQueues.contains(config.defaultQueue())) {
            return List.of();
        }
        return store.claimReady(nodeId, config.defaultQueue(), max, Instant.now());
    }

    private List<Job> claimQueueFamily(int max) {
        var now = Instant.now();
        discoverFamilyQueues(now);
        int attempts = Math.max(1, familyQueues.size());
        for (int i = 0; i < attempts; i++) {
            FamilyQueue queue = pickFamilyQueue();
            if (queue == null) {
                return List.of();
            }
            if (pausedQueues.contains(queue.name)) {
                if (queue.emptySince == null) {
                    queue.emptySince = now;
                }
                continue;
            }
            List<Job> claimed = store.claimReady(nodeId, queue.name, max, now);
            if (!claimed.isEmpty()) {
                queue.emptySince = null;
                return claimed;
            }
            if (queue.emptySince == null) {
                queue.emptySince = now;
            }
        }
        return List.of();
    }

    private void discoverFamilyQueues(Instant now) {
        if (now.isBefore(nextDiscoveryAt)) {
            removeRetainedEmptyQueues(now);
            return;
        }
        nextDiscoveryAt = now.plus(config.queueFamilyDiscoveryInterval());
        var seen = new HashSet<String>();
        for (String queue : store.listEnqueuedQueues()) {
            if (!queueFamily.matches(queue)) continue;
            seen.add(queue);
            int weight = queueFamily.weights().weightFor(queue);
            familyQueues.compute(queue, (name, existing) -> {
                FamilyQueue updated = existing == null ? new FamilyQueue(name) : existing;
                updated.weight = weight;
                if (weight > 0) {
                    updated.emptySince = null;
                }
                return updated;
            });
        }
        for (FamilyQueue queue : familyQueues.values()) {
            if (!seen.contains(queue.name) && queue.emptySince == null) {
                queue.emptySince = now;
            }
        }
        removeRetainedEmptyQueues(now);
    }

    private void removeRetainedEmptyQueues(Instant now) {
        familyQueues
                .entrySet()
                .removeIf(e -> e.getValue().emptySince != null
                        && !e.getValue()
                                .emptySince
                                .plus(config.queueFamilyRetentionAfterEmpty())
                                .isAfter(now));
    }

    private FamilyQueue pickFamilyQueue() {
        FamilyQueue best = null;
        for (FamilyQueue queue : familyQueues.values()) {
            if (queue.weight <= 0) continue;
            if (best == null || queue.pass < best.pass) {
                best = queue;
            }
        }
        if (best == null) {
            return null;
        }
        best.pass += Math.max(1L, 10_000L / best.weight);
        return best;
    }

    /**
     * Returns {@code true} if this node's tag set satisfies the job's required-tag set.
     * A job without required tags is eligible everywhere. A node with no tags can only
     * run jobs that have no required tags.
     */
    private boolean eligibleByTags(Job job) {
        var required = job.metadata().get(REQUIRED_TAGS_META);
        if (required.isEmpty() || required.get().isBlank()) return true;
        Set<String> wanted = new HashSet<>(Arrays.asList(required.get().split("\\s*,\\s*")));
        wanted.remove("");
        return nodeTags.containsAll(wanted);
    }

    private void releaseClaimed(Job job, String reason) {
        // Re-schedule with a small backoff so this node doesn't immediately re-claim
        // a job it can't run, busy-looping it between ENQUEUED and PROCESSING. A
        // properly-tagged node either picks up the rescheduled job promptly, or
        // the promotion loop puts it back on the queue and another node tries.
        try {
            long v = job.version();
            job.transitionTo(JobState.SCHEDULED, Instant.now(), reason, null);
            job.scheduleAt(Instant.now().plusSeconds(2));
            job.clearOwner();
            store.saveAtomic(job, v);
        } catch (StaleJobException ignored) {
            // Another node already claimed it
        } catch (Throwable t) {
            LOG.warn("Could not release claimed job {} — it stays PROCESSING until reclaimed", job.id(), t);
        }
    }

    private boolean probeStore() {
        try {
            store.verifyWritable();
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    private static void sleep(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class FamilyQueue {
        final String name;
        int weight = 1;
        long pass;
        Instant emptySince;

        FamilyQueue(String name) {
            this.name = name;
        }
    }
}
