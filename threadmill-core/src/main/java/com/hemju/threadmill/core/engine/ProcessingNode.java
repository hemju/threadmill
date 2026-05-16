package com.hemju.threadmill.core.engine;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.Names;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.handler.JobHandlerResolver;
import com.hemju.threadmill.core.handler.ReflectiveJobHandlerResolver;
import com.hemju.threadmill.core.schedule.RecurringMaterializer;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.store.JobStore;

/**
 * The per-application-instance engine: bundles the {@link NodeRegistry},
 * one {@link Dispatcher} per configured {@link QueueLane},
 * {@link MaintenanceCycle}, {@link JobRunner}, and the worker pool.
 *
 * <p>Threadmill provides <strong>at-least-once</strong> delivery. A job
 * may run more than once — after a node crash, after a long GC pause that
 * makes the heartbeat look expired, or after a store outage that resets
 * a partial save. Handlers must be idempotent.
 */
public final class ProcessingNode implements AutoCloseable {

    private final NodeId nodeId;
    private final ProcessingNodeConfig config;
    private final JobStore store;
    private final JobSerializer serializer;
    private final ExecutorService workerPool;
    private final JobInterceptors interceptors;
    private final RetryInterceptor retryInterceptor;
    private final JobRunner runner;
    private final NodeRegistry registry;
    private final List<Dispatcher> dispatchers = new ArrayList<>();
    private final List<QueueLane> lanes;
    private final MaintenanceCycle maintenance;
    private final LocalWakeBus wakeBus;
    private final Runnable wakeRegistration;
    private final Set<String> tags;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public static Builder builder(JobStore store) {
        return new Builder(store);
    }

    private ProcessingNode(Builder b) {
        this.store = Objects.requireNonNull(b.store, "store");
        this.config = b.config == null ? ProcessingNodeConfig.defaults() : b.config;
        this.serializer = b.serializer == null ? new JsonJobSerializer() : b.serializer;
        this.nodeId = b.nodeId == null ? NodeId.newId() : b.nodeId;
        this.wakeBus = b.wakeBus == null ? new LocalWakeBus() : b.wakeBus;
        JobHandlerResolver resolver = b.resolver == null ? new ReflectiveJobHandlerResolver() : b.resolver;
        this.workerPool = Executors.newVirtualThreadPerTaskExecutor();
        this.interceptors = new JobInterceptors();
        this.retryInterceptor = new RetryInterceptor(store, config.defaultMaxAttempts(), config.retryInitialBackoff());
        b.exceptionPolicies.forEach(retryInterceptor::policyFor);
        this.interceptors.add(retryInterceptor);
        this.interceptors.add(new WorkflowInterceptor(store));
        b.userInterceptors.forEach(interceptors::add);
        this.tags = Set.copyOf(b.tags);

        this.runner = new JobRunner(store, nodeId, resolver, serializer, interceptors, config);
        this.registry = new NodeRegistry(
                store, nodeId, config.heartbeatTimeout(), config.claimHeartbeat(), config.maintenanceLeaseDuration());

        // Build one Dispatcher per configured lane. Each gets its own Semaphore,
        // which is the engine's defence against starvation: a flood of jobs on
        // one queue cannot occupy capacity reserved for another.
        this.lanes = b.lanes.isEmpty()
                ? List.of(new QueueLane(config.defaultQueue(), config.workerCount()))
                : List.copyOf(b.lanes);
        for (QueueLane lane : lanes) {
            ProcessingNodeConfig laneConfig = config.toBuilder()
                    .defaultQueue(lane.queue())
                    .workerCount(lane.workers())
                    .build();
            var capacity = new Semaphore(lane.workers());
            dispatchers.add(
                    new Dispatcher(store, nodeId, runner, workerPool, capacity, laneConfig, tags, lane.family()));
        }
        this.wakeRegistration = wakeBus.register(this::wake);

        var materializer = new RecurringMaterializer(store, wakeBus);
        this.maintenance = new MaintenanceCycle(store, nodeId, registry, runner, materializer, config, wakeBus);
    }

    public NodeId nodeId() {
        return nodeId;
    }

    public ProcessingNodeConfig config() {
        return config;
    }

    public JobStore store() {
        return store;
    }

    public JobInterceptors interceptors() {
        return interceptors;
    }

    /** Persist a freshly-built job. Adopted version is updated on the input. */
    public void enqueue(Job job) {
        store.insert(job);
        if (job.currentState() == JobState.ENQUEUED) {
            wakeBus.wake(job.queue());
        }
    }

    public Optional<Job> findById(JobId id) {
        return store.findById(id);
    }

    public void start() {
        if (!started.compareAndSet(false, true)) return;
        registry.start();
        try {
            Thread.sleep(50);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        for (Dispatcher d : dispatchers) d.start();
        maintenance.start();
    }

    @Override
    public void close() {
        if (!stopped.compareAndSet(false, true)) return;
        for (Dispatcher d : dispatchers) d.stop();
        maintenance.stop();
        wakeRegistration.run();
        registry.stop();
        workerPool.shutdown();
        try {
            if (!workerPool.awaitTermination(config.shutdownGracePeriod().toMillis(), TimeUnit.MILLISECONDS)) {
                workerPool.shutdownNow();
            }
        } catch (InterruptedException ie) {
            workerPool.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            runner.shutdown();
        }
    }

    /** Returns the set of tags this node advertises. */
    public Set<String> tags() {
        return tags;
    }

    /** Returns the queue lanes this node polls. Each lane has its own dispatcher and capacity. */
    public List<QueueLane> lanes() {
        return lanes;
    }

    /**
     * Wake the local dispatcher(s) handling {@code queue}. Opportunistic — the
     * dispatcher will pick the job up on its next poll regardless; this just
     * eliminates the {@code pollInterval} wait when the producer is in the
     * same JVM. Cross-node wakes are not part of this hook.
     *
     * <p>Typically called via {@link LocalWakeBus} after each producer-side
     * insert that puts a job into {@code ENQUEUED} state.
     */
    public void wake(String queue) {
        for (Dispatcher d : dispatchers) {
            if (d.matches(queue)) d.wake();
        }
    }

    /** Builder for a {@link ProcessingNode}. */
    public static final class Builder {
        private final JobStore store;
        private NodeId nodeId;
        private ProcessingNodeConfig config;
        private JobHandlerResolver resolver;
        private JobSerializer serializer;
        private LocalWakeBus wakeBus;
        private final List<JobInterceptor> userInterceptors = new ArrayList<>();
        private final List<QueueLane> lanes = new ArrayList<>();
        private final Set<String> tags = new LinkedHashSet<>();
        private final LinkedHashMap<Class<? extends Throwable>, RetryPolicy> exceptionPolicies = new LinkedHashMap<>();

        private Builder(JobStore store) {
            this.store = Objects.requireNonNull(store, "store");
        }

        public Builder nodeId(NodeId v) {
            this.nodeId = v;
            return this;
        }

        public Builder config(ProcessingNodeConfig v) {
            this.config = v;
            return this;
        }

        public Builder handlerResolver(JobHandlerResolver v) {
            this.resolver = v;
            return this;
        }

        public Builder serializer(JobSerializer v) {
            this.serializer = v;
            return this;
        }

        public Builder wakeBus(LocalWakeBus v) {
            this.wakeBus = v;
            return this;
        }

        public Builder interceptor(JobInterceptor v) {
            this.userInterceptors.add(v);
            return this;
        }

        public Builder lane(QueueLane v) {
            this.lanes.add(v);
            return this;
        }

        public Builder lane(String queue, int workers) {
            return lane(new QueueLane(queue, workers));
        }

        public Builder lane(String pattern, int workers, QueueWeights weights) {
            return lane(QueueLane.family(pattern, workers, weights));
        }

        public Builder tag(String tag) {
            this.tags.add(Names.requireName("tag", tag));
            return this;
        }

        public Builder retryPolicyFor(Class<? extends Throwable> exceptionType, RetryPolicy policy) {
            this.exceptionPolicies.put(exceptionType, policy);
            return this;
        }

        public ProcessingNode build() {
            return new ProcessingNode(this);
        }
    }
}
