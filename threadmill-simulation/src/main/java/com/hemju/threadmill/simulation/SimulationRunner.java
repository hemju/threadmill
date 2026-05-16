package com.hemju.threadmill.simulation;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.engine.QueueWeights;
import com.hemju.threadmill.core.handler.JobHandlerResolver;
import com.hemju.threadmill.core.handler.ReflectiveJobHandlerResolver;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.simulation.SimulationPayloads.Export;
import com.hemju.threadmill.simulation.SimulationPayloads.ExportHandler;
import com.hemju.threadmill.simulation.SimulationPayloads.Import;
import com.hemju.threadmill.simulation.SimulationPayloads.ImportHandler;

/**
 * Drives a complete simulation against one store: enqueues the workload,
 * starts a {@link ProcessingNode}, pauses a random queue mid-run and resumes
 * it later, and waits for everything to drain inside {@code config.runBudget()}.
 *
 * <p>Records every lifecycle event via {@link SimulationInterceptor} →
 * {@link TraceWriter}, plus the enqueue / pause / resume events that come
 * from the runner itself.
 */
public final class SimulationRunner {

    private static final Logger LOG = LoggerFactory.getLogger(SimulationRunner.class);

    private final JobStore store;
    private final SimulationConfig config;
    private final TraceWriter trace;
    private final JsonJobSerializer serializer = new JsonJobSerializer();
    private final String backendLabel;

    public SimulationRunner(JobStore store, SimulationConfig config, TraceWriter trace, String backendLabel) {
        this.store = Objects.requireNonNull(store, "store");
        this.config = Objects.requireNonNull(config, "config");
        this.trace = Objects.requireNonNull(trace, "trace");
        this.backendLabel = Objects.requireNonNull(backendLabel, "backendLabel");
    }

    public SimulationResult run() throws InterruptedException {
        Instant start = Instant.now();
        SimulationInterceptor interceptor = new SimulationInterceptor(trace);
        JobHandlerResolver resolver = new ReflectiveJobHandlerResolver();
        ProcessingNodeConfig nodeConfig = ProcessingNodeConfig.builder()
                .workerCount(config.workerCount())
                .pollInterval(Duration.ofMillis(20))
                .claimHeartbeat(Duration.ofMillis(200))
                .heartbeatTimeout(Duration.ofSeconds(5))
                .maintenanceLeaseDuration(Duration.ofSeconds(2))
                .jobTimeout(config.jobTimeout())
                .defaultMaxAttempts(3)
                .retryInitialBackoff(Duration.ofMillis(20))
                .claimBatchSize(8)
                .storeOutagePollInterval(Duration.ofMillis(200))
                .build();

        ProcessingNode node = ProcessingNode.builder(store)
                .config(nodeConfig)
                .handlerResolver(resolver)
                .interceptor(interceptor)
                .lane("project:*", config.workerCount(), QueueWeights.uniform())
                .build();
        trace.emit(
                "node_started",
                Map.of(
                        "nodeId", node.nodeId().toString(),
                        "workerCount", config.workerCount(),
                        "backend", backendLabel));
        node.start();

        try {
            // Enqueue the workload — half ad-hoc one-by-one, half via insertAll
            // so the bulk-insert path is exercised under contention with the
            // running engine.
            List<JobId> enqueuedIds = enqueueWorkload();

            // Pick a queue to pause mid-run, then resume after a moment.
            int pauseProject = ThreadLocalRandom.current().nextInt(config.projectCount());
            String pausedQueue = "project:" + pauseProject;
            Thread.sleep(150);
            store.pauseQueue(pausedQueue, "simulation mid-run pause");
            trace.emit("queue_paused", Map.of("queue", pausedQueue));
            Thread.sleep(400);
            store.resumeQueue(pausedQueue);
            trace.emit("queue_resumed", Map.of("queue", pausedQueue));

            // Wait for the engine to drain everything or the run budget to expire.
            Instant deadline = start.plus(config.runBudget());
            while (Instant.now().isBefore(deadline)) {
                long remaining = remainingActive();
                if (remaining == 0) break;
                Thread.sleep(100);
            }
            long remaining = remainingActive();
            return new SimulationResult(
                    backendLabel,
                    start,
                    Instant.now(),
                    enqueuedIds.size(),
                    interceptor.succeeded(),
                    interceptor.failed(),
                    interceptor.timedOut(),
                    interceptor.quarantined(),
                    remaining);
        } finally {
            node.close();
            trace.emit("node_stopped", Map.of("nodeId", node.nodeId().toString()));
        }
    }

    private List<JobId> enqueueWorkload() {
        int imports = (int) Math.round(config.totalJobs() * config.importFraction());
        int exports = config.totalJobs() - imports;
        Random rng = new Random(0xC0FFEEL);

        List<Job> singles = new ArrayList<>(config.totalJobs() / 2);
        List<Job> bulk = new ArrayList<>(config.totalJobs() / 2);
        for (int i = 0; i < imports; i++) {
            int projectId = rng.nextInt(config.projectCount());
            Job job = buildImportJob(projectId);
            (i % 2 == 0 ? singles : bulk).add(job);
        }
        for (int i = 0; i < exports; i++) {
            int projectId = rng.nextInt(config.projectCount());
            Job job = buildExportJob(projectId);
            (i % 2 == 0 ? singles : bulk).add(job);
        }

        List<JobId> ids = new ArrayList<>(config.totalJobs());
        for (Job j : singles) {
            store.insert(j);
            ids.add(j.id());
            emitEnqueued(j);
        }
        store.insertAll(bulk);
        for (Job j : bulk) {
            ids.add(j.id());
            emitEnqueued(j);
        }
        return ids;
    }

    private void emitEnqueued(Job j) {
        var fields = new LinkedHashMap<String, Object>();
        fields.put("jobId", j.id().toString());
        fields.put("queue", j.queue());
        fields.put("type", j.spec().handlerType().endsWith("ImportHandler") ? "import" : "export");
        fields.put("lockKey", j.concurrencyKey().orElse(null));
        fields.put("lockMode", j.concurrencyMode().map(Enum::name).orElse(null));
        trace.emit("enqueued", fields);
    }

    private Job buildImportJob(int projectId) {
        Import payload = new Import(
                projectId,
                config.importDuration().toMillis(),
                config.failureRate(),
                config.hangRate(),
                config.jobTimeout().plusMillis(200).toMillis());
        JobArgument arg = serializer.serializePayload(payload);
        return Job.builder()
                .spec(new JobSpec(ImportHandler.class.getName(), List.of(arg)))
                .queue("project:" + projectId)
                .concurrencyKey("project:" + projectId)
                .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
                .build();
    }

    private Job buildExportJob(int projectId) {
        Export payload = new Export(
                projectId,
                config.exportDuration().toMillis(),
                config.failureRate(),
                config.hangRate(),
                config.jobTimeout().plusMillis(200).toMillis());
        JobArgument arg = serializer.serializePayload(payload);
        return Job.builder()
                .spec(new JobSpec(ExportHandler.class.getName(), List.of(arg)))
                .queue("project:" + projectId)
                .concurrencyKey("project:" + projectId)
                .concurrencyMode(ConcurrencyMode.SHARED)
                .build();
    }

    private long remainingActive() {
        var counts = store.countsByState();
        long active = 0;
        for (JobState s : List.of(JobState.ENQUEUED, JobState.SCHEDULED, JobState.AWAITING, JobState.PROCESSING)) {
            active += counts.getOrDefault(s, 0L);
        }
        return active;
    }

    public record SimulationResult(
            String backend,
            Instant startedAt,
            Instant endedAt,
            int enqueued,
            long succeeded,
            long failed,
            long timedOut,
            long quarantined,
            long stillActive) {

        public Duration duration() {
            return Duration.between(startedAt, endedAt);
        }

        public boolean drained() {
            return stillActive == 0;
        }
    }
}
