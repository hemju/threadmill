package com.hemju.threadmill.simulation.workerchurn;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.engine.QueueLane;
import com.hemju.threadmill.core.schedule.Scheduler;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.store.JobStore;

/**
 * Multi-process worker-churn simulation for Postgres and Redis.
 *
 * <p>The supervisor enqueues jobs, starts several worker JVMs, repeatedly
 * kills and recreates workers, and writes a JSON-lines trace file for later review.
 */
public final class WorkerChurnSimulatorMain {

    private WorkerChurnSimulatorMain() {}

    static void main(String[] args) throws Exception {
        var options = Options.parse(args);
        if (options.workerMode) {
            runWorker(options);
        } else {
            runSupervisor(options);
        }
    }

    private static void runSupervisor(Options options) throws Exception {
        String runId = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
        if (options.queue == null) {
            options.queue = "sim-" + Instant.now().toEpochMilli();
        }
        Path trace = options.traceFile;
        WorkerChurnTraceLog.append(
                trace,
                "worker-churn-start",
                Map.of(
                        "backend", options.backend,
                        "runId", runId,
                        "queue", options.queue,
                        "workers", options.workers,
                        "durationMillis", options.duration.toMillis(),
                        "jobsPerSecond", options.jobsPerSecond));

        List<WorkerProcess> workers = new ArrayList<>();
        try (WorkerChurnStores.StoreHandle handle = WorkerChurnStores.open(options.backend)) {
            var scheduler = new Scheduler(handle.store(), new JsonJobSerializer());
            for (int i = 0; i < options.workers; i++) {
                workers.add(startWorker(options, i, runId));
            }

            long endAt = System.nanoTime() + options.duration.toNanos();
            long nextSubmitAt = System.nanoTime();
            long submitEveryNanos = Math.max(1L, 1_000_000_000L / options.jobsPerSecond);
            long nextKillAt =
                    options.killEvery.isZero() ? Long.MAX_VALUE : System.nanoTime() + options.killEvery.toNanos();
            long nextReportAt = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            int sequence = 0;
            List<JobId> submittedIds = new ArrayList<>();

            while (System.nanoTime() < endAt) {
                long now = System.nanoTime();
                if (now >= nextSubmitAt) {
                    sequence++;
                    long workMillis =
                            ThreadLocalRandom.current().nextLong(options.minWorkMillis, options.maxWorkMillis + 1);
                    JobId id = scheduler.enqueue(
                            new WorkerChurnPayload(runId, sequence, workMillis, trace.toString()),
                            WorkerChurnHandler.class,
                            options.queue,
                            0);
                    submittedIds.add(id);
                    WorkerChurnTraceLog.append(
                            trace,
                            "job-enqueued",
                            Map.of(
                                    "runId", runId,
                                    "queue", options.queue,
                                    "sequence", sequence,
                                    "jobId", id,
                                    "workMillis", workMillis));
                    nextSubmitAt += submitEveryNanos;
                }
                if (now >= nextKillAt) {
                    killAndRestartOne(options, workers, runId);
                    nextKillAt = System.nanoTime() + options.killEvery.toNanos();
                }
                restartExitedWorkers(options, workers, runId);
                if (now >= nextReportAt) {
                    appendStoreSnapshot(trace, runId, handle.store(), "store-snapshot", options.queue);
                    nextReportAt = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                }
                Thread.sleep(25);
            }

            boolean drained = waitForDrain(options, workers, runId, handle.store(), submittedIds);
            appendStoreSnapshot(trace, runId, handle.store(), "worker-churn-summary", options.queue);
            WorkerChurnTraceLog.append(
                    trace, "worker-churn-stop", Map.of("runId", runId, "submitted", sequence, "drained", drained));
            System.out.println("trace written to " + trace.toAbsolutePath());
            if (!drained) {
                throw new IllegalStateException("worker-churn simulation did not drain within " + options.drainTimeout);
            }
        } finally {
            for (WorkerProcess worker : workers) {
                worker.stop(trace, runId, "supervisor-stop");
            }
        }
    }

    private static void runWorker(Options options) throws Exception {
        String label = Objects.requireNonNull(options.label, "label");
        Path trace = options.traceFile;
        try (WorkerChurnStores.StoreHandle handle = WorkerChurnStores.open(options.backend)) {
            ProcessingNodeConfig config = ProcessingNodeConfig.builder()
                    .workerCount(4)
                    .pollInterval(Duration.ofMillis(100))
                    .claimHeartbeat(Duration.ofSeconds(1))
                    .heartbeatTimeout(Duration.ofSeconds(5))
                    .maintenanceLeaseDuration(Duration.ofSeconds(6))
                    .jobTimeout(Duration.ofSeconds(30))
                    .shutdownGracePeriod(Duration.ofSeconds(2))
                    .defaultMaxAttempts(20)
                    .retryInitialBackoff(Duration.ofSeconds(1))
                    .storeOutagePollInterval(Duration.ofSeconds(1))
                    .claimBatchSize(8)
                    .build();
            ProcessingNode node = ProcessingNode.builder(handle.store())
                    .config(config)
                    .lane(new QueueLane(options.queue, 4))
                    .build();
            Runtime.getRuntime()
                    .addShutdownHook(new Thread(
                            () -> {
                                WorkerChurnTraceLog.append(
                                        trace,
                                        "worker-close",
                                        Map.of(
                                                "label",
                                                label,
                                                "nodeId",
                                                node.nodeId(),
                                                "pid",
                                                ProcessHandle.current().pid()));
                                node.close();
                            },
                            "threadmill-worker-churn-shutdown"));
            WorkerChurnTraceLog.append(
                    trace,
                    "worker-start",
                    Map.of(
                            "label",
                            label,
                            "nodeId",
                            node.nodeId(),
                            "pid",
                            ProcessHandle.current().pid()));
            node.start();
            Thread.currentThread().join();
        }
    }

    private static WorkerProcess startWorker(Options options, int index, String runId) throws Exception {
        String label = "worker-" + index;
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(WorkerChurnSimulatorMain.class.getName());
        command.add("worker");
        command.add("--backend");
        command.add(options.backend.name().toLowerCase());
        command.add("--label");
        command.add(label);
        command.add("--trace");
        command.add(options.traceFile.toString());
        command.add("--queue");
        command.add(options.queue);
        Path traceParent = options.traceFile.toAbsolutePath().getParent();
        if (traceParent == null) traceParent = Path.of(".").toAbsolutePath();
        File childLog = traceParent.resolve(label + ".out.log").toFile();
        Process process = new ProcessBuilder(command)
                .redirectOutput(childLog)
                .redirectErrorStream(true)
                .start();
        WorkerChurnTraceLog.append(
                options.traceFile,
                "worker-process-start",
                Map.of("runId", runId, "label", label, "pid", process.pid(), "output", childLog.getAbsolutePath()));
        return new WorkerProcess(index, label, process);
    }

    private static void restartExitedWorkers(Options options, List<WorkerProcess> workers, String runId)
            throws Exception {
        for (int i = 0; i < workers.size(); i++) {
            WorkerProcess worker = workers.get(i);
            if (!worker.process.isAlive()) {
                WorkerChurnTraceLog.append(
                        options.traceFile,
                        "worker-process-exited",
                        Map.of("runId", runId, "label", worker.label, "exitCode", worker.process.exitValue()));
                workers.set(i, startWorker(options, worker.index, runId));
            }
        }
    }

    private static void killAndRestartOne(Options options, List<WorkerProcess> workers, String runId) throws Exception {
        int index = ThreadLocalRandom.current().nextInt(workers.size());
        WorkerProcess worker = workers.get(index);
        worker.stop(options.traceFile, runId, "forced-restart");
        workers.set(index, startWorker(options, worker.index, runId));
    }

    private static void appendStoreSnapshot(Path trace, String runId, JobStore store, String event, String queue) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("runId", runId);
        fields.put("queue", queue);
        fields.put("counts", store.countsByState());
        fields.put("queueDepth", store.queueDepths().getOrDefault(queue, 0L));
        fields.put("queueDepths", store.queueDepths());
        fields.put(
                "oldestProcessingHeartbeat",
                store.oldestProcessingHeartbeat().map(Instant::toString).orElse(""));
        fields.put("nodes", store.listNodeHeartbeats().size());
        WorkerChurnTraceLog.append(trace, event, fields);
    }

    private static boolean waitForDrain(
            Options options, List<WorkerProcess> workers, String runId, JobStore store, List<JobId> submittedIds)
            throws Exception {
        WorkerChurnTraceLog.append(
                options.traceFile,
                "submission-stop",
                Map.of("runId", runId, "drainTimeoutMillis", options.drainTimeout.toMillis()));
        long deadline = System.nanoTime() + options.drainTimeout.toNanos();
        while (System.nanoTime() < deadline) {
            restartExitedWorkers(options, workers, runId);
            long unfinished = submittedIds.stream()
                    .map(store::findById)
                    .filter(WorkerChurnSimulatorMain::isUnfinished)
                    .count();
            if (unfinished == 0) {
                WorkerChurnTraceLog.append(options.traceFile, "drain-complete", Map.of("runId", runId));
                return true;
            }
            Thread.sleep(100);
        }
        appendStoreSnapshot(options.traceFile, runId, store, "drain-timeout", options.queue);
        return false;
    }

    private static boolean isUnfinished(Optional<Job> job) {
        return job.isPresent() && !TERMINAL_STATES.contains(job.get().currentState());
    }

    private static final Set<JobState> TERMINAL_STATES =
            Set.of(JobState.SUCCEEDED, JobState.FAILED, JobState.DELETED, JobState.QUARANTINED, JobState.PROCESSED);

    private record WorkerProcess(int index, String label, Process process) {
        void stop(Path trace, String runId, String reason) {
            if (!process.isAlive()) return;
            WorkerChurnTraceLog.append(
                    trace,
                    "worker-process-kill",
                    Map.of("runId", runId, "label", label, "pid", process.pid(), "reason", reason));
            process.destroyForcibly();
        }
    }

    private static final class Options {
        private WorkerChurnStores.Backend backend = WorkerChurnStores.Backend.defaultBackend();
        private boolean workerMode;
        private String label;
        private int workers = 3;
        private int jobsPerSecond = 5;
        private Duration duration = Duration.ofMinutes(1);
        private Duration killEvery = Duration.ofSeconds(15);
        private Duration drainTimeout = Duration.ofSeconds(30);
        private long minWorkMillis = 100;
        private long maxWorkMillis = 1200;
        private String queue;
        private Path traceFile;

        private static Options parse(String[] args) {
            var options = new Options();
            int i = 0;
            if (args.length > 0 && "worker".equals(args[0])) {
                options.workerMode = true;
                i = 1;
            }
            while (i < args.length) {
                String key = args[i++];
                String value = i < args.length ? args[i++] : "";
                switch (key) {
                    case "--backend" -> options.backend = WorkerChurnStores.Backend.parse(value);
                    case "--label" -> options.label = value;
                    case "--workers" -> options.workers = Integer.parseInt(value);
                    case "--jobs-per-second" -> options.jobsPerSecond = Integer.parseInt(value);
                    case "--duration" -> options.duration = parseDuration(value);
                    case "--kill-every" -> options.killEvery = parseDuration(value);
                    case "--drain-timeout" -> options.drainTimeout = parseDuration(value);
                    case "--min-work-millis" -> options.minWorkMillis = Long.parseLong(value);
                    case "--max-work-millis" -> options.maxWorkMillis = Long.parseLong(value);
                    case "--trace" -> options.traceFile = Path.of(value);
                    case "--queue" -> options.queue = value;
                    default -> throw new IllegalArgumentException("unknown worker-churn argument: " + key);
                }
            }
            if (options.workers <= 0) throw new IllegalArgumentException("--workers must be positive");
            if (options.jobsPerSecond <= 0) throw new IllegalArgumentException("--jobs-per-second must be positive");
            if (options.duration.isNegative() || options.duration.isZero()) {
                throw new IllegalArgumentException("--duration must be positive");
            }
            if (options.killEvery.isNegative()) throw new IllegalArgumentException("--kill-every must not be negative");
            if (options.drainTimeout.isNegative()) {
                throw new IllegalArgumentException("--drain-timeout must not be negative");
            }
            if (options.minWorkMillis <= 0 || options.maxWorkMillis < options.minWorkMillis) {
                throw new IllegalArgumentException("work millis range is invalid");
            }
            if (options.workerMode && (options.queue == null || options.queue.isBlank())) {
                throw new IllegalArgumentException("worker mode requires --queue");
            }
            if (options.traceFile == null) {
                options.traceFile = defaultTraceFile(options.backend);
            }
            return options;
        }

        private static Path defaultTraceFile(WorkerChurnStores.Backend backend) {
            var timestamp = Instant.now().toString().replace(':', '-');
            var backendName = backend.name().toLowerCase();
            return Path.of("build", "simulation", "worker-churn-" + timestamp + "-" + backendName + ".jsonl");
        }

        private static Duration parseDuration(String value) {
            if (value.equals("0")) return Duration.ZERO;
            if (value.endsWith("ms")) return Duration.ofMillis(Long.parseLong(value.substring(0, value.length() - 2)));
            if (value.endsWith("s")) return Duration.ofSeconds(Long.parseLong(value.substring(0, value.length() - 1)));
            if (value.endsWith("m")) return Duration.ofMinutes(Long.parseLong(value.substring(0, value.length() - 1)));
            if (value.endsWith("h")) return Duration.ofHours(Long.parseLong(value.substring(0, value.length() - 1)));
            return Duration.parse(value);
        }
    }
}
