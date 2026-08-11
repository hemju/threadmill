package com.hemju.threadmill.simulation.nudge;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.engine.QueueLane;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.Scheduler;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.simulation.nudge.NudgeSimulationStores.Backend;
import com.hemju.threadmill.simulation.nudge.NudgeSimulationStores.ConnectionInfo;

/**
 * Process-separated nudge correctness simulation for Postgres and Redis.
 *
 * <p>A leader JVM registers a recurring outbox pump, a standby JVM competes
 * for maintenance leadership, and producer JVMs write durable work. The
 * supervisor hard-kills the leader after one accepted nudge and hard-kills a
 * second producer after its work write but before its nudge. The trace
 * verifier requires the standby to serve the accepted nudge and the regular
 * schedule to drain the producer's crash-window row.
 */
public final class NudgeSimulationMain {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Duration BACKSTOP_INTERVAL = Duration.ofSeconds(8);
    private static final Duration LEADER_MAINTENANCE_POLL = Duration.ofSeconds(10);
    private static final Duration STANDBY_MAINTENANCE_POLL = Duration.ofMillis(100);
    private static final Duration PROCESS_START_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration FAILOVER_TIMEOUT = Duration.ofSeconds(8);
    private static final Duration BACKSTOP_TIMEOUT = Duration.ofSeconds(16);

    private NudgeSimulationMain() {}

    static void main(String[] args) throws Exception {
        var options = Options.parse(args);
        switch (options.role) {
            case SUPERVISOR -> runSupervisor(options);
            case NODE -> runNode(options);
            case PRODUCER -> runProducer(options);
        }
    }

    private static void runSupervisor(Options options) throws Exception {
        var backendName = options.backend.name().toLowerCase(Locale.ROOT);
        var runId = UUID.randomUUID().toString();
        var timestamp = Instant.now().toString().replace(':', '-');
        var outputDirectory = Path.of("build", "simulation", "nudge-cross-node-" + timestamp + "-" + backendName)
                .toAbsolutePath()
                .normalize();
        Files.createDirectories(outputDirectory);
        var trace = outputDirectory.resolve("trace.jsonl");
        var taskName = "nudge-simulation-" + runId;
        var queue = "nudge-simulation-" + runId;
        NudgeSimulationTrace.append(
                trace,
                "simulation-start",
                Map.of(
                        "backend", backendName,
                        "runId", runId,
                        "taskName", taskName,
                        "queue", queue,
                        "backstopMillis", BACKSTOP_INTERVAL.toMillis()));

        ManagedProcess leader = null;
        ManagedProcess standby = null;
        ManagedProcess crashProducer = null;
        try (var fixture = NudgeSimulationStores.startBackend(options.backend);
                var storeHandle = NudgeSimulationStores.openJobStore(fixture.connectionInfo());
                var workStore = NudgeSimulationStores.openWorkStore(fixture.connectionInfo(), runId)) {
            workStore.prepare();

            var leaderReadyFile = outputDirectory.resolve("leader.ready.json");
            leader = startNode(
                    fixture.connectionInfo(),
                    outputDirectory,
                    trace,
                    runId,
                    taskName,
                    queue,
                    "leader",
                    true,
                    LEADER_MAINTENANCE_POLL,
                    leaderReadyFile);
            var leaderReady = awaitReady(leader, leaderReadyFile, PROCESS_START_TIMEOUT);
            await(
                    "leader to acquire the maintenance lease",
                    FAILOVER_TIMEOUT,
                    () -> storeHandle
                            .store()
                            .readMaintenanceLeaseOwner()
                            .filter(leaderReady.nodeId()::equals)
                            .isPresent());
            // Node.start() creates the maintenance thread asynchronously. Let
            // its immediate no-nudge tick finish so the leader is sleeping on
            // the deliberately long poll when the producer commits.
            Thread.sleep(500);

            var standbyReadyFile = outputDirectory.resolve("standby.ready.json");
            standby = startNode(
                    fixture.connectionInfo(),
                    outputDirectory,
                    trace,
                    runId,
                    taskName,
                    queue,
                    "standby",
                    false,
                    STANDBY_MAINTENANCE_POLL,
                    standbyReadyFile);
            var standbyReady = awaitReady(standby, standbyReadyFile, PROCESS_START_TIMEOUT);
            require(
                    storeHandle
                            .store()
                            .readMaintenanceLeaseOwner()
                            .filter(leaderReady.nodeId()::equals)
                            .isPresent(),
                    "standby unexpectedly displaced the live leader");

            var acceptedProducer = startProducer(
                    fixture.connectionInfo(),
                    outputDirectory,
                    trace,
                    runId,
                    taskName,
                    queue,
                    1,
                    ProducerMode.NUDGE,
                    outputDirectory.resolve("producer-accepted.ready"));
            awaitSuccess(acceptedProducer, PROCESS_START_TIMEOUT);
            var observedNudge = storeHandle.store().findCronTaskState(taskName).orElseThrow();
            require(observedNudge.nudgeRequestedAt() != null, "accepted nudge was consumed before the leader kill");
            require(observedNudge.nudgeRevision() != null, "accepted nudge has no revision");

            var killedLeaderPid = leader.pid();
            leader.destroyForcibly();
            NudgeSimulationTrace.append(
                    trace,
                    "leader-hard-killed",
                    Map.of(
                            "runId",
                            runId,
                            "pid",
                            killedLeaderPid,
                            "nodeId",
                            leaderReady.nodeId().toString(),
                            "observedNudgeRevision",
                            observedNudge.nudgeRevision()));
            awaitExit(leader, PROCESS_START_TIMEOUT);
            leader = null;

            await(
                    "standby to acquire the expired maintenance lease",
                    FAILOVER_TIMEOUT,
                    () -> storeHandle
                            .store()
                            .readMaintenanceLeaseOwner()
                            .filter(standbyReady.nodeId()::equals)
                            .isPresent());
            NudgeSimulationTrace.append(
                    trace,
                    "maintenance-elected",
                    Map.of(
                            "runId", runId,
                            "pid", standbyReady.pid(),
                            "nodeId", standbyReady.nodeId().toString()));
            await("accepted nudge work to drain", FAILOVER_TIMEOUT, () -> !workStore.isPending(1));
            awaitRecurringInstanceSuccess(storeHandle.store(), taskName, FAILOVER_TIMEOUT);

            var crashReadyFile = outputDirectory.resolve("producer-crash.ready.json");
            crashProducer = startProducer(
                    fixture.connectionInfo(),
                    outputDirectory,
                    trace,
                    runId,
                    taskName,
                    queue,
                    2,
                    ProducerMode.WAIT_BEFORE_NUDGE,
                    crashReadyFile);
            var crashReady = awaitReady(crashProducer, crashReadyFile, PROCESS_START_TIMEOUT);
            require(workStore.isPending(2), "crash-window producer did not persist its work row");
            crashProducer.destroyForcibly();
            NudgeSimulationTrace.append(
                    trace,
                    "producer-hard-killed-before-nudge",
                    Map.of("runId", runId, "sequence", 2, "pid", crashReady.pid()));
            awaitExit(crashProducer, PROCESS_START_TIMEOUT);
            crashProducer = null;

            await(
                    "crash-window work to drain on the backstop schedule",
                    BACKSTOP_TIMEOUT,
                    () -> !workStore.isPending(2));
            awaitRecurringInstanceSuccess(storeHandle.store(), taskName, FAILOVER_TIMEOUT);

            stopGracefully(standby, PROCESS_START_TIMEOUT);
            standby = null;
            verifyTrace(trace, standbyReady.pid());
            NudgeSimulationTrace.append(
                    trace,
                    "verification-passed",
                    Map.of("backend", backendName, "runId", runId, "trace", trace.toString()));
            System.out.println("process-separated nudge simulation passed; trace: " + trace);
        } finally {
            destroyIfAlive(crashProducer);
            destroyIfAlive(standby);
            destroyIfAlive(leader);
        }
    }

    private static void runNode(Options options) throws Exception {
        var connectionInfo = options.connectionInfo();
        NudgeSimulationStores.configureProcess(connectionInfo);
        try (var storeHandle = NudgeSimulationStores.openJobStore(connectionInfo)) {
            if (options.registerTask) {
                var scheduler = new Scheduler(storeHandle.store(), new JsonJobSerializer());
                scheduler.defineRecurring(
                        options.taskName,
                        new CronTask.Trigger.Interval(BACKSTOP_INTERVAL),
                        new NudgeSimulationPayload(options.runId, options.traceFile.toString()),
                        NudgeSimulationHandler.class.getName(),
                        options.queue,
                        0,
                        null,
                        null,
                        true,
                        CronTask.MissedRunPolicy.DROP);
            }

            var config = ProcessingNodeConfig.builder()
                    .workerCount(1)
                    .pollInterval(Duration.ofMillis(50))
                    .claimHeartbeat(Duration.ofMillis(250))
                    .heartbeatTimeout(Duration.ofSeconds(2))
                    .maintenanceLeaseDuration(Duration.ofMillis(1500))
                    .maintenancePollInterval(options.maintenancePoll)
                    .jobTimeout(Duration.ofSeconds(10))
                    .shutdownGracePeriod(Duration.ofSeconds(2))
                    .claimBatchSize(1)
                    .build();
            var node = ProcessingNode.builder(storeHandle.store())
                    .config(config)
                    .lane(new QueueLane(options.queue, 1))
                    .build();
            Runtime.getRuntime()
                    .addShutdownHook(new Thread(
                            () -> {
                                NudgeSimulationTrace.append(
                                        options.traceFile,
                                        "node-stop",
                                        Map.of(
                                                "runId",
                                                options.runId,
                                                "label",
                                                options.label,
                                                "pid",
                                                ProcessHandle.current().pid(),
                                                "nodeId",
                                                node.nodeId().toString()));
                                node.close();
                            },
                            "threadmill-nudge-simulation-shutdown"));
            node.start();
            var pid = ProcessHandle.current().pid();
            NudgeSimulationTrace.append(
                    options.traceFile,
                    "node-start",
                    Map.of(
                            "runId",
                            options.runId,
                            "label",
                            options.label,
                            "pid",
                            pid,
                            "nodeId",
                            node.nodeId().toString(),
                            "registeredTask",
                            options.registerTask,
                            "maintenancePollMillis",
                            options.maintenancePoll.toMillis()));
            writeReady(options.readyFile, node.nodeId(), pid);
            Thread.currentThread().join();
        }
    }

    private static void runProducer(Options options) throws Exception {
        var connectionInfo = options.connectionInfo();
        try (var storeHandle = NudgeSimulationStores.openJobStore(connectionInfo);
                var workStore = NudgeSimulationStores.openWorkStore(connectionInfo, options.runId)) {
            var pid = ProcessHandle.current().pid();
            workStore.record(options.sequence);
            NudgeSimulationTrace.append(
                    options.traceFile,
                    "work-recorded",
                    Map.of("runId", options.runId, "sequence", options.sequence, "pid", pid));
            writeReady(options.readyFile, null, pid);
            if (options.producerMode == ProducerMode.WAIT_BEFORE_NUDGE) {
                Thread.currentThread().join();
                return;
            }

            var scheduler = new Scheduler(storeHandle.store(), new JsonJobSerializer());
            scheduler.nudgeRecurring(options.taskName);
            var revision = storeHandle
                    .store()
                    .findCronTaskState(options.taskName)
                    .map(state -> state.nudgeRevision())
                    .orElseThrow();
            NudgeSimulationTrace.append(
                    options.traceFile,
                    "nudge-accepted",
                    Map.of("runId", options.runId, "sequence", options.sequence, "pid", pid, "revision", revision));
        }
    }

    private static ManagedProcess startNode(
            ConnectionInfo connectionInfo,
            Path outputDirectory,
            Path trace,
            String runId,
            String taskName,
            String queue,
            String label,
            boolean registerTask,
            Duration maintenancePoll,
            Path readyFile)
            throws IOException {
        var arguments = new ArrayList<String>();
        arguments.add("--role");
        arguments.add("node");
        arguments.add("--run-id");
        arguments.add(runId);
        arguments.add("--trace");
        arguments.add(trace.toString());
        arguments.add("--task");
        arguments.add(taskName);
        arguments.add("--queue");
        arguments.add(queue);
        arguments.add("--label");
        arguments.add(label);
        arguments.add("--register-task");
        arguments.add(Boolean.toString(registerTask));
        arguments.add("--maintenance-poll");
        arguments.add(maintenancePoll.toMillis() + "ms");
        arguments.add("--ready");
        arguments.add(readyFile.toString());
        return startProcess(connectionInfo, outputDirectory, label, arguments);
    }

    private static ManagedProcess startProducer(
            ConnectionInfo connectionInfo,
            Path outputDirectory,
            Path trace,
            String runId,
            String taskName,
            String queue,
            int sequence,
            ProducerMode mode,
            Path readyFile)
            throws IOException {
        var label = "producer-" + sequence;
        var arguments = new ArrayList<String>();
        arguments.add("--role");
        arguments.add("producer");
        arguments.add("--run-id");
        arguments.add(runId);
        arguments.add("--trace");
        arguments.add(trace.toString());
        arguments.add("--task");
        arguments.add(taskName);
        arguments.add("--queue");
        arguments.add(queue);
        arguments.add("--sequence");
        arguments.add(Integer.toString(sequence));
        arguments.add("--producer-mode");
        arguments.add(mode.name().toLowerCase(Locale.ROOT));
        arguments.add("--ready");
        arguments.add(readyFile.toString());
        return startProcess(connectionInfo, outputDirectory, label, arguments);
    }

    private static ManagedProcess startProcess(
            ConnectionInfo connectionInfo, Path outputDirectory, String label, List<String> arguments)
            throws IOException {
        var command = new ArrayList<String>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(NudgeSimulationMain.class.getName());
        connectionInfo.appendArguments(command);
        command.addAll(arguments);
        File log = outputDirectory.resolve(label + ".out.log").toFile();
        var process = new ProcessBuilder(command)
                .redirectOutput(log)
                .redirectErrorStream(true)
                .start();
        return new ManagedProcess(label, process, log.toPath());
    }

    private static Ready awaitReady(ManagedProcess process, Path readyFile, Duration timeout) throws Exception {
        await(
                process.label() + " to become ready",
                timeout,
                () -> Files.isRegularFile(readyFile) || !process.isAlive());
        if (!Files.isRegularFile(readyFile)) {
            throw new IllegalStateException(process.label() + " exited before ready; output: " + process.logFile());
        }
        var document = JSON.readTree(Files.readString(readyFile, StandardCharsets.UTF_8));
        var nodeId = document.hasNonNull("nodeId")
                ? NodeId.of(UUID.fromString(document.get("nodeId").asText()))
                : null;
        return new Ready(nodeId, document.get("pid").asLong());
    }

    private static void writeReady(Path readyFile, NodeId nodeId, long pid) {
        try {
            var fields = new LinkedHashMap<String, Object>();
            fields.put("pid", pid);
            fields.put("nodeId", nodeId == null ? null : nodeId.toString());
            Files.writeString(readyFile, JSON.writeValueAsString(fields), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("failed to write process-ready marker: " + readyFile, e);
        }
    }

    private static void awaitRecurringInstanceSuccess(JobStore store, String taskName, Duration timeout)
            throws Exception {
        await(
                "recurring instance to finish",
                timeout,
                () -> store.findCronTaskState(taskName)
                        .map(state -> state.inFlightJobId())
                        .map(JobId::of)
                        .flatMap(store::findById)
                        .map(job -> job.currentState() == JobState.SUCCEEDED)
                        .orElse(false));
    }

    private static void await(String description, Duration timeout, BooleanSupplier condition) throws Exception {
        var deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(25);
        }
        throw new IllegalStateException("timed out waiting for " + description + " after " + timeout);
    }

    private static void awaitSuccess(ManagedProcess process, Duration timeout) throws Exception {
        awaitExit(process, timeout);
        if (process.exitValue() != 0) {
            throw new IllegalStateException(
                    process.label() + " exited " + process.exitValue() + "; output: " + process.logFile());
        }
    }

    private static void awaitExit(ManagedProcess process, Duration timeout) throws Exception {
        if (!process.waitFor(timeout)) {
            throw new IllegalStateException(process.label() + " did not exit within " + timeout);
        }
    }

    private static void stopGracefully(ManagedProcess process, Duration timeout) throws Exception {
        if (process == null || !process.isAlive()) return;
        process.destroy();
        if (!process.waitFor(timeout)) {
            process.destroyForcibly();
            awaitExit(process, timeout);
        }
    }

    private static void destroyIfAlive(ManagedProcess process) {
        if (process == null || !process.isAlive()) return;
        process.destroyForcibly();
        try {
            process.waitFor(Duration.ofSeconds(5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void verifyTrace(Path trace, long standbyPid) throws IOException {
        var events = new ArrayList<JsonNode>();
        for (var line : Files.readAllLines(trace, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) events.add(JSON.readTree(line));
        }

        var acceptIndexes = indexesOf(events, "nudge-accepted");
        require(!acceptIndexes.isEmpty(), "trace has no accepted nudge");
        for (var acceptIndex : acceptIndexes) {
            var producerPid = events.get(acceptIndex).get("pid").asLong();
            require(
                    findAfter(
                                    events,
                                    acceptIndex,
                                    "pump-run-start",
                                    event -> event.get("pid").asLong() != producerPid)
                            >= 0,
                    "accepted nudge was not followed by a run in another OS process");
        }

        var accepted = requireEvent(events, "nudge-accepted", 1);
        var leaderKilled = requireEvent(events, "leader-hard-killed", null);
        var elected = requireEvent(events, "maintenance-elected", null);
        var firstDrain = requireEvent(events, "work-drained", 1);
        var firstRun = requireRun(events, firstDrain.event().get("jobId").asText());
        require(
                accepted.index() < leaderKilled.index()
                        && leaderKilled.index() < firstRun.index()
                        && firstRun.index() < firstDrain.index(),
                "accepted-nudge leader-kill ordering is not proven by the trace");
        require(leaderKilled.index() < elected.index(), "standby election was observed before the leader kill");
        require(elected.event().get("pid").asLong() == standbyPid, "maintenance ownership did not move to standby");
        require(firstRun.event().get("pid").asLong() == standbyPid, "accepted nudge was not served by the standby");
        require(
                "nudge".equals(firstRun.event().get("origin").asText()),
                "accepted nudge was served only by the backstop");

        var secondRecorded = requireEvent(events, "work-recorded", 2);
        var producerKilled = requireEvent(events, "producer-hard-killed-before-nudge", 2);
        var secondDrain = requireEvent(events, "work-drained", 2);
        var secondRun = requireRun(events, secondDrain.event().get("jobId").asText());
        require(
                secondRecorded.index() < producerKilled.index()
                        && producerKilled.index() < secondRun.index()
                        && secondRun.index() < secondDrain.index(),
                "producer crash-window ordering is not proven by the trace");
        require(
                events.stream()
                        .noneMatch(event ->
                                "nudge-accepted".equals(event.path("event").asText())
                                        && event.path("sequence").asInt(-1) == 2),
                "the hard-killed producer accepted a nudge unexpectedly");
        require(
                "schedule".equals(secondRun.event().get("origin").asText()),
                "producer crash-window row was not drained by the backstop schedule");
    }

    private static List<Integer> indexesOf(List<JsonNode> events, String eventName) {
        var indexes = new ArrayList<Integer>();
        for (int i = 0; i < events.size(); i++) {
            if (eventName.equals(events.get(i).path("event").asText())) indexes.add(i);
        }
        return indexes;
    }

    private static int findAfter(
            List<JsonNode> events, int afterIndex, String eventName, Predicate<JsonNode> predicate) {
        for (int i = afterIndex + 1; i < events.size(); i++) {
            var event = events.get(i);
            if (eventName.equals(event.path("event").asText()) && predicate.test(event)) return i;
        }
        return -1;
    }

    private static IndexedEvent requireEvent(List<JsonNode> events, String eventName, Integer sequence) {
        for (int i = 0; i < events.size(); i++) {
            var event = events.get(i);
            if (!eventName.equals(event.path("event").asText())) continue;
            if (sequence == null || event.path("sequence").asInt(-1) == sequence) {
                return new IndexedEvent(i, event);
            }
        }
        throw new IllegalStateException("trace has no " + eventName + (sequence == null ? "" : " for " + sequence));
    }

    private static IndexedEvent requireRun(List<JsonNode> events, String jobId) {
        for (int i = 0; i < events.size(); i++) {
            var event = events.get(i);
            if ("pump-run-start".equals(event.path("event").asText())
                    && jobId.equals(event.path("jobId").asText())) {
                return new IndexedEvent(i, event);
            }
        }
        throw new IllegalStateException("trace has no pump run for job " + jobId);
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }

    private enum Role {
        SUPERVISOR,
        NODE,
        PRODUCER
    }

    private enum ProducerMode {
        NUDGE,
        WAIT_BEFORE_NUDGE
    }

    private record Ready(NodeId nodeId, long pid) {}

    private record IndexedEvent(int index, JsonNode event) {}

    private record ManagedProcess(String label, Process process, Path logFile) {
        long pid() {
            return process.pid();
        }

        boolean isAlive() {
            return process.isAlive();
        }

        void destroy() {
            process.destroy();
        }

        void destroyForcibly() {
            process.destroyForcibly();
        }

        boolean waitFor(Duration timeout) throws InterruptedException {
            return process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        int exitValue() {
            return process.exitValue();
        }
    }

    private static final class Options {
        private Role role = Role.SUPERVISOR;
        private Backend backend = Backend.POSTGRES;
        private String jdbcUrl;
        private String databaseUser;
        private String databasePassword;
        private String redisUri;
        private String runId;
        private Path traceFile;
        private String taskName;
        private String queue;
        private String label;
        private boolean registerTask;
        private Duration maintenancePoll;
        private Path readyFile;
        private int sequence;
        private ProducerMode producerMode;

        private static Options parse(String[] args) {
            var options = new Options();
            for (int i = 0; i < args.length; i += 2) {
                var key = args[i];
                var value = i + 1 < args.length ? args[i + 1] : "";
                switch (key) {
                    case "--role" -> options.role = Role.valueOf(value.toUpperCase(Locale.ROOT));
                    case "--backend" -> options.backend = Backend.parse(value);
                    case "--jdbc-url" -> options.jdbcUrl = value;
                    case "--db-user" -> options.databaseUser = value;
                    case "--db-password" -> options.databasePassword = value;
                    case "--redis-uri" -> options.redisUri = value;
                    case "--run-id" -> options.runId = value;
                    case "--trace" -> options.traceFile = Path.of(value);
                    case "--task" -> options.taskName = value;
                    case "--queue" -> options.queue = value;
                    case "--label" -> options.label = value;
                    case "--register-task" -> options.registerTask = Boolean.parseBoolean(value);
                    case "--maintenance-poll" -> options.maintenancePoll = parseDuration(value);
                    case "--ready" -> options.readyFile = Path.of(value);
                    case "--sequence" -> options.sequence = Integer.parseInt(value);
                    case "--producer-mode" ->
                        options.producerMode = ProducerMode.valueOf(value.toUpperCase(Locale.ROOT));
                    default -> throw new IllegalArgumentException("unknown nudge simulation argument: " + key);
                }
            }
            options.validate();
            return options;
        }

        private void validate() {
            if (role == Role.SUPERVISOR) return;
            Objects.requireNonNull(runId, "--run-id");
            Objects.requireNonNull(traceFile, "--trace");
            Objects.requireNonNull(taskName, "--task");
            Objects.requireNonNull(queue, "--queue");
            Objects.requireNonNull(readyFile, "--ready");
            connectionInfo();
            if (role == Role.NODE) {
                Objects.requireNonNull(label, "--label");
                Objects.requireNonNull(maintenancePoll, "--maintenance-poll");
            } else {
                if (sequence <= 0) throw new IllegalArgumentException("--sequence must be positive");
                Objects.requireNonNull(producerMode, "--producer-mode");
            }
        }

        private ConnectionInfo connectionInfo() {
            return switch (backend) {
                case POSTGRES ->
                    new ConnectionInfo(
                            backend,
                            Objects.requireNonNull(jdbcUrl, "--jdbc-url"),
                            Objects.requireNonNull(databaseUser, "--db-user"),
                            Objects.requireNonNull(databasePassword, "--db-password"),
                            null);
                case REDIS ->
                    new ConnectionInfo(backend, null, null, null, Objects.requireNonNull(redisUri, "--redis-uri"));
            };
        }

        private static Duration parseDuration(String value) {
            if (value.endsWith("ms")) return Duration.ofMillis(Long.parseLong(value.substring(0, value.length() - 2)));
            if (value.endsWith("s")) return Duration.ofSeconds(Long.parseLong(value.substring(0, value.length() - 1)));
            return Duration.parse(value);
        }
    }
}
