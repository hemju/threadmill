package com.hemju.threadmill.store.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobRelationship;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobSpec;

/** Bounded real primary failure and slot-move qualification on minimum/newer Redis. */
@ResourceLock("redis-failover-fixed-ports")
class RedisFailoverTest {
  @ParameterizedTest
  @ValueSource(strings = {"7.4-alpine", "8.6-alpine"})
  void sentinelPromotesReplicaAndExistingWorkersDrainQueuedAndInFlightJobs(String image)
      throws Exception {
    verifySentinelRecovery(image, false);
  }

  @ParameterizedTest
  @ValueSource(strings = {"7.4-alpine", "8.6-alpine"})
  void sentinelPromotesReplicaAfterTiltProtectionAndExistingWorkersDrain(String image)
      throws Exception {
    verifySentinelRecovery(image, true);
  }

  private static void verifySentinelRecovery(String image, boolean induceTilt) throws Exception {
    long started = System.nanoTime();
    try (var topology = RedisFailoverTopology.start(image, false);
        var workload = new Workload(topology.config())) {
      workload.startBlocked();
      replicationBarrier(topology, topology.ports.getFirst());
      if (induceTilt) topology.enterSentinelTilt();
      topology.stopProcess(topology.ports.getFirst());
      await().atMost(RedisFailoverTopology.SENTINEL_RECOVERY_TIMEOUT).untilAsserted(() -> {
        assertThat(topology.cli(
                topology.ports.get(2),
                "SENTINEL",
                "GET-MASTER-ADDR-BY-NAME",
                RedisFailoverTopology.MASTER))
            .endsWith(Integer.toString(topology.ports.get(1)));
        assertThat(topology.cli(topology.ports.get(1), "ROLE")).startsWith("master");
      });
      workload.assertHeldAndDrain();
      report(
          induceTilt ? "sentinel-tilt" : "sentinel",
          image,
          started,
          topology,
          topology.ports.get(1),
          workload);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"7.4-alpine", "8.6-alpine"})
  void clusterPromotesReplicaAndRefreshesExistingWorkerConnections(String image) throws Exception {
    long started = System.nanoTime();
    try (var topology = RedisFailoverTopology.start(image, true);
        var workload = new Workload(topology.config())) {
      int slot = Integer.parseInt(
          topology.cli(topology.ports.getFirst(), "CLUSTER", "KEYSLOT", RedisKeys.COUNTS));
      var nodes = topology.nodes(topology.ports.getFirst());
      var primary = nodes.stream().filter(node -> node.owns(slot)).findFirst().orElseThrow();
      var replica = nodes.stream()
          .filter(node -> node.parent().equals(primary.id()))
          .findFirst()
          .orElseThrow();
      workload.startBlocked();
      replicationBarrier(topology, primary.port());
      topology.stopProcess(primary.port());
      await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
        assertThat(topology.cli(replica.port(), "ROLE")).startsWith("master");
        assertThat(topology.nodes(replica.port()).stream()
                .anyMatch(node -> node.port() == replica.port() && node.owns(slot)))
            .isTrue();
      });
      workload.assertHeldAndDrain();
      report("cluster-failover", image, started, topology, replica.port(), workload);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"7.4-alpine", "8.6-alpine"})
  void clusterSlotMigrationKeepsConcurrentJobTransitionsAndHoldsConsistent(String image)
      throws Exception {
    long started = System.nanoTime();
    try (var topology = RedisFailoverTopology.start(image, true);
        var workload = new Workload(topology.config())) {
      int slot = Integer.parseInt(
          topology.cli(topology.ports.getFirst(), "CLUSTER", "KEYSLOT", RedisKeys.COUNTS));
      var nodes = topology.nodes(topology.ports.getFirst());
      var source = nodes.stream().filter(node -> node.owns(slot)).findFirst().orElseThrow();
      var target = nodes.stream()
          .filter(node -> node.primary() && node.port() != source.port())
          .findFirst()
          .orElseThrow();
      workload.startBlocked();
      assertThat(topology.cli(
              target.port(),
              "CLUSTER",
              "SETSLOT",
              Integer.toString(slot),
              "IMPORTING",
              source.id()))
          .isEqualTo("OK");
      assertThat(topology.cli(
              source.port(),
              "CLUSTER",
              "SETSLOT",
              Integer.toString(slot),
              "MIGRATING",
              target.id()))
          .isEqualTo("OK");
      workload.release.countDown();
      long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      int moved = 0;
      while (true) {
        assertThat(System.nanoTime()).as("bounded slot migration").isLessThan(deadline);
        var keys =
            topology.cli(source.port(), "CLUSTER", "GETKEYSINSLOT", Integer.toString(slot), "100");
        if (keys.isBlank()) break;
        var command = new ArrayList<>(List.of(
            "MIGRATE",
            "127.0.0.1",
            Integer.toString(target.port()),
            "",
            "0",
            "5000",
            "REPLACE",
            "KEYS"));
        command.addAll(List.of(keys.split("\\R")));
        assertThat(topology.cli(source.port(), command.toArray(String[]::new))).isEqualTo("OK");
        moved += keys.split("\\R").length;
      }
      for (var node : nodes) {
        if (node.primary())
          assertThat(topology.cli(
                  node.port(), "CLUSTER", "SETSLOT", Integer.toString(slot), "NODE", target.id()))
              .isEqualTo("OK");
      }
      assertThat(moved).isPositive();
      workload.assertDrained();
      assertThat(topology.cli(source.port(), "CLUSTER", "COUNTKEYSINSLOT", Integer.toString(slot)))
          .isEqualTo("0");
      report("cluster-slot-move", image, started, topology, target.port(), workload);
    }
  }

  private static void replicationBarrier(RedisFailoverTopology topology, int primary)
      throws Exception {
    // WAIT applies to writes on the same connection. Keep the marker SET and WAIT
    // in one redis-cli session, after all seeded jobs and blocked claims exist.
    var result = topology.container.execInContainer(
        "sh",
        "-c",
        "printf 'SET {threadmill}:qualification_barrier ready\\nWAIT 1 5000\\n' | redis-cli --raw -p "
            + primary);
    assertThat(result.getExitCode()).isZero();
    assertThat(result.getStdout().trim()).endsWith("1");
  }

  private static void report(
      String scenario,
      String image,
      long started,
      RedisFailoverTopology topology,
      int primary,
      Workload workload)
      throws Exception {
    var directory = Path.of("build", "redis-topology");
    Files.createDirectories(directory);
    new ObjectMapper()
        .writerWithDefaultPrettyPrinter()
        .writeValue(
            directory.resolve(scenario + "-" + image + ".json").toFile(),
            Map.of(
                "scenario",
                scenario,
                "image",
                image,
                "server",
                topology.cli(primary, "INFO", "server"),
                "elapsedMillis",
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started),
                "jobs",
                workload.jobs.size(),
                "exclusiveOverlaps",
                workload.violations.size(),
                "counts",
                workload.store.countsByState()));
  }

  public record Work(int sequence, boolean exclusive) implements JobPayload {}

  private static final class Workload implements AutoCloseable {
    final RedisJobStore store;
    final List<Job> jobs = new ArrayList<>();
    final List<ProcessingNode> nodes = new ArrayList<>();
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch entered = new CountDownLatch(4);
    final CountDownLatch rootStarted = new CountDownLatch(1);
    final AtomicInteger exclusive = new AtomicInteger();
    final ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();
    final Map<Integer, AtomicInteger> executions = new ConcurrentHashMap<>();

    Workload(RedisStoreConfig config) {
      store = new RedisJobStore(config);
      var serializer = new JsonJobSerializer();
      for (int i = 0; i < 203; i++) {
        var builder = Job.builder()
            .queue(i < 3 ? "exclusive" : "default")
            .priority(i == 0 ? 100 : 0)
            .spec(JobSpec.of(
                Handler.class.getName(), serializer.serializePayload(new Work(i, i < 3))));
        if (i == 0 || i == 2)
          builder.concurrencyKey("failover-exclusive").concurrencyMode(ConcurrencyMode.EXCLUSIVE);
        if (i == 1)
          builder
              .initialState(JobState.AWAITING)
              .relationship(
                  new JobRelationship(jobs.getFirst().id(), JobRelationship.Kind.WORKFLOW_STEP));
        var job = builder.build();
        store.insert(job);
        jobs.add(job);
      }
      var handler = new Handler(this);
      var processing = ProcessingNodeConfig.builder()
          .workerCount(4)
          .pollInterval(Duration.ofMillis(20))
          .claimHeartbeat(Duration.ofMillis(200))
          .heartbeatTimeout(Duration.ofMinutes(3))
          .jobTimeout(Duration.ofMinutes(2))
          .shutdownGracePeriod(Duration.ofSeconds(3))
          .maintenancePollInterval(Duration.ofMillis(100))
          .storeOutagePollInterval(Duration.ofMillis(100))
          .maxConsecutiveDispatcherFailures(1)
          .build();
      for (int i = 0; i < 2; i++)
        nodes.add(ProcessingNode.builder(store)
            .config(processing)
            .lane("exclusive", 1)
            .lane("default", 3)
            .handlerResolver(name -> handler)
            .build());
    }

    void startBlocked() throws Exception {
      for (var node : nodes) node.start();
      assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(rootStarted.await(10, TimeUnit.SECONDS)).isTrue();
    }

    void assertHeldAndDrain() {
      await().atMost(Duration.ofSeconds(45)).ignoreExceptions().untilAsserted(() -> {
        assertThat(store.findById(jobs.getFirst().id()).orElseThrow().currentState())
            .isEqualTo(JobState.PROCESSING);
        assertThat(store.claimReady(NodeId.newId(), "exclusive", 1, Instant.now()))
            .isEmpty();
      });
      release.countDown();
      assertDrained();
    }

    void assertDrained() {
      await().atMost(Duration.ofSeconds(60)).ignoreExceptions().untilAsserted(() -> {
        assertThat(store.countsByState().getOrDefault(JobState.SUCCEEDED, 0L))
            .isEqualTo((long) jobs.size());
      });
      for (var job : jobs)
        assertThat(store.findById(job.id()).orElseThrow().currentState())
            .isEqualTo(JobState.SUCCEEDED);
      assertThat(violations).isEmpty();
      assertThat(exclusive).hasValue(0);
      assertThat(executions).hasSize(jobs.size());
      assertThat(store.countsByState().getOrDefault(JobState.PROCESSING, 0L)).isZero();
      assertThat(store.countsByState().getOrDefault(JobState.AWAITING, 0L)).isZero();
    }

    @Override
    public void close() {
      release.countDown();
      for (var node : nodes) node.close();
      store.close();
    }
  }

  public static final class Handler implements JobHandler<Work> {
    private final Workload workload;

    Handler(Workload workload) {
      this.workload = workload;
    }

    @Override
    public void run(Work work, JobExecutionContext context) throws Exception {
      if (work.exclusive() && workload.exclusive.incrementAndGet() != 1)
        workload.violations.add("overlap " + work.sequence());
      try {
        workload
            .executions
            .computeIfAbsent(work.sequence(), key -> new AtomicInteger())
            .incrementAndGet();
        workload.entered.countDown();
        if (work.sequence() == 0) workload.rootStarted.countDown();
        if (!workload.release.await(90, TimeUnit.SECONDS))
          throw new IllegalStateException("test release timed out");
        Thread.sleep(30);
      } finally {
        if (work.exclusive()) workload.exclusive.decrementAndGet();
      }
    }
  }
}
