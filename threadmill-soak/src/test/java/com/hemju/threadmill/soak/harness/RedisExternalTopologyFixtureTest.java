package com.hemju.threadmill.soak.harness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;

/** Verifies the harness's external URL wiring; full failover qualification lives in the Redis store suite. */
@Tag("soak")
class RedisExternalTopologyFixtureTest {
  @ParameterizedTest
  @ValueSource(strings = {"sentinel", "cluster"})
  void externalTopologyResetsOnlyThreadmillAndProcessesJobs(String topology) throws Exception {
    int dataPort = availablePort();
    int sentinelPort;
    do {
      sentinelPort = availablePort();
    } while (sentinelPort == dataPort);
    boolean cluster = topology.equals("cluster");
    try (var container = new TopologyContainer(dataPort, cluster ? dataPort : sentinelPort)) {
      var script = "redis-server --port " + dataPort
          + " --bind 0.0.0.0 --protected-mode no --daemonize yes --appendonly yes --maxmemory-policy noeviction"
          + (cluster
              ? " --cluster-enabled yes --cluster-config-file /data/cluster.conf --cluster-announce-ip 127.0.0.1"
              : "")
          + "\n";
      if (!cluster) {
        container.withCopyToContainer(
            Transferable.of("port " + sentinelPort + "\n"
                + "bind 0.0.0.0\nprotected-mode no\n"
                + "sentinel monitor soak-primary 127.0.0.1 " + dataPort + " 1\n"),
            "/tmp/sentinel.conf");
        script += "redis-server /tmp/sentinel.conf --sentinel --daemonize yes\n";
      }
      container
          .withCopyToContainer(
              Transferable.of("#!/bin/sh\nset -eu\n" + script + "exec tail -f /dev/null\n"),
              "/tmp/start.sh")
          .withCommand("sh", "/tmp/start.sh")
          .waitingFor(Wait.forListeningPorts(
              cluster ? new int[] {dataPort} : new int[] {dataPort, sentinelPort}));
      container.start();
      if (cluster) {
        assertThat(container
                .execInContainer(
                    "redis-cli",
                    "-p",
                    Integer.toString(dataPort),
                    "CLUSTER",
                    "ADDSLOTSRANGE",
                    "0",
                    "16383")
                .getStdout())
            .contains("OK");
        await()
            .atMost(Duration.ofSeconds(10))
            .untilAsserted(() -> assertThat(container
                    .execInContainer(
                        "redis-cli", "-p", Integer.toString(dataPort), "CLUSTER", "INFO")
                    .getStdout())
                .contains("cluster_state:ok"));
      }
      container.execInContainer(
          "redis-cli", "-p", Integer.toString(dataPort), "SET", "unrelated:key", "preserve");
      container.execInContainer(
          "redis-cli", "-p", Integer.toString(dataPort), "SET", "{threadmill}:old", "reset");
      var url = cluster
          ? "redis://127.0.0.1:" + dataPort
          : "redis-sentinel://127.0.0.1:" + sentinelPort + "/0#soak-primary";
      try (var fixture = new RedisHarnessFixture(topology, Optional.of(url))) {
        var store = fixture.store();
        var job = Job.builder()
            .spec(JobSpec.of("example.Handler", new JobArgument("java.lang.String", "\"data\"")))
            .build();
        store.insert(job);
        var claimed =
            store.claimReady(NodeId.newId(), "default", 1, Instant.now()).getFirst();
        claimed.transitionTo(JobState.SUCCEEDED, Instant.now());
        store.saveAtomic(claimed, claimed.version());
        assertThat(store.findById(job.id()).orElseThrow().currentState())
            .isEqualTo(JobState.SUCCEEDED);
      }
      assertThat(container
              .execInContainer(
                  "redis-cli", "--raw", "-p", Integer.toString(dataPort), "GET", "unrelated:key")
              .getStdout()
              .trim())
          .isEqualTo("preserve");
      assertThat(container
              .execInContainer(
                  "redis-cli",
                  "--raw",
                  "-p",
                  Integer.toString(dataPort),
                  "EXISTS",
                  "{threadmill}:old")
              .getStdout()
              .trim())
          .isEqualTo("0");
    }
  }

  private static int availablePort() throws Exception {
    while (true) {
      try (var socket = new ServerSocket(0)) {
        if (socket.getLocalPort() < 55000) return socket.getLocalPort();
      }
    }
  }

  private static final class TopologyContainer extends GenericContainer<TopologyContainer> {
    TopologyContainer(int dataPort, int sentinelPort) {
      super(DockerImageName.parse("redis:7.4-alpine"));
      addFixedExposedPort(dataPort, dataPort);
      if (sentinelPort != dataPort) addFixedExposedPort(sentinelPort, sentinelPort);
    }
  }
}
