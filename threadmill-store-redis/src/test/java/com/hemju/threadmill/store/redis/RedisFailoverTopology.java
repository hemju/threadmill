package com.hemju.threadmill.store.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

/** Real independent Redis processes with one-to-one advertised host ports. */
final class RedisFailoverTopology implements AutoCloseable {
  static final String MASTER = "threadmill-failover";
  // Sentinel deliberately suspends elections for 30 stable seconds after TILT.
  // Allow that guard plus election retries without disabling the protection.
  static final Duration SENTINEL_RECOVERY_TIMEOUT = Duration.ofSeconds(90);
  final ProcessContainer container;
  final List<Integer> ports;
  private final boolean cluster;

  private RedisFailoverTopology(ProcessContainer container, List<Integer> ports, boolean cluster) {
    this.container = container;
    this.ports = ports;
    this.cluster = cluster;
  }

  static RedisFailoverTopology start(String image, boolean cluster) throws Exception {
    RuntimeException lastFailure = null;
    for (int attempt = 0; attempt < 3; attempt++) {
      var ports = availablePorts(cluster ? 6 : 5);
      var container = new ProcessContainer(image, ports);
      var script = new StringBuilder("#!/bin/sh\nset -eu\n");
      for (int i = 0; i < ports.size(); i++) {
        int port = ports.get(i);
        var config = """
            bind 0.0.0.0
            protected-mode no
            port %d
            daemonize yes
            dir /data/node-%d
            pidfile /data/node-%d/redis.pid
            logfile /data/node-%d/redis.log
            """.formatted(port, i, i, i);
        if (cluster || i < 2) {
          config +=
              "appendonly yes\nappendfsync everysec\nmaxmemory-policy noeviction\nrepl-diskless-sync-delay 0\n";
        }
        if (cluster) {
          config += """
              cluster-enabled yes
              cluster-config-file nodes.conf
              cluster-node-timeout 1000
              cluster-announce-ip 127.0.0.1
              cluster-announce-port %d
              cluster-announce-bus-port %d
              """.formatted(port, port + 10000);
        } else if (i == 1) {
          config += "replicaof 127.0.0.1 " + ports.getFirst()
              + "\nreplica-announce-ip 127.0.0.1\nreplica-announce-port " + port + "\n";
        } else if (i >= 2) {
          // Replica eligibility allows 10 * down-after plus observed master downtime.
          // A one-second test setting wrongly excludes a replicated candidate when
          // TILT delays the initial down observation by thirty seconds.
          config += """
              sentinel monitor %s 127.0.0.1 %d 2
              sentinel down-after-milliseconds %s 5000
              sentinel failover-timeout %s 10000
              sentinel parallel-syncs %s 1
              sentinel announce-ip 127.0.0.1
              sentinel announce-port %d
              """.formatted(MASTER, ports.getFirst(), MASTER, MASTER, MASTER, port);
        }
        container.withCopyToContainer(Transferable.of(config), "/tmp/node-" + i + ".conf");
        script.append("mkdir -p /data/node-").append(i).append('\n');
        script
            .append("cp /tmp/node-")
            .append(i)
            .append(".conf /data/node-")
            .append(i)
            .append("/redis.conf\n");
        script.append("redis-server /data/node-").append(i).append("/redis.conf");
        if (!cluster && i >= 2) script.append(" --sentinel");
        script.append('\n');
      }
      script.append("exec tail -f /dev/null\n");
      container
          .withCopyToContainer(Transferable.of(script.toString()), "/tmp/start.sh")
          .withCommand("sh", "/tmp/start.sh")
          .waitingFor(
              Wait.forListeningPorts(ports.stream().mapToInt(Integer::intValue).toArray()))
          .withStartupTimeout(Duration.ofSeconds(45));
      try {
        container.start();
        var result = new RedisFailoverTopology(container, ports, cluster);
        if (cluster) {
          var command = new ArrayList<>(List.of("redis-cli", "--cluster", "create"));
          for (int port : ports) command.add("127.0.0.1:" + port);
          command.addAll(List.of("--cluster-replicas", "1", "--cluster-yes"));
          var created = container.execInContainer(command.toArray(String[]::new));
          assertThat(created.getExitCode())
              .as(created.getStdout() + created.getStderr())
              .isZero();
          await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var reference = result.nodes(ports.getFirst());
            assertThat(reference.stream().filter(ClusterNode::primary).count()).isEqualTo(3);
            for (int port : ports) {
              assertThat(result.cli(port, "CLUSTER", "INFO")).contains("cluster_state:ok");
              assertThat(result.nodes(port)).containsExactlyInAnyOrderElementsOf(reference);
              var info = result.cli(port, "INFO", "replication");
              if (info.contains("role:slave"))
                assertThat(info).contains("master_link_status:up", "master_sync_in_progress:0");
              else assertThat(info).contains("connected_slaves:1", "state=online");
            }
          });
        } else {
          await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            assertThat(result.cli(ports.get(1), "INFO", "replication"))
                .contains("master_link_status:up");
            for (int sentinel : ports.subList(2, 5)) {
              assertThat(result.cli(sentinel, "INFO", "sentinel")).contains("sentinel_tilt:0");
              assertThat(result.cli(sentinel, "SENTINEL", "CKQUORUM", MASTER)).startsWith("OK");
              assertThat(result.cli(sentinel, "SENTINEL", "REPLICAS", MASTER))
                  .contains(Integer.toString(ports.get(1)));
            }
          });
        }
        return result;
      } catch (RuntimeException failure) {
        lastFailure = failure;
        container.close();
      } catch (Throwable failure) {
        container.close();
        throw failure;
      }
    }
    throw lastFailure;
  }

  RedisStoreConfig config() {
    if (cluster)
      return new RedisStoreConfig.Cluster(
          ports.stream()
              .map(port -> new RedisStoreConfig.HostAndPort("127.0.0.1", port))
              .toList(),
          "master");
    return new RedisStoreConfig.Sentinel(
        MASTER,
        ports.subList(2, 5).stream()
            .map(port -> new RedisStoreConfig.HostAndPort("127.0.0.1", port))
            .toList(),
        RedisStoreConfig.Credentials.none(),
        RedisStoreConfig.Credentials.none(),
        RedisStoreConfig.Tls.disabled());
  }

  String cli(int port, String... args) throws Exception {
    var command = new ArrayList<>(List.of("redis-cli", "--raw", "-p", Integer.toString(port)));
    command.addAll(List.of(args));
    var result = container.execInContainer(command.toArray(String[]::new));
    assertThat(result.getExitCode()).as(result.getStdout() + result.getStderr()).isZero();
    return result.getStdout().trim();
  }

  void stopProcess(int port) throws Exception {
    int index = ports.indexOf(port);
    assertThat(index).isNotNegative();
    var killed =
        container.execInContainer("sh", "-c", "kill -9 $(cat /data/node-" + index + "/redis.pid)");
    assertThat(killed.getExitCode()).as(killed.getStderr()).isZero();
  }

  void enterSentinelTilt() throws Exception {
    assertThat(cluster).isFalse();
    signalSentinels("STOP");
    try {
      // More than Sentinel's two-second timer discontinuity threshold.
      Thread.sleep(Duration.ofMillis(2200));
    } finally {
      signalSentinels("CONT");
    }
    await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
      for (int sentinel : ports.subList(2, 5))
        assertThat(cli(sentinel, "INFO", "sentinel")).contains("sentinel_tilt:1");
    });
  }

  private void signalSentinels(String signal) throws Exception {
    var result = container.execInContainer(
        "sh",
        "-c",
        "kill -" + signal
            + " $(cat /data/node-2/redis.pid /data/node-3/redis.pid /data/node-4/redis.pid)");
    assertThat(result.getExitCode()).as(result.getStderr()).isZero();
  }

  List<ClusterNode> nodes(int port) throws Exception {
    var result = new ArrayList<ClusterNode>();
    for (var line : cli(port, "CLUSTER", "NODES").split("\\R")) {
      var fields = line.split(" ");
      var endpoint = fields[1].split("@")[0];
      result.add(new ClusterNode(
          fields[0],
          Integer.parseInt(endpoint.substring(endpoint.lastIndexOf(':') + 1)),
          fields[2].contains("master"),
          fields[3],
          List.of(fields).subList(8, fields.length)));
    }
    return result;
  }

  record ClusterNode(String id, int port, boolean primary, String parent, List<String> slots) {
    boolean owns(int slot) {
      if (!primary) return false;
      for (var range : slots) {
        if (range.startsWith("[")) continue;
        var bounds = range.split("-");
        int first = Integer.parseInt(bounds[0]);
        int last = bounds.length == 1 ? first : Integer.parseInt(bounds[1]);
        if (slot >= first && slot <= last) return true;
      }
      return false;
    }
  }

  @Override
  public void close() {
    try {
      var directory =
          Path.of("build", "redis-topology", "process-logs", container.getContainerId());
      Files.createDirectories(directory);
      for (int i = 0; i < ports.size(); i++) {
        var logs = container.execInContainer("cat", "/data/node-" + i + "/redis.log");
        Files.writeString(directory.resolve(ports.get(i) + ".log"), logs.getStdout());
      }
    } catch (Exception failure) {
      throw new IllegalStateException("Could not preserve Redis process diagnostics", failure);
    } finally {
      container.close();
    }
  }

  private static List<Integer> availablePorts(int count) throws IOException {
    var ports = new ArrayList<Integer>();
    Set<Integer> reserved = new HashSet<>();
    while (ports.size() < count) {
      try (var socket = new ServerSocket(0)) {
        int port = socket.getLocalPort();
        if (port >= 55000 || reserved.contains(port) || reserved.contains(port + 10000)) continue;
        ports.add(port);
        reserved.add(port);
        reserved.add(port + 10000);
      }
    }
    return ports;
  }

  static final class ProcessContainer extends GenericContainer<ProcessContainer> {
    ProcessContainer(String image, List<Integer> ports) {
      super(DockerImageName.parse("redis:" + image));
      for (int port : ports) addFixedExposedPort(port, port);
    }
  }
}
