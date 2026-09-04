package com.hemju.threadmill.store.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.cluster.RedisClusterClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;

@ResourceLock("redis-secure-fixed-ports")
class RedisSecureTopologyTest {

  private static final String DATA_USERNAME = "threadmill-data";
  private static final String DATA_PASSWORD = "threadmill-data-secret";
  private static final String SENTINEL_USERNAME = "threadmill-sentinel";
  private static final String SENTINEL_PASSWORD = "threadmill-sentinel-secret";
  private static final String MASTER = "threadmill-master";

  @TempDir
  static Path tempDirectory;

  private static Path caCertificate;
  private static Path serverCertificate;
  private static Path serverKey;

  @BeforeAll
  static void materializeTestCertificates() throws Exception {
    caCertificate = copyResource("/redis-secure/ca.crt", "ca.crt");
    serverCertificate = copyResource("/redis-secure/server.crt", "server.crt");
    serverKey = tempDirectory.resolve("server.key");
    Files.write(
        serverKey, Base64.getMimeDecoder().decode(resourceBytes("/redis-secure/server.key.b64")));

    try (var input = Files.newInputStream(serverCertificate)) {
      var certificate =
          (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(input);
      assertThat(certificate.getNotAfter().toInstant())
          .as("committed TLS fixture must not expire during normal maintenance")
          .isAfter(Instant.now().plus(Duration.ofDays(365)));
    }
  }

  @Test
  void authenticatedVerifiedTlsClusterKeepsOptionalLuaKeysInOneSlot() throws Exception {
    int hostPort = availablePort();
    try (var redis = clusterContainer(hostPort, true)) {
      redis.start();
      prepareCluster(redis);

      var config = new RedisStoreConfig.Cluster(
          List.of(new RedisStoreConfig.HostAndPort("localhost", hostPort)),
          "master",
          new RedisStoreConfig.Credentials(DATA_USERNAME, DATA_PASSWORD),
          RedisStoreConfig.Tls.verified());

      try (var client = secureClusterClient(config);
          var inspection = client.connect()) {
        var store = injectedStore(client);
        try {
          assertStoreRoundTrip(store, "cluster (client-injected)");
          assertThat(inspection.sync().exists(RedisKeys.NO_KEY))
              .as("the optional-key sentinel is protocol-only and must never be materialized")
              .isZero();
        } finally {
          store.close();
        }
      }
    }
  }

  @Test
  void authenticatedVerifiedTlsSentinelUsesSeparateControlAndDataCredentials() throws Exception {
    int dataPort = availablePort();
    int sentinelPort = differentAvailablePort(dataPort);
    try (var redis = sentinelContainer(dataPort, sentinelPort)) {
      redis.start();
      var config = new RedisStoreConfig.Sentinel(
          MASTER,
          List.of(new RedisStoreConfig.HostAndPort("localhost", sentinelPort)),
          new RedisStoreConfig.Credentials(DATA_USERNAME, DATA_PASSWORD),
          new RedisStoreConfig.Credentials(SENTINEL_USERNAME, SENTINEL_PASSWORD),
          RedisStoreConfig.Tls.verified());

      try (var client = secureSentinelClient(config)) {
        var store = injectedStore(client);
        try {
          assertStoreRoundTrip(store, "standalone (client-injected)");
        } finally {
          store.close();
        }
      }
    }
  }

  @Test
  void authenticatedTlsStartupFailureDoesNotExposeCredentials() throws Exception {
    int hostPort = availablePort();
    try (var redis = clusterContainer(hostPort, false)) {
      redis.start();
      prepareCluster(redis);

      var wrongPassword = "must-never-appear-in-errors";
      var config = new RedisStoreConfig.Cluster(
          List.of(new RedisStoreConfig.HostAndPort("localhost", hostPort)),
          "master",
          new RedisStoreConfig.Credentials(DATA_USERNAME, wrongPassword),
          RedisStoreConfig.Tls.unverified());

      var failure = catchThrowable(() -> new RedisJobStore(config));
      assertThat(failure)
          .isNotNull()
          .hasMessageContaining("authentication failed")
          .hasMessageContaining("cluster nodes=1")
          .hasCauseInstanceOf(RedisConnectionException.class);
      assertThat(stackTrace(failure)).doesNotContain(DATA_USERNAME, wrongPassword);

      var standaloneUri = RedisURI.builder()
          .withHost("localhost")
          .withPort(hostPort)
          .withAuthentication(DATA_USERNAME, wrongPassword.toCharArray())
          .withSsl(true)
          .withVerifyPeer(SslVerifyMode.NONE)
          .build();
      var standaloneFailure = catchThrowable(() -> new RedisJobStore(standaloneUri));
      assertThat(standaloneFailure)
          .isNotNull()
          .hasMessageContaining("standalone host=localhost")
          .hasMessageContaining("authentication failed");
      assertThat(stackTrace(standaloneFailure)).doesNotContain(DATA_USERNAME, wrongPassword);
    }
  }

  private static void assertStoreRoundTrip(RedisJobStore store, String topology) {
    var job = Job.builder()
        .spec(JobSpec.of("com.example.SecureHandler", new JobArgument("java.lang.String", "\"x\"")))
        .build();
    store.insert(job);

    assertThat(store.findById(job.id())).isPresent();
    var claimed = store.claimReady(NodeId.newId(), "default", 1, Instant.now()).getFirst();
    long claimedVersion = claimed.version();
    claimed.transitionTo(JobState.SUCCEEDED, Instant.now());
    store.saveAtomic(claimed, claimedVersion);
    assertThat(store.softDelete(job.id())).isTrue();
    assertThat(store.findById(job.id()))
        .get()
        .extracting(Job::currentState)
        .isEqualTo(JobState.DELETED);
    assertThat(store.describe())
        .contains(topology)
        .doesNotContain(DATA_USERNAME, DATA_PASSWORD, SENTINEL_USERNAME, SENTINEL_PASSWORD);
  }

  private static SecureRedisContainer clusterContainer(
      int hostPort, boolean requireClientCertificate) {
    var config = """
        bind 0.0.0.0
        protected-mode no
        port 0
        tls-port 6379
        tls-cert-file /secure/server.crt
        tls-key-file /secure/server.key
        tls-ca-cert-file /secure/ca.crt
        tls-auth-clients %s
        tls-cluster yes
        cluster-enabled yes
        cluster-config-file /tmp/nodes.conf
        cluster-node-timeout 1000
        cluster-announce-ip 127.0.0.1
        cluster-announce-port %d
        cluster-announce-tls-port %d
        cluster-announce-bus-port 16379
        appendonly yes
        maxmemory-policy noeviction
        user default off
        user %s on >%s ~* &* +@all
        """.formatted(
        requireClientCertificate ? "yes" : "no", hostPort, hostPort, DATA_USERNAME, DATA_PASSWORD);
    return secureContainer()
        .bindFixedPort(hostPort, 6379)
        .withCopyToContainer(Transferable.of(config), "/secure/redis.conf")
        .withCommand("redis-server", "/secure/redis.conf")
        .waitingFor(Wait.forListeningPort());
  }

  private static SecureRedisContainer sentinelContainer(int dataPort, int sentinelPort) {
    var dataConfig = """
        bind 0.0.0.0
        protected-mode no
        port 0
        tls-port %d
        tls-cert-file /secure/server.crt
        tls-key-file /secure/server.key
        tls-ca-cert-file /secure/ca.crt
        tls-auth-clients yes
        appendonly yes
        maxmemory-policy noeviction
        pidfile /tmp/redis-data.pid
        user default off
        user %s on >%s ~* &* +@all
        """.formatted(dataPort, DATA_USERNAME, DATA_PASSWORD);
    var sentinelConfig = """
        bind 0.0.0.0
        protected-mode no
        port 0
        tls-port %d
        tls-cert-file /secure/server.crt
        tls-key-file /secure/server.key
        tls-ca-cert-file /secure/ca.crt
        tls-auth-clients yes
        tls-replication yes
        pidfile /tmp/redis-sentinel.pid
        user default off
        user %s on >%s ~* &* +@all
        sentinel monitor %s 127.0.0.1 %d 1
        sentinel auth-user %s %s
        sentinel auth-pass %s %s
        sentinel sentinel-user %s
        sentinel sentinel-pass %s
        """.formatted(
            sentinelPort,
            SENTINEL_USERNAME,
            SENTINEL_PASSWORD,
            MASTER,
            dataPort,
            MASTER,
            DATA_USERNAME,
            MASTER,
            DATA_PASSWORD,
            SENTINEL_USERNAME,
            SENTINEL_PASSWORD);
    return secureContainer()
        .bindFixedPort(dataPort, dataPort)
        .bindFixedPort(sentinelPort, sentinelPort)
        .withCopyToContainer(Transferable.of(dataConfig), "/secure/data.conf")
        .withCopyToContainer(Transferable.of(sentinelConfig), "/secure/sentinel.conf")
        .withCommand(
            "sh",
            "-c",
            "redis-server /secure/data.conf --daemonize yes"
                + " && exec redis-server /secure/sentinel.conf --sentinel")
        .waitingFor(Wait.forListeningPorts(dataPort, sentinelPort));
  }

  private static SecureRedisContainer secureContainer() {
    return new SecureRedisContainer()
        .withCopyFileToContainer(MountableFile.forHostPath(caCertificate), "/secure/ca.crt")
        .withCopyFileToContainer(MountableFile.forHostPath(serverCertificate), "/secure/server.crt")
        .withCopyFileToContainer(MountableFile.forHostPath(serverKey), "/secure/server.key");
  }

  private static RedisClusterClient secureClusterClient(RedisStoreConfig.Cluster config) {
    var client = RedisClusterClient.create(RedisConnectionConfig.clusterUris(config));
    client.setOptions(RedisClusterOptions.topologyRefreshing()
        .mutate()
        .sslOptions(testSslOptions())
        .build());
    return client;
  }

  private static RedisClient secureSentinelClient(RedisStoreConfig.Sentinel config) {
    var client = RedisClient.create(RedisConnectionConfig.sentinelUri(config));
    client.setOptions(RedisClusterOptions.standaloneOptions()
        .mutate()
        .sslOptions(testSslOptions())
        .build());
    return client;
  }

  private static SslOptions testSslOptions() {
    return SslOptions.builder()
        .trustManager(caCertificate.toFile())
        .keyManager(serverCertificate.toFile(), serverKey.toFile(), null)
        .build();
  }

  private static RedisJobStore injectedStore(RedisClient client) {
    return new RedisJobStore(client);
  }

  private static RedisJobStore injectedStore(RedisClusterClient client) {
    return new RedisJobStore(client);
  }

  private static void prepareCluster(SecureRedisContainer redis) throws Exception {
    var addSlots = redis.execInContainer(
        "redis-cli",
        "--tls",
        "--cacert",
        "/secure/ca.crt",
        "--cert",
        "/secure/server.crt",
        "--key",
        "/secure/server.key",
        "--user",
        DATA_USERNAME,
        "--pass",
        DATA_PASSWORD,
        "-p",
        "6379",
        "cluster",
        "addslotsrange",
        "0",
        "16383");
    assertThat(addSlots.getExitCode()).as(addSlots.getStderr()).isZero();

    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    String lastInfo = "";
    do {
      var info = redis.execInContainer(
          "redis-cli",
          "--tls",
          "--cacert",
          "/secure/ca.crt",
          "--cert",
          "/secure/server.crt",
          "--key",
          "/secure/server.key",
          "--user",
          DATA_USERNAME,
          "--pass",
          DATA_PASSWORD,
          "-p",
          "6379",
          "cluster",
          "info");
      assertThat(info.getExitCode()).as(info.getStderr()).isZero();
      lastInfo = info.getStdout();
      if (lastInfo.contains("cluster_state:ok")) return;
      Thread.sleep(100);
    } while (System.nanoTime() < deadline);
    throw new AssertionError("Cluster did not become ready: " + lastInfo);
  }

  private static int differentAvailablePort(int excluded) throws IOException {
    int port;
    do {
      port = availablePort();
    } while (port == excluded);
    return port;
  }

  private static int availablePort() throws IOException {
    // Redis Cluster and Sentinel advertise their ports before Testcontainers can
    // assign random host mappings. The fixed binding therefore has an unavoidable
    // close-to-bind TOCTOU window; the resource lock prevents this suite racing itself.
    try (var socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static byte[] resourceBytes(String resource) throws IOException {
    try (var input = RedisSecureTopologyTest.class.getResourceAsStream(resource)) {
      if (input == null) throw new IOException("Missing test resource: " + resource);
      return input.readAllBytes();
    }
  }

  private static Path copyResource(String resource, String filename) throws IOException {
    var destination = tempDirectory.resolve(filename);
    Files.write(destination, resourceBytes(resource));
    return destination;
  }

  private static String stackTrace(Throwable failure) {
    var output = new StringWriter();
    failure.printStackTrace(new PrintWriter(output));
    return output.toString();
  }

  private static final class SecureRedisContainer extends GenericContainer<SecureRedisContainer> {

    private SecureRedisContainer() {
      super(DockerImageName.parse("redis:7-alpine"));
    }

    private SecureRedisContainer bindFixedPort(int hostPort, int containerPort) {
      addFixedExposedPort(hostPort, containerPort);
      return this;
    }
  }
}
