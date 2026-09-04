package com.hemju.threadmill.store.redis;

import java.util.List;
import java.util.Objects;

import io.lettuce.core.RedisURI;

/** Configuration for creating a Redis-backed job store across supported topologies. */
public sealed interface RedisStoreConfig
    permits RedisStoreConfig.Standalone, RedisStoreConfig.Sentinel, RedisStoreConfig.Cluster {

  RedisSafetyValidation safetyValidation();

  record RedisSafetyValidation(boolean requireNoEviction, boolean externallyValidated) {
    public static RedisSafetyValidation strict() {
      return new RedisSafetyValidation(true, false);
    }

    public static RedisSafetyValidation externallyValidatedMode() {
      return new RedisSafetyValidation(true, true);
    }
  }

  /** Static Redis ACL credentials. Password-only authentication is supported. */
  record Credentials(String username, String password) {
    public Credentials {
      username = normalize(username);
      password = normalize(password);
      if (username != null && password == null) {
        throw new IllegalArgumentException("password must be set when username is set");
      }
    }

    public static Credentials none() {
      return new Credentials(null, null);
    }

    public static Credentials passwordOnly(String password) {
      return new Credentials(null, password);
    }

    boolean configured() {
      return password != null;
    }

    @Override
    public String toString() {
      return configured() ? "Credentials[redacted]" : "Credentials[none]";
    }

    private static String normalize(String value) {
      return value == null || value.isBlank() ? null : value;
    }
  }

  /** TLS transport settings. Peer verification remains enabled by default. */
  record Tls(boolean enabled, boolean verifyPeer) {
    public Tls {
      if (!enabled && !verifyPeer) {
        throw new IllegalArgumentException("verifyPeer=false requires TLS to be enabled");
      }
    }

    public static Tls disabled() {
      return new Tls(false, true);
    }

    public static Tls verified() {
      return new Tls(true, true);
    }
  }

  record Standalone(RedisURI uri, RedisSafetyValidation safetyValidation)
      implements RedisStoreConfig {
    public Standalone(RedisURI uri) {
      this(uri, RedisSafetyValidation.strict());
    }

    public Standalone {
      Objects.requireNonNull(uri, "uri");
      Objects.requireNonNull(safetyValidation, "safetyValidation");
    }
  }

  record Sentinel(
      String master,
      List<HostAndPort> nodes,
      Credentials dataNodeCredentials,
      Credentials sentinelCredentials,
      Tls tls,
      RedisSafetyValidation safetyValidation)
      implements RedisStoreConfig {
    public Sentinel(String master, List<HostAndPort> nodes, String password) {
      this(master, nodes, password, RedisSafetyValidation.strict());
    }

    public Sentinel(
        String master,
        List<HostAndPort> nodes,
        String password,
        RedisSafetyValidation safetyValidation) {
      this(
          master,
          nodes,
          Credentials.passwordOnly(password),
          Credentials.none(),
          Tls.disabled(),
          safetyValidation);
    }

    public Sentinel(
        String master,
        List<HostAndPort> nodes,
        Credentials dataNodeCredentials,
        Credentials sentinelCredentials,
        Tls tls) {
      this(
          master,
          nodes,
          dataNodeCredentials,
          sentinelCredentials,
          tls,
          RedisSafetyValidation.strict());
    }

    public Sentinel {
      Objects.requireNonNull(master, "master");
      Objects.requireNonNull(dataNodeCredentials, "dataNodeCredentials");
      Objects.requireNonNull(sentinelCredentials, "sentinelCredentials");
      Objects.requireNonNull(tls, "tls");
      Objects.requireNonNull(safetyValidation, "safetyValidation");
      if (master.isBlank()) throw new IllegalArgumentException("master must not be blank");
      nodes = List.copyOf(nodes);
      if (nodes.isEmpty()) throw new IllegalArgumentException("sentinel nodes must not be empty");
    }

    /**
     * Returns the legacy password-only Redis data-node credential.
     *
     * @deprecated use {@link #dataNodeCredentials()} to access the complete ACL credential
     */
    @Deprecated(forRemoval = false)
    public String password() {
      return dataNodeCredentials.password();
    }
  }

  record Cluster(
      List<HostAndPort> nodes,
      String readFrom,
      Credentials credentials,
      Tls tls,
      RedisSafetyValidation safetyValidation)
      implements RedisStoreConfig {
    public Cluster(List<HostAndPort> nodes, String readFrom) {
      this(nodes, readFrom, RedisSafetyValidation.strict());
    }

    public Cluster(
        List<HostAndPort> nodes, String readFrom, RedisSafetyValidation safetyValidation) {
      this(nodes, readFrom, Credentials.none(), Tls.disabled(), safetyValidation);
    }

    public Cluster(List<HostAndPort> nodes, String readFrom, Credentials credentials, Tls tls) {
      this(nodes, readFrom, credentials, tls, RedisSafetyValidation.strict());
    }

    public Cluster {
      Objects.requireNonNull(credentials, "credentials");
      Objects.requireNonNull(tls, "tls");
      Objects.requireNonNull(safetyValidation, "safetyValidation");
      nodes = List.copyOf(nodes);
      if (nodes.isEmpty()) throw new IllegalArgumentException("cluster nodes must not be empty");
      readFrom = readFrom == null || readFrom.isBlank() ? "master" : readFrom;
      if (!"master".equals(readFrom)) {
        throw new IllegalArgumentException("Threadmill engine Redis reads must use master");
      }
    }
  }

  record HostAndPort(String host, int port) {
    public HostAndPort {
      Objects.requireNonNull(host, "host");
      if (host.isBlank()) throw new IllegalArgumentException("host must not be blank");
      if (port <= 0 || port > 65535) throw new IllegalArgumentException("port out of range");
    }
  }
}
