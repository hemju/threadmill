package com.hemju.threadmill.store.redis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;

import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;

/** Builds redaction-safe Lettuce connection details from Threadmill topology configuration. */
final class RedisConnectionConfig {

  private static final int MAX_CAUSE_DEPTH = 32;

  private RedisConnectionConfig() {}

  static RedisURI sentinelUri(RedisStoreConfig.Sentinel config) {
    var builder = RedisURI.builder().withSentinelMasterId(config.master());
    applyCredentials(builder, config.dataNodeCredentials());
    for (var node : config.nodes()) {
      builder.withSentinel(redisUri(node, config.sentinelCredentials(), config.tls()));
    }
    // RedisURI.Builder propagates the aggregate TLS policy to every Sentinel URI.
    applyTls(builder, config.tls());
    return builder.build();
  }

  static List<RedisURI> clusterUris(RedisStoreConfig.Cluster config) {
    return config.nodes().stream()
        .map(node -> redisUri(node, config.credentials(), config.tls()))
        .toList();
  }

  static String describe(RedisStoreConfig config) {
    return switch (config) {
      case RedisStoreConfig.Standalone standalone -> describeUri(standalone.uri());
      case RedisStoreConfig.Sentinel sentinel ->
        "sentinel master="
            + sentinel.master()
            + " nodes="
            + sentinel.nodes().size()
            + " tls="
            + describeTls(sentinel.tls());
      case RedisStoreConfig.Cluster cluster ->
        "cluster nodes=" + cluster.nodes().size() + " tls=" + describeTls(cluster.tls());
    };
  }

  static String describeUri(RedisURI uri) {
    return "standalone host="
        + uri.getHost()
        + " port="
        + uri.getPort()
        + " tls="
        + (uri.isSsl()
            ? (uri.getVerifyMode() == SslVerifyMode.NONE ? "unverified" : "verified")
            : "disabled");
  }

  static RedisConnectionException redactedConnectionFailure(
      String topology, RuntimeException failure) {
    String reason;
    if (failureContains(failure, "WRONGPASS")
        || failureContains(failure, "NOAUTH")
        || failureContains(failure, "authentication")) {
      reason = "authentication failed";
    } else if (failureContains(failure, "SSL")
        || failureContains(failure, "TLS")
        || failureContains(failure, "certificate")) {
      reason = "TLS negotiation failed";
    } else {
      reason = "connection failed";
    }
    var diagnostic = new RedisConnectionException(
        "Sanitized original exception chain: " + safeExceptionChain(failure));
    return new RedisConnectionException(
        "Unable to connect to " + topology + ": " + reason + " (credentials redacted)", diagnostic);
  }

  private static RedisURI redisUri(
      RedisStoreConfig.HostAndPort node,
      RedisStoreConfig.Credentials credentials,
      RedisStoreConfig.Tls tls) {
    var builder = RedisURI.Builder.redis(node.host(), node.port());
    applyCredentials(builder, credentials);
    applyTls(builder, tls);
    return builder.build();
  }

  private static void applyCredentials(
      RedisURI.Builder builder, RedisStoreConfig.Credentials credentials) {
    if (!credentials.configured()) return;
    if (credentials.username() == null) {
      builder.withPassword(credentials.password().toCharArray());
    } else {
      builder.withAuthentication(credentials.username(), credentials.password().toCharArray());
    }
  }

  private static void applyTls(RedisURI.Builder builder, RedisStoreConfig.Tls tls) {
    if (!tls.enabled()) return;
    builder.withSsl(true).withVerifyPeer(tls.verifyMode());
  }

  private static String describeTls(RedisStoreConfig.Tls tls) {
    if (!tls.enabled()) return "disabled";
    return switch (tls.verifyMode()) {
      case FULL -> "verified";
      case CA -> "ca-verified";
      case NONE -> "unverified";
    };
  }

  private static boolean failureContains(Throwable failure, String fragment) {
    var expected = fragment.toLowerCase(Locale.ROOT);
    var seen = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
    int depth = 0;
    for (var current = failure;
        current != null && depth++ < MAX_CAUSE_DEPTH && seen.add(current);
        current = current.getCause()) {
      if (current.getClass().getSimpleName().toLowerCase(Locale.ROOT).contains(expected))
        return true;
      if (current.getMessage() != null
          && current.getMessage().toLowerCase(Locale.ROOT).contains(expected)) return true;
    }
    return false;
  }

  private static String safeExceptionChain(Throwable failure) {
    var names = new ArrayList<String>();
    var seen = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
    int depth = 0;
    for (var current = failure;
        current != null && depth++ < MAX_CAUSE_DEPTH && seen.add(current);
        current = current.getCause()) {
      names.add(current.getClass().getSimpleName());
    }
    return String.join(" -> ", names);
  }
}
