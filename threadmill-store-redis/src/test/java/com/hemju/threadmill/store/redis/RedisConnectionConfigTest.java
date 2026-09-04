package com.hemju.threadmill.store.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisCredentials;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;
import org.junit.jupiter.api.Test;

class RedisConnectionConfigTest {

  private static final RedisStoreConfig.HostAndPort FIRST =
      new RedisStoreConfig.HostAndPort("redis-1.example", 6379);
  private static final RedisStoreConfig.HostAndPort SECOND =
      new RedisStoreConfig.HostAndPort("redis-2.example", 6380);

  @Test
  void clusterUrisCarryAclCredentialsAndVerifiedTlsToEverySeed() {
    var config = new RedisStoreConfig.Cluster(
        List.of(FIRST, SECOND),
        "master",
        new RedisStoreConfig.Credentials("cluster-user", "cluster-password"),
        RedisStoreConfig.Tls.verified());

    var uris = RedisConnectionConfig.clusterUris(config);

    assertThat(uris).hasSize(2).allSatisfy(uri -> {
      assertThat(credentials(uri).getUsername()).isEqualTo("cluster-user");
      assertThat(credentials(uri).getPassword()).containsExactly("cluster-password".toCharArray());
      assertThat(uri.isSsl()).isTrue();
      assertThat(uri.getVerifyMode()).isEqualTo(SslVerifyMode.FULL);
    });
  }

  @Test
  void sentinelUriKeepsCredentialsDistinctUnderTheSharedTlsPolicy() {
    var config = new RedisStoreConfig.Sentinel(
        "threadmill-master",
        List.of(FIRST, SECOND),
        new RedisStoreConfig.Credentials("data-user", "data-password"),
        new RedisStoreConfig.Credentials("sentinel-user", "sentinel-password"),
        RedisStoreConfig.Tls.verified());

    var uri = RedisConnectionConfig.sentinelUri(config);

    assertThat(uri.getSentinelMasterId()).isEqualTo("threadmill-master");
    assertThat(credentials(uri).getUsername()).isEqualTo("data-user");
    assertThat(credentials(uri).getPassword()).containsExactly("data-password".toCharArray());
    assertThat(uri.isSsl()).isTrue();
    assertThat(uri.getVerifyMode()).isEqualTo(SslVerifyMode.FULL);
    assertThat(uri.getSentinels()).hasSize(2).allSatisfy(sentinel -> {
      assertThat(credentials(sentinel).getUsername()).isEqualTo("sentinel-user");
      assertThat(credentials(sentinel).getPassword())
          .containsExactly("sentinel-password".toCharArray());
      assertThat(sentinel.isSsl()).isTrue();
      assertThat(sentinel.getVerifyMode()).isEqualTo(SslVerifyMode.FULL);
    });
  }

  @Test
  void topologyDescriptionsAndConfigurationStringsNeverExposeCredentials() {
    var config = new RedisStoreConfig.Sentinel(
        "threadmill-master",
        List.of(FIRST),
        new RedisStoreConfig.Credentials("private-data-user", "private-data-password"),
        new RedisStoreConfig.Credentials("private-sentinel-user", "private-sentinel-password"),
        RedisStoreConfig.Tls.verified());

    assertThat(config.toString())
        .doesNotContain("private-data-user", "private-data-password")
        .doesNotContain("private-sentinel-user", "private-sentinel-password");
    assertThat(RedisConnectionConfig.describe(config))
        .doesNotContain("private-data-user", "private-data-password")
        .doesNotContain("private-sentinel-user", "private-sentinel-password")
        .contains("tls=verified");
  }

  @Test
  void usernameWithoutPasswordIsRejectedBeforeAConnectionAttempt() {
    assertThatThrownBy(() -> new RedisStoreConfig.Credentials("username", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("password must be set when username is set");
  }

  @Test
  void disablingPeerVerificationWithoutTlsIsRejected() {
    assertThatThrownBy(() -> new RedisStoreConfig.Tls(false, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("non-default TLS verification requires TLS to be enabled");
  }

  @Test
  void certificateAuthorityVerificationModeReachesEveryClusterSeed() {
    var config = new RedisStoreConfig.Cluster(
        List.of(FIRST, SECOND),
        "master",
        RedisStoreConfig.Credentials.none(),
        RedisStoreConfig.Tls.verifiedCertificateAuthority());

    assertThat(RedisConnectionConfig.clusterUris(config))
        .allSatisfy(uri -> assertThat(uri.getVerifyMode()).isEqualTo(SslVerifyMode.CA));
    assertThat(RedisConnectionConfig.describe(config)).contains("tls=ca-verified");
  }

  @Test
  void redactedFailureKeepsOnlySafeExceptionTypesInItsCause() {
    var original = new RedisConnectionException(
        "Cannot connect to redis://private-user:private-password@example",
        new IllegalStateException("WRONGPASS private-password"));

    var redacted = RedisConnectionConfig.redactedConnectionFailure("cluster nodes=1", original);

    assertThat(redacted)
        .hasMessageContaining("authentication failed")
        .hasCauseInstanceOf(RedisConnectionException.class);
    assertThat(stackTrace(redacted))
        .contains("RedisConnectionException -> IllegalStateException")
        .doesNotContain("private-user", "private-password");
  }

  @Test
  void legacySentinelPasswordConstructorStillAuthenticatesOnlyTheDataNode() {
    var uri = RedisConnectionConfig.sentinelUri(
        new RedisStoreConfig.Sentinel("threadmill-master", List.of(FIRST), "data-password"));

    assertThat(credentials(uri).hasUsername()).isFalse();
    assertThat(credentials(uri).getPassword()).containsExactly("data-password".toCharArray());
    assertThat(credentials(uri.getSentinels().getFirst()).hasPassword()).isFalse();
  }

  private static RedisCredentials credentials(RedisURI uri) {
    return uri.getCredentialsProvider().resolveCredentials().block();
  }

  private static String stackTrace(Throwable failure) {
    var output = new StringWriter();
    failure.printStackTrace(new PrintWriter(output));
    return output.toString();
  }
}
