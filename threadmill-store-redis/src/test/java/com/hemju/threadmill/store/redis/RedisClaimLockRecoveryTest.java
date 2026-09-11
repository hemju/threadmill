package com.hemju.threadmill.store.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandInterruptedException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobRelationship;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStoreCapabilities;

/** Faults are injected after real Redis commands execute, never in place of the datastore. */
class RedisClaimLockRecoveryTest {
  @SuppressWarnings("resource")
  private static final GenericContainer<?> REDIS =
      new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);

  private static RedisURI uri;
  private static RedisClient adminClient;
  private static StatefulRedisConnection<String, String> adminConnection;

  @BeforeAll
  static void start() {
    REDIS.start();
    uri = RedisURI.create("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
    adminClient = RedisClient.create(uri);
    adminConnection = adminClient.connect();
  }

  @AfterAll
  static void stop() {
    if (adminConnection != null) adminConnection.close();
    if (adminClient != null) adminClient.shutdown();
    REDIS.stop();
  }

  @BeforeEach
  void reset() {
    adminConnection.sync().flushdb();
  }

  @ParameterizedTest
  @ValueSource(strings = {"insert", "bulk", "dedup", "claim"})
  void lostClaimLockAcknowledgementReleasesTheOwnedToken(String operation) {
    try (var client = new FaultClient();
        var store = open(client)) {
      var job = keyed("key-a");
      if (operation.equals("claim")) store.insert(job);
      var lockKey = RedisKeys.concurrencyClaimLock("key-a");
      var failure = new RedisCommandTimeoutException("lost SET acknowledgement");
      var injected = new AtomicBoolean();
      client.after = (method, args, result) -> {
        if (method.getName().equals("set")
            && lockKey.equals(args[0])
            && "OK".equals(result)
            && injected.compareAndSet(false, true)) throw failure;
      };
      assertThatThrownBy(() -> run(store, operation, job)).isSameAs(failure);
      assertThat(injected).isTrue();
      assertThat(adminConnection.sync().get(lockKey)).isNull();
      assertThat(job.version()).isEqualTo(operation.equals("claim") ? 1L : 0L);
      run(store, operation, job);
      assertThat(store.findById(job.id())).isPresent();
    }
  }

  @Test
  void failedLaterBulkLockAcquisitionReleasesEarlierLocks() {
    try (var client = new FaultClient();
        var store = open(client)) {
      var jobs = List.of(keyed("key-a"), keyed("key-b"));
      var failure = new RedisConnectionException("lost second SET reply");
      var injected = new AtomicBoolean();
      client.after = (method, args, result) -> {
        if (method.getName().equals("set")
            && RedisKeys.concurrencyClaimLock("key-b").equals(args[0])
            && "OK".equals(result)
            && injected.compareAndSet(false, true)) throw failure;
      };
      assertThatThrownBy(() -> store.insertAll(jobs)).isSameAs(failure);
      assertThat(adminConnection.sync().get(RedisKeys.concurrencyClaimLock("key-a")))
          .isNull();
      assertThat(adminConnection.sync().get(RedisKeys.concurrencyClaimLock("key-b")))
          .isNull();
      assertThat(jobs).allSatisfy(job -> {
        assertThat(job.version()).isZero();
        assertThat(store.findById(job.id())).isEmpty();
      });
      assertThat(store.insertAll(jobs)).hasSize(2);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"insert", "bulk", "dedup"})
  void failedWorkflowResnapshotReleasesTheAcquiredClaimLock(String operation) {
    try (var client = new FaultClient();
        var store = open(client)) {
      var parent = keyed("key-a");
      store.insert(parent);
      var child = Job.builder()
          .spec(parent.spec())
          .relationship(new JobRelationship(parent.id(), JobRelationship.Kind.WORKFLOW_STEP))
          .initialState(JobState.AWAITING)
          .build();
      var lockKey = RedisKeys.concurrencyClaimLock("key-a");
      var failure = new RedisConnectionException("lost parent snapshot reply");
      var injected = new AtomicBoolean();
      client.after = (method, args, result) -> {
        if (method.getName().equals("hgetall")
            && RedisKeys.job(parent.id()).equals(args[0])
            && adminConnection.sync().get(lockKey) != null
            && injected.compareAndSet(false, true)) throw failure;
      };
      assertThatThrownBy(() -> run(store, operation, child)).isSameAs(failure);
      assertThat(injected).isTrue();
      assertThat(adminConnection.sync().get(lockKey)).isNull();
      assertThat(child.version()).isZero();
      assertThat(store.findById(child.id())).isEmpty();
      run(store, operation, child);
      assertThat(store.findById(child.id())).isPresent();
    }
  }

  @Test
  void uncertainAcquisitionCleanupCannotDeleteAReplacementOwnersLock() {
    try (var client = new FaultClient();
        var store = open(client)) {
      var job = keyed("key-a");
      var lockKey = RedisKeys.concurrencyClaimLock("key-a");
      var failure = new RedisCommandTimeoutException("reply lost after ownership changed");
      client.after = (method, args, result) -> {
        if (method.getName().equals("set") && lockKey.equals(args[0])) {
          adminConnection.sync().set(lockKey, "replacement-owner");
          throw failure;
        }
      };
      assertThatThrownBy(() -> store.insert(job)).isSameAs(failure);
      assertThat(adminConnection.sync().get(lockKey)).isEqualTo("replacement-owner");
      assertThat(store.findById(job.id())).isEmpty();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"insert", "claim"})
  void interruptedAcquisitionCleansTheTokenAndRestoresInterruption(String operation) {
    try (var client = new FaultClient();
        var store = open(client)) {
      var job = keyed("key-a");
      if (operation.equals("claim")) store.insert(job);
      var lockKey = RedisKeys.concurrencyClaimLock("key-a");
      var failure =
          new RedisCommandInterruptedException(new InterruptedException("shutdown after SET"));
      var injected = new AtomicBoolean();
      client.after = (method, args, result) -> {
        if (method.getName().equals("set")
            && lockKey.equals(args[0])
            && "OK".equals(result)
            && injected.compareAndSet(false, true)) {
          Thread.currentThread().interrupt();
          throw failure;
        }
      };
      try {
        assertThatThrownBy(() -> run(store, operation, job)).isSameAs(failure);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
      } finally {
        Thread.interrupted();
      }
      assertThat(adminConnection.sync().get(lockKey)).isNull();
      run(store, operation, job);
      assertThat(store.findById(job.id())).isPresent();
    }
  }

  @Test
  void failedCleanupStillReleasesOtherBulkLocksAndPreservesAcquisitionFailure() {
    try (var client = new FaultClient();
        var store = open(client)) {
      var jobs = List.of(keyed("key-a"), keyed("key-b"), keyed("key-c"));
      var acquisitionFailure = new RedisConnectionException("lost final SET reply");
      var cleanupFailure = new RedisConnectionException("lost cleanup acknowledgement");
      var injectedAcquisition = new AtomicBoolean();
      var injectedCleanup = new AtomicBoolean();
      client.after = (method, args, result) -> {
        if (method.getName().equals("set")
            && RedisKeys.concurrencyClaimLock("key-c").equals(args[0])
            && "OK".equals(result)
            && injectedAcquisition.compareAndSet(false, true)) throw acquisitionFailure;
        if ((method.getName().equals("eval") || method.getName().equals("evalsha"))
            && ((String[]) args[2])[0].equals(RedisKeys.concurrencyClaimLock("key-b"))
            && injectedCleanup.compareAndSet(false, true)) throw cleanupFailure;
      };
      assertThatThrownBy(() -> store.insertAll(jobs))
          .isSameAs(acquisitionFailure)
          .satisfies(
              failure -> assertThat(failure.getSuppressed()).containsExactly(cleanupFailure));
      for (var key : List.of("key-a", "key-b", "key-c")) {
        assertThat(adminConnection.sync().get(RedisKeys.concurrencyClaimLock(key)))
            .isNull();
      }
      assertThat(store.insertAll(jobs)).hasSize(3);
    }
  }

  private static Job keyed(String key) {
    return Job.builder()
        .spec(JobSpec.of("com.example.Handler", new JobArgument("java.lang.String", "\"test\"")))
        .concurrencyKey(key)
        .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
        .build();
  }

  private static RedisJobStore open(RedisClient client) {
    return new RedisJobStore(client, new JsonJobSerializer(), JobStoreCapabilities.defaults());
  }

  private static void run(RedisJobStore store, String operation, Job job) {
    switch (operation) {
      case "insert" -> store.insert(job);
      case "bulk" -> store.insertAll(List.of(job));
      case "dedup" -> store.enqueueIfAbsent(job, "dedup-key", Duration.ofMinutes(1), Instant.now());
      case "claim" -> store.claimReady(NodeId.newId(), "default", 1, Instant.now());
      default -> throw new IllegalArgumentException(operation);
    }
  }

  @FunctionalInterface
  private interface CommandHook {
    void after(Method method, Object[] args, Object result);
  }

  private static final class FaultClient extends RedisClient {
    private CommandHook after = (method, args, result) -> {};

    private FaultClient() {
      super(null, uri);
    }

    @Override
    public StatefulRedisConnection<String, String> connect() {
      var connection = super.connect();
      var commands = proxy(RedisCommands.class, (target, method, args) -> {
        var result = invoke(connection.sync(), method, args);
        after.after(method, args, result);
        return result;
      });
      return proxy(
          StatefulRedisConnection.class,
          (target, method, args) ->
              method.getName().equals("sync") ? commands : invoke(connection, method, args));
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<?> type, InvocationHandler handler) {
      return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
    }

    private static Object invoke(Object receiver, Method method, Object[] args) throws Throwable {
      try {
        return method.invoke(receiver, args);
      } catch (InvocationTargetException failure) {
        throw failure.getCause();
      }
    }
  }
}
