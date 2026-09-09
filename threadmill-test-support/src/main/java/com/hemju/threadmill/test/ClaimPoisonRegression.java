package com.hemju.threadmill.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Instant;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobSnapshot;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.serialization.SerializationException;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStore;

/** Shared fault injection for deterministic claim serialization failures. */
public final class ClaimPoisonRegression {
  private ClaimPoisonRegression() {}

  /** Fails only when the deliberately poisoned candidate enters PROCESSING. */
  public static JobSerializer serializer() {
    var delegate = new JsonJobSerializer();
    return (JobSerializer) Proxy.newProxyInstance(
        JobSerializer.class.getClassLoader(),
        new Class<?>[] {JobSerializer.class},
        (proxy, method, args) -> {
          if (method.getName().equals("serializeJob")
              && args[0] instanceof JobSnapshot snapshot
              && snapshot.currentState() == JobState.PROCESSING
              && snapshot.spec().handlerType().equals("poison.Handler")) {
            throw new SerializationException("Injected processing serialization rejection");
          }
          try {
            return method.invoke(delegate, args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }

  /** Every committed claim is returned, and poison leaves the ready index. */
  public static void verify(JobStore store) {
    var first = Job.builder().spec(JobSpec.of("normal.Handler")).priority(3).build();
    var poison = Job.builder().spec(JobSpec.of("poison.Handler")).priority(2).build();
    var last = Job.builder().spec(JobSpec.of("normal.Handler")).priority(1).build();
    store.insert(first);
    store.insert(poison);
    store.insert(last);
    var claimed = store.claimReady(NodeId.newId(), "default", 3, Instant.now());
    assertThat(claimed).extracting(Job::id).containsExactly(first.id(), last.id());
    assertThat(store.countsByState().getOrDefault(JobState.PROCESSING, 0L)).isEqualTo(2);
    assertThat(store.countsByState().getOrDefault(JobState.QUARANTINED, 0L)).isEqualTo(1);
    assertThat(store.findById(poison.id())).hasValueSatisfying(job -> {
      assertThat(job.currentState()).isEqualTo(JobState.QUARANTINED);
      assertThat(job.version()).isEqualTo(poison.version() + 1);
    });
    assertThat(store.claimReady(NodeId.newId(), "default", 3, Instant.now())).isEmpty();
  }
}
