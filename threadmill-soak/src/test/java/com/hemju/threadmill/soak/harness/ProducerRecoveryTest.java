package com.hemju.threadmill.soak.harness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import io.lettuce.core.RedisCommandTimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.ForwardingJobStore;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

class ProducerRecoveryTest {
  @TempDir
  Path temporary;

  @Test
  void lostInsertAcknowledgementReconcilesTheSameJobInsteadOfDuplicatingIt() throws Exception {
    var real = new InMemoryJobStore();
    var calls = new AtomicBoolean();
    var interrupted = new ForwardingJobStore(real) {
      @Override
      public void insert(Job job) {
        assertThat(calls.getAndSet(true)).isFalse();
        super.insert(job);
        throw new RedisCommandTimeoutException("lost acknowledgement");
      }
    };
    try (var trace = new SoakTraceWriter(temporary.resolve("trace.jsonl"))) {
      var job = job();
      new RecoveringProducerStore(interrupted, trace, () -> false).insert(job);
      assertThat(real.countsByState().get(JobState.ENQUEUED)).isEqualTo(1);
      assertThat(job.version()).isEqualTo(1);
    }
    assertThat(Files.readString(temporary.resolve("trace.jsonl")))
        .contains("producer_outage", "producer_recovered");
  }

  @Test
  void lostAtomicBatchAcknowledgementDoesNotReinsertItsMembers() throws Exception {
    var real = new InMemoryJobStore();
    var interrupted = new ForwardingJobStore(real) {
      @Override
      public List<JobId> insertAll(List<Job> jobs) {
        super.insertAll(jobs);
        throw new RedisCommandTimeoutException("lost acknowledgement");
      }
    };
    try (var trace = new SoakTraceWriter(temporary.resolve("trace.jsonl"))) {
      var jobs = List.of(job(), job());
      assertThat(new RecoveringProducerStore(interrupted, trace, () -> false).insertAll(jobs))
          .containsExactlyElementsOf(jobs.stream().map(Job::id).toList());
      assertThat(real.countsByState().get(JobState.ENQUEUED)).isEqualTo(2);
    }
  }

  @Test
  void lostDedupAcknowledgementPreservesCreatedAndCoalescedResults() throws Exception {
    var real = new InMemoryJobStore();
    var interrupted = new ForwardingJobStore(real) {
      @Override
      public EnqueueResult enqueueIfAbsent(Job job, String key, Duration ttl, Instant now) {
        super.enqueueIfAbsent(job, key, ttl, now);
        throw new RedisCommandTimeoutException("lost acknowledgement");
      }
    };
    try (var trace = new SoakTraceWriter(temporary.resolve("trace.jsonl"))) {
      var store = new RecoveringProducerStore(interrupted, trace, () -> false);
      var job = job();
      assertThat(store.enqueueIfAbsent(job, "key", Duration.ofMinutes(1), Instant.now()))
          .isEqualTo(new EnqueueResult.Created(job.id()));
      assertThat(store.enqueueIfAbsent(job, "key", Duration.ofMinutes(1), Instant.now()))
          .isEqualTo(new EnqueueResult.Coalesced(job.id()));
      assertThat(real.countsByState().get(JobState.ENQUEUED)).isEqualTo(1);
    }
  }

  @Test
  void dedupDecoratorPreservesTheCallersTimeAcrossAnOutage() throws Exception {
    var real = new InMemoryJobStore();
    var first = new AtomicBoolean(true);
    var now = Instant.parse("2024-01-02T03:04:05Z");
    var interrupted = new ForwardingJobStore(real) {
      @Override
      public EnqueueResult enqueueIfAbsent(Job job, String key, Duration ttl, Instant suppliedNow) {
        assertThat(suppliedNow).isEqualTo(now);
        if (first.getAndSet(false)) throw new RedisCommandTimeoutException("before write");
        return super.enqueueIfAbsent(job, key, ttl, suppliedNow);
      }
    };
    try (var trace = new SoakTraceWriter(temporary.resolve("trace.jsonl"))) {
      var job = job();
      assertThat(new RecoveringProducerStore(interrupted, trace, () -> false)
              .enqueueIfAbsent(job, "key", Duration.ofMinutes(1), now))
          .isEqualTo(new EnqueueResult.Created(job.id()));
    }
  }

  @Test
  void deterministicProducerFailureIsNotRetriedAndOutageBudgetStopsRetries() throws Exception {
    var real = new InMemoryJobStore();
    var broken = new ForwardingJobStore(real) {
      @Override
      public void insert(Job job) {
        throw new IllegalArgumentException("invalid job");
      }
    };
    try (var trace = new SoakTraceWriter(temporary.resolve("trace.jsonl"))) {
      assertThatThrownBy(
              () -> new RecoveringProducerStore(broken, trace, () -> false).insert(job()))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("invalid job");
      var unavailable = new ForwardingJobStore(real) {
        @Override
        public void insert(Job job) {
          throw new RedisCommandTimeoutException("outage");
        }
      };
      assertThatThrownBy(
              () -> new RecoveringProducerStore(unavailable, trace, () -> false, Duration.ZERO)
                  .insert(job()))
          .isInstanceOf(RedisCommandTimeoutException.class);
    }
  }

  private static Job job() {
    return Job.builder().spec(new JobSpec("soak.TestHandler", List.of())).build();
  }
}
