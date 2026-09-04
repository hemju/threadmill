package com.hemju.threadmill.core.handler;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobLog;
import com.hemju.threadmill.core.JobMetadata;
import com.hemju.threadmill.core.JobProgress;
import com.hemju.threadmill.core.NodeId;

/**
 * Pins the handler-facing behavior of {@link JobExecutionContext}: this test
 * context is unbounded and uncancelled, and
 * {@link JobExecutionContext#current()} resolves the scoped binding.
 */
class JobExecutionContextTest {

  @Test
  void stubDeadlineIsUnboundedAndRemainingDoesNotOverflow() {
    JobExecutionContext ctx = stub();

    assertThat(ctx.deadline()).isEqualTo(Instant.MAX);
    assertThat(ctx.remaining()).isPositive();
    assertThat(ctx.cancellation()).isEmpty();
    assertThat(ctx.isCancelled()).isFalse();
  }

  @Test
  void remainingClampsToZeroOncePastTheDeadline() {
    Instant past = Instant.now().minus(Duration.ofMinutes(1));
    JobExecutionContext ctx = new StubContext() {
      @Override
      public Instant deadline() {
        return past;
      }
    };

    assertThat(ctx.remaining()).isEqualTo(Duration.ZERO);
  }

  @Test
  void currentIsEmptyOutsideAJobAndBoundInsideTheScope() {
    assertThat(JobExecutionContext.current()).isEmpty();

    JobExecutionContext ctx = stub();
    var seen = new AtomicReference<JobExecutionContext>();
    ScopedValue.where(JobExecutionContexts.CURRENT, ctx).run(() -> seen.set(nestedLookup()));

    assertThat(seen.get()).isSameAs(ctx);
    assertThat(JobExecutionContext.current()).isEmpty();
  }

  /** Stands in for the service layer two calls below the handler. */
  private static JobExecutionContext nestedLookup() {
    return JobExecutionContext.current().orElseThrow();
  }

  private static JobExecutionContext stub() {
    return new StubContext();
  }

  private static class StubContext implements JobExecutionContext {
    @Override
    public JobId jobId() {
      return JobId.newId();
    }

    @Override
    public NodeId nodeId() {
      return NodeId.newId();
    }

    @Override
    public int attempt() {
      return 1;
    }

    @Override
    public Instant claimedAt() {
      return Instant.now();
    }

    @Override
    public Instant deadline() {
      return Instant.MAX;
    }

    @Override
    public Optional<CancellationReason> cancellation() {
      return Optional.empty();
    }

    @Override
    public void checkIn() {}

    @Override
    public void setResult(Object value) {}

    @Override
    public Optional<Object> readResult() {
      return Optional.empty();
    }

    @Override
    public JobLog log() {
      return null;
    }

    @Override
    public JobProgress progress() {
      return null;
    }

    @Override
    public JobMetadata metadata() {
      return null;
    }
  }
}
