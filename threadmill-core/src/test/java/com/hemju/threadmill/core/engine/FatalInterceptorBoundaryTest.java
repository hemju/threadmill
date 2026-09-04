package com.hemju.threadmill.core.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.spec.JobSpec;

class FatalInterceptorBoundaryTest {

  @Test
  void processFatalErrorsEscapeInterceptorIsolation() {
    for (Error fatal : fatalErrors()) {
      var interceptors = new JobInterceptors().add(new JobInterceptor() {
        @Override
        public void onStateChange(Job job, JobState from, JobState to) {
          throw fatal;
        }
      });

      assertThatThrownBy(
              () -> interceptors.onStateChange(job(), JobState.ENQUEUED, JobState.FAILED))
          .isSameAs(fatal);
    }
  }

  @Test
  void ordinaryAssertionErrorIsolatedToOneInterceptor() {
    var laterInterceptorRan = new AtomicBoolean();
    var interceptors = new JobInterceptors()
        .add(new JobInterceptor() {
          @Override
          public void onStateChange(Job job, JobState from, JobState to) {
            throw new AssertionError("bad metric assertion");
          }
        })
        .add(new JobInterceptor() {
          @Override
          public void onStateChange(Job job, JobState from, JobState to) {
            laterInterceptorRan.set(true);
          }
        });

    assertThatCode(() -> interceptors.onStateChange(job(), JobState.ENQUEUED, JobState.FAILED))
        .doesNotThrowAnyException();
    assertThat(laterInterceptorRan).isTrue();
  }

  @Test
  void processFatalErrorsEscapeWakeSinkIsolation() {
    for (Error fatal : fatalErrors()) {
      var wakeBus = new LocalWakeBus();
      wakeBus.register(queue -> {
        throw fatal;
      });

      assertThatThrownBy(() -> wakeBus.wake("default")).isSameAs(fatal);
    }
  }

  private static Job job() {
    return Job.builder().spec(new JobSpec("example.Handler", List.of())).build();
  }

  @SuppressWarnings("removal")
  private static Error[] fatalErrors() {
    return new Error[] {new TestVirtualMachineError(), new ThreadDeath()};
  }

  private static final class TestVirtualMachineError extends VirtualMachineError {
    private static final long serialVersionUID = 1L;
  }
}
