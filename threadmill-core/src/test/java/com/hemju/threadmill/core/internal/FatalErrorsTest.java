package com.hemju.threadmill.core.internal;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class FatalErrorsTest {

  @Test
  @SuppressWarnings("removal")
  void virtualMachineErrorAndThreadDeathAreRethrown() {
    for (Error fatal : new Error[] {new TestVirtualMachineError(), new ThreadDeath()}) {
      assertThatThrownBy(() -> FatalErrors.rethrowIfFatal(fatal)).isSameAs(fatal);
    }
  }

  @Test
  void fatalErrorsAreFoundThroughWrapperCauseChains() {
    var fatal = new TestVirtualMachineError();
    var wrapper = new IllegalStateException("outer", new Exception("inner", fatal));

    assertThatThrownBy(() -> FatalErrors.rethrowIfFatal(wrapper)).isSameAs(fatal);
  }

  @Test
  void fatalAtTheLastInspectedCauseDepthIsRethrown() {
    var fatal = new TestVirtualMachineError();
    Throwable wrapper = fatal;
    for (int depth = 0; depth < 63; depth++) {
      wrapper = new IllegalStateException("wrapper-" + depth, wrapper);
    }
    Throwable failure = wrapper;

    assertThatThrownBy(() -> FatalErrors.rethrowIfFatal(failure)).isSameAs(fatal);
  }

  @Test
  void fatalBeyondTheCauseDepthBoundIsNotInspected() {
    Throwable wrapper = new TestVirtualMachineError();
    for (int depth = 0; depth < 64; depth++) {
      wrapper = new IllegalStateException("wrapper-" + depth, wrapper);
    }
    Throwable failure = wrapper;

    assertThatCode(() -> FatalErrors.rethrowIfFatal(failure)).doesNotThrowAnyException();
  }

  @Test
  void malformedCyclicCauseChainsTerminateWithoutAllocationTracking() {
    var selfCycle = new CyclicFailure();
    selfCycle.pointsTo(selfCycle);
    assertThatCode(() -> FatalErrors.rethrowIfFatal(selfCycle)).doesNotThrowAnyException();

    var first = new CyclicFailure();
    var second = new CyclicFailure();
    first.pointsTo(second);
    second.pointsTo(first);
    assertThatCode(() -> FatalErrors.rethrowIfFatal(first)).doesNotThrowAnyException();
  }

  @Test
  void ordinaryErrorsAreNotClassifiedAsProcessFatal() {
    assertThatCode(() -> FatalErrors.rethrowIfFatal(new AssertionError("job assertion")))
        .doesNotThrowAnyException();
    assertThatCode(() -> FatalErrors.rethrowIfFatal(new LinkageError("handler linkage")))
        .doesNotThrowAnyException();
  }

  private static final class TestVirtualMachineError extends VirtualMachineError {
    private static final long serialVersionUID = 1L;
  }

  private static final class CyclicFailure extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private Throwable next;

    void pointsTo(Throwable next) {
      this.next = next;
    }

    @Override
    public synchronized Throwable getCause() {
      return next;
    }
  }
}
