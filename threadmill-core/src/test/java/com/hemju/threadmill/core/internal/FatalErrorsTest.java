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
  void ordinaryErrorsAreNotClassifiedAsProcessFatal() {
    assertThatCode(() -> FatalErrors.rethrowIfFatal(new AssertionError("job assertion")))
        .doesNotThrowAnyException();
    assertThatCode(() -> FatalErrors.rethrowIfFatal(new LinkageError("handler linkage")))
        .doesNotThrowAnyException();
  }

  private static final class TestVirtualMachineError extends VirtualMachineError {
    private static final long serialVersionUID = 1L;
  }
}
