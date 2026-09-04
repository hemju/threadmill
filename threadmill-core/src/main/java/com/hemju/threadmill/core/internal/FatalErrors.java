package com.hemju.threadmill.core.internal;

/** Internal classification for JVM errors that Threadmill must never contain. */
public final class FatalErrors {

  private static final int MAX_CAUSE_DEPTH = 64;

  private FatalErrors() {}

  /**
   * Rethrow a process-fatal JVM error found in {@code failure} or its cause chain.
   *
   * <p>The bounded walk avoids allocating while the VM may already be under resource pressure and
   * cannot loop forever on a malformed cyclic cause chain.
   */
  @SuppressWarnings("removal")
  public static void rethrowIfFatal(Throwable failure) {
    Throwable current = failure;
    for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
      if (current instanceof VirtualMachineError fatal) {
        throw fatal;
      }
      if (current instanceof ThreadDeath fatal) {
        throw fatal;
      }
      Throwable cause = current.getCause();
      if (cause == current) {
        return;
      }
      current = cause;
    }
  }
}
