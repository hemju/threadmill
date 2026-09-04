package com.hemju.threadmill.core.internal;

/**
 * Internal classification for JVM errors that Threadmill must never contain.
 *
 * <p><strong>Engine-internal.</strong> This class is {@code public} only for cross-module engine
 * wiring; it is NOT part of Threadmill's supported public API. Its methods and behavior may change
 * in any release without notice.
 */
public final class FatalErrors {

  private static final int MAX_CAUSE_DEPTH = 64;

  private FatalErrors() {}

  /**
   * Rethrow a process-fatal JVM error found in {@code failure} or its simple cause chain.
   *
   * <p>The walk is bounded so a malformed cyclic cause chain terminates without an allocating
   * visited set while the VM may already be under resource pressure. Suppressed exceptions are not
   * traversed: the engine contract covers causal wrappers, and reading suppressed exceptions would
   * broaden this into an allocating graph walk on the failure path.
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
