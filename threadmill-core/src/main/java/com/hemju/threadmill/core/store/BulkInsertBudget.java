package com.hemju.threadmill.core.store;

import java.nio.charset.StandardCharsets;

/** Internal preflight budget shared by atomic bulk-insert implementations. */
public final class BulkInsertBudget {
  private final long maxBytes;
  private long bytes;

  /** Reject an excessive job count before serialization or datastore work. */
  public BulkInsertBudget(int jobs, JobStoreCapabilities capabilities) {
    if (jobs > capabilities.maxBulkInsertJobs()) {
      throw new IllegalArgumentException(
          "Atomic bulk insert exceeds " + capabilities.maxBulkInsertJobs()
              + " jobs; split the submission into smaller atomic batches");
    }
    maxBytes = capabilities.maxBulkInsertBytes();
  }

  /** Include one encoded body, rejecting the whole batch if its byte budget is exceeded. */
  public void include(String body) {
    bytes += body.getBytes(StandardCharsets.UTF_8).length;
    if (bytes > maxBytes) {
      throw new IllegalArgumentException("Atomic bulk insert exceeds " + maxBytes
          + " serialized bytes; split the submission into smaller atomic batches");
    }
  }
}
