package com.hemju.threadmill.core.serialization;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobSnapshot;
import com.hemju.threadmill.core.OversizedJobException;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.store.JobStoreCapabilities;

/**
 * SPI for converting jobs, payloads, and arguments to and from a wire form.
 *
 * <p>The default implementation is JSON ({@link JsonJobSerializer}). JSON is
 * preferred because the persisted form is human-readable in the store and
 * in any console — and because there are no Java-serialization footguns.
 *
 * <p>Hosts may swap in their own implementation backed by the same JSON
 * library their application already uses; this avoids dragging in a second
 * mapper.
 */
public interface JobSerializer {

    /**
     * Serialize a job snapshot. The snapshot is immutable, so the result is
     * free from torn-write hazards by construction.
     *
     * <p>This signature performs no truncation — the byte budget is the only
     * gate. Use {@link #serializeJob(JobSnapshot, JobStoreCapabilities)} for
     * the engine-side path that needs failure-detail / log truncation so a
     * {@code PROCESSING → FAILED} transition is never blocked by an
     * oversized exception trace.
     *
     * @param snapshot       the snapshot to serialize
     * @param maxBytes       upper bound on the serialized size; on overflow,
     *                       the implementation throws {@link OversizedJobException}
     *                       and does <em>not</em> mutate caller state
     */
    String serializeJob(JobSnapshot snapshot, long maxBytes);

    /**
     * Serialize a job snapshot with the truncation policy from
     * {@code capabilities}. The serializer:
     * <ul>
     *   <li>Trims {@code JobLog} entries from the head (oldest first) until
     *       the log fits {@link JobStoreCapabilities#maxJobLogBytes()}.</li>
     *   <li>Caps the {@code message} field of {@code FAILED} and
     *       {@code QUARANTINED} state-history entries at
     *       {@link JobStoreCapabilities#maxFailureMetadataBytes()} bytes,
     *       preserving the leading content and appending a truncation
     *       sentinel.</li>
     *   <li>Then enforces the overall
     *       {@link JobStoreCapabilities#maxSerializedJobBytes()} cap; if the
     *       truncated body still exceeds the cap (e.g. a metadata explosion),
     *       throws {@link OversizedJobException} without mutating caller state.</li>
     * </ul>
     *
     * <p>This is the path stores should use. It ensures the single failure
     * code path (see AGENTS.md §6) cannot be blocked by an oversized stack
     * trace: truncation lives in the serializer, so {@code RetryInterceptor}
     * and {@code JobRunner.recordFailure} do not need to know about it.
     */
    String serializeJob(JobSnapshot snapshot, JobStoreCapabilities capabilities);

    /** Deserialize a job from its wire form. */
    Job deserializeJob(String wire);

    /** Serialize a typed argument or payload to its on-disk representation. */
    JobArgument serializeArgument(Object value);

    /** Deserialize a typed argument to a concrete instance. */
    Object deserializeArgument(JobArgument argument);

    /** Serialize a {@link JobPayload} (a {@code JobArgument} shaped for a payload). */
    JobArgument serializePayload(JobPayload payload);

    /** Deserialize a {@code JobPayload} from its on-disk representation. */
    <P extends JobPayload> P deserializePayload(JobArgument argument, Class<P> type);
}
