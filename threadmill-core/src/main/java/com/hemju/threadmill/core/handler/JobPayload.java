package com.hemju.threadmill.core.handler;

/**
 * Marker interface for the data-transfer-object carried by a job.
 *
 * <p>A {@code JobPayload} must:
 * <ul>
 *   <li>carry only serializable fields (no service / bean references),</li>
 *   <li>be a stable shape that can be round-tripped through the active
 *       {@code JobSerializer}, and</li>
 *   <li>be safe to deserialize on any node — no implicit reliance on
 *       host-side state.</li>
 * </ul>
 *
 * <p>The combination of a {@code JobPayload} and a typed {@link JobHandler}
 * is Threadmill's <em>only</em> job-definition model. There is no lambda or
 * bytecode-capture model — this keeps native-image compatibility cheap and
 * avoids the entire class of bugs where captured references survive the
 * serialization round trip.
 */
public interface JobPayload {}
