package com.hemju.threadmill.core.handler;

/**
 * The canonical zero-argument {@link JobPayload}.
 *
 * <p>Use this for handlers whose work does not depend on per-invocation
 * arguments — for example, periodic housekeeping tasks, heartbeats, or
 * "sweep" jobs. Declaring a handler as {@code JobHandler<NoPayload>} avoids
 * inventing an empty record per job. Serialisation round-trips to {@code {}}.
 *
 * <p>Always use {@link #INSTANCE} rather than constructing new instances —
 * the record is empty, so identity does not carry information.
 */
public record NoPayload() implements JobPayload {

    public static final NoPayload INSTANCE = new NoPayload();
}
