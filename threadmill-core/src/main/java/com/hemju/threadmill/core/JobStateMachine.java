package com.hemju.threadmill.core;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * The single source of truth for legal {@link JobState} transitions.
 *
 * <p>Illegal transitions throw {@link IllegalJobTransitionException} — they
 * are <strong>never</strong> silently coerced. The table below is the only
 * place transitions are encoded; every transition in the engine, the
 * store, the housekeeping cycle, and user-facing APIs routes through
 * {@link #requireLegal(JobState, JobState)} or {@link #isLegal(JobState, JobState)}.
 *
 * <h2>Rules</h2>
 * <ul>
 *   <li>{@code AWAITING} → {@code AWAITING, SCHEDULED, ENQUEUED, DELETED, QUARANTINED}</li>
 *   <li>{@code SCHEDULED} → anything except {@code PROCESSING}: {@code AWAITING, SCHEDULED, ENQUEUED, SUCCEEDED, FAILED, DELETED, QUARANTINED}</li>
 *   <li>{@code ENQUEUED} → anything except {@code ENQUEUED}: {@code AWAITING, SCHEDULED, PROCESSING, SUCCEEDED, FAILED, DELETED, QUARANTINED}</li>
 *   <li>{@code PROCESSING} → {@code SUCCEEDED, FAILED, DELETED, QUARANTINED}.
 *       <em>Not</em> directly back to {@code ENQUEUED} — orphan recovery
 *       always flows through {@code FAILED} so the engine's single failure
 *       code path runs for it.</li>
 *   <li>{@code FAILED} → {@code SCHEDULED, ENQUEUED, DELETED, QUARANTINED}</li>
 *   <li>{@code SUCCEEDED} → {@code SCHEDULED, ENQUEUED, DELETED, QUARANTINED}</li>
 *   <li>{@code DELETED} → {@code SCHEDULED, ENQUEUED, QUARANTINED}</li>
 *   <li>{@code QUARANTINED} → terminal — no outgoing transitions in v1.
 *       (Operators reset quarantined jobs by re-creating them.)</li>
 *   <li>{@code PROCESSED} (reserved for external jobs in a later phase)
 *       has no legal transitions yet.</li>
 *   <li>Any state may move to itself only where listed (currently only
 *       {@code AWAITING} can self-loop, while a waiting precondition is
 *       still unresolved).</li>
 * </ul>
 */
public final class JobStateMachine {

    private static final Map<JobState, Set<JobState>> TABLE = buildTable();

    private JobStateMachine() {}

    private static Map<JobState, Set<JobState>> buildTable() {
        var t = new EnumMap<JobState, Set<JobState>>(JobState.class);
        t.put(
                JobState.AWAITING,
                EnumSet.of(
                        JobState.AWAITING,
                        JobState.SCHEDULED,
                        JobState.ENQUEUED,
                        JobState.DELETED,
                        JobState.QUARANTINED));
        t.put(
                JobState.SCHEDULED,
                EnumSet.of(
                        JobState.AWAITING,
                        JobState.SCHEDULED,
                        JobState.ENQUEUED,
                        JobState.SUCCEEDED,
                        JobState.FAILED,
                        JobState.DELETED,
                        JobState.QUARANTINED));
        t.put(
                JobState.ENQUEUED,
                EnumSet.of(
                        JobState.AWAITING,
                        JobState.SCHEDULED,
                        JobState.PROCESSING,
                        JobState.SUCCEEDED,
                        JobState.FAILED,
                        JobState.DELETED,
                        JobState.QUARANTINED));
        t.put(
                JobState.PROCESSING,
                EnumSet.of(JobState.SUCCEEDED, JobState.FAILED, JobState.DELETED, JobState.QUARANTINED));
        t.put(JobState.PROCESSED, EnumSet.noneOf(JobState.class));
        t.put(
                JobState.SUCCEEDED,
                EnumSet.of(JobState.SCHEDULED, JobState.ENQUEUED, JobState.DELETED, JobState.QUARANTINED));
        t.put(
                JobState.FAILED,
                EnumSet.of(JobState.SCHEDULED, JobState.ENQUEUED, JobState.DELETED, JobState.QUARANTINED));
        t.put(JobState.DELETED, EnumSet.of(JobState.SCHEDULED, JobState.ENQUEUED, JobState.QUARANTINED));
        t.put(JobState.QUARANTINED, EnumSet.noneOf(JobState.class));
        return Collections.unmodifiableMap(t);
    }

    public static boolean isLegal(JobState from, JobState to) {
        return TABLE.get(from).contains(to);
    }

    public static void requireLegal(JobState from, JobState to) {
        if (!isLegal(from, to)) {
            throw new IllegalJobTransitionException(from, to);
        }
    }

    public static Set<JobState> legalSuccessorsOf(JobState from) {
        return Collections.unmodifiableSet(TABLE.get(from));
    }
}
