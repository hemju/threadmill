package com.hemju.threadmill.core;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.hemju.threadmill.core.spec.JobSpec;

/**
 * Threadmill's job aggregate.
 *
 * <h2>Design invariants</h2>
 * <ul>
 *   <li><strong>The optimistic-lock version is persisted state.</strong> The
 *       {@code version} field on this in-memory object is the version the
 *       store last confirmed; it is updated <em>only</em> after a successful
 *       save via {@link #adoptVersion(long)}. If a save fails, the
 *       {@code Job} remains exactly reusable — no desync between memory and
 *       storage is possible.</li>
 *   <li><strong>State is an append-only history.</strong> There is no
 *       single mutable {@code state} field. The current state is the last
 *       element of the history. This gives a free audit trail and makes
 *       per-state latency metrics computable directly from the model.</li>
 *   <li><strong>The object handed to user code is not what is serialized.</strong>
 *       The engine takes a {@link JobSnapshot} via {@link #snapshot()} before
 *       serialization; concurrent mutation of user-touchable areas during the
 *       serialization itself is therefore impossible by construction.</li>
 *   <li><strong>Relationships and a result slot are part of the model.</strong>
 *       Workflows, batches, and external-trigger jobs are built on the
 *       relationship and result fields that the core job already carries.</li>
 * </ul>
 *
 * <p>Use {@link #builder()} to create a job.
 */
public final class Job {

    private final JobId id;
    private final JobSpec spec;
    private final Instant createdAt;
    private final String queue;
    private final int priority;
    private final String cronTaskName;
    private final JobRelationship relationship;
    private final JobId workflowRootId;
    private final String concurrencyKey;
    private final ConcurrencyMode concurrencyMode;

    private final List<JobStateEntry> stateHistory;
    private final JobMetadata metadata;
    private final JobLog log;
    private final JobProgress progress;

    // Mutable, but always under the lock 'this'. Owner/heartbeat fields
    // belong to the persisted state and are adopted from the store only.
    private long version;
    private NodeId ownerNodeId;
    private Instant ownerHeartbeatAt;
    private Instant lastCheckinAt;
    private Instant scheduledFor;
    private JobResult result;
    private int attempts;

    private Job(Builder b) {
        this.id = Objects.requireNonNull(b.id, "id");
        this.spec = Objects.requireNonNull(b.spec, "spec");
        this.createdAt = Objects.requireNonNull(b.createdAt, "createdAt");
        this.queue = b.queue == null ? "default" : Names.requireName("queue", b.queue);
        this.priority = b.priority;
        this.cronTaskName = b.cronTaskName;
        this.relationship = b.relationship;
        this.workflowRootId = b.workflowRootId == null ? this.id : b.workflowRootId;
        this.concurrencyKey = validateConcurrencyKey(b.concurrencyKey);
        if (b.concurrencyKey == null && b.concurrencyMode != null) {
            // A mode without a key would silently provide no concurrency
            // control at all — exactly the API misuse worth failing loudly.
            throw new IllegalArgumentException("concurrencyMode requires a concurrencyKey");
        }
        this.concurrencyMode =
                concurrencyKey == null ? null : Objects.requireNonNull(b.concurrencyMode, "concurrencyMode");
        this.metadata = new JobMetadata(b.metadata.snapshot());
        this.log = new JobLog();
        this.progress = new JobProgress();
        this.stateHistory = new ArrayList<>();
        if (b.initialStateHistory.isEmpty()) {
            this.stateHistory.add(JobStateEntry.of(b.initialState, b.createdAt));
        } else {
            this.stateHistory.addAll(b.initialStateHistory);
        }
        this.version = b.version;
        this.scheduledFor = b.scheduledFor;
        this.attempts = b.attempts;
    }

    // ---------------------------------------------------------------- identity & metadata

    public JobId id() {
        return id;
    }

    public JobSpec spec() {
        return spec;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public String queue() {
        return queue;
    }

    public int priority() {
        return priority;
    }

    public Optional<String> cronTaskName() {
        return Optional.ofNullable(cronTaskName);
    }

    public Optional<JobRelationship> relationship() {
        return Optional.ofNullable(relationship);
    }

    public JobId workflowRootId() {
        return workflowRootId;
    }

    public Optional<String> concurrencyKey() {
        return Optional.ofNullable(concurrencyKey);
    }

    public Optional<ConcurrencyMode> concurrencyMode() {
        return Optional.ofNullable(concurrencyMode);
    }

    public JobMetadata metadata() {
        return metadata;
    }

    public JobLog log() {
        return log;
    }

    public JobProgress progress() {
        return progress;
    }

    // ---------------------------------------------------------------- state & version

    public synchronized JobState currentState() {
        return stateHistory.get(stateHistory.size() - 1).state();
    }

    public synchronized List<JobStateEntry> stateHistory() {
        return Collections.unmodifiableList(new ArrayList<>(stateHistory));
    }

    public synchronized long version() {
        return version;
    }

    public synchronized Optional<NodeId> ownerNodeId() {
        return Optional.ofNullable(ownerNodeId);
    }

    public synchronized Optional<Instant> ownerHeartbeatAt() {
        return Optional.ofNullable(ownerHeartbeatAt);
    }

    public synchronized Optional<Instant> lastCheckinAt() {
        return Optional.ofNullable(lastCheckinAt);
    }

    public synchronized Optional<Instant> scheduledFor() {
        return Optional.ofNullable(scheduledFor);
    }

    public synchronized Optional<JobResult> result() {
        return Optional.ofNullable(result);
    }

    public synchronized int attempts() {
        return attempts;
    }

    /**
     * Move the job to a new state. Routes through {@link JobStateMachine} so
     * the transition table is the single source of truth; throws
     * {@link IllegalJobTransitionException} for an illegal transition.
     */
    public synchronized void transitionTo(JobState next, Instant at, String reason, String message) {
        JobState current = currentState();
        JobStateMachine.requireLegal(current, next);
        stateHistory.add(new JobStateEntry(next, at, reason, message));
    }

    public synchronized void transitionTo(JobState next, Instant at) {
        transitionTo(next, at, null, null);
    }

    /**
     * Assign an owner and a fresh heartbeat. Intended for the store's claim
     * code path, called as part of a {@code PROCESSING} transition.
     */
    public synchronized void assignOwner(NodeId nodeId, Instant heartbeatAt) {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(heartbeatAt, "heartbeatAt");
        this.ownerNodeId = nodeId;
        this.ownerHeartbeatAt = heartbeatAt;
        this.lastCheckinAt = null;
    }

    public synchronized void clearOwner() {
        this.ownerNodeId = null;
        this.ownerHeartbeatAt = null;
    }

    public synchronized void updateHeartbeat(Instant at) {
        Objects.requireNonNull(at, "at");
        this.ownerHeartbeatAt = at;
    }

    public synchronized void checkIn(Instant at) {
        Objects.requireNonNull(at, "at");
        this.lastCheckinAt = at;
        if (ownerHeartbeatAt == null || ownerHeartbeatAt.isBefore(at)) {
            this.ownerHeartbeatAt = at;
        }
    }

    /**
     * Restore the last check-in time from persisted state. Intended for
     * serializer code only: unlike {@link #checkIn(Instant)} it never touches
     * the owner heartbeat, so deserializing an ownerless job (for example a
     * FAILED job that checked in during its last attempt) cannot fabricate
     * owner activity the wire form never contained.
     */
    public synchronized void restoreCheckIn(Instant at) {
        Objects.requireNonNull(at, "at");
        this.lastCheckinAt = at;
    }

    public synchronized void scheduleAt(Instant at) {
        Objects.requireNonNull(at, "at");
        this.scheduledFor = at;
    }

    public synchronized void clearScheduledFor() {
        this.scheduledFor = null;
    }

    public synchronized void setResult(JobResult result) {
        this.result = result;
    }

    public synchronized void incrementAttempts() {
        this.attempts++;
    }

    /**
     * Undo one claim-time attempt increment. Engine use only: when a handler
     * is interrupted because its node is shutting down, the attempt did not
     * fail on its own merits and must not consume retry budget.
     */
    public synchronized void revertAttempt() {
        this.attempts = Math.max(0, this.attempts - 1);
    }

    /**
     * Adopt a new persisted version after a successful save. The store calls
     * this <em>after</em> the write commits; never before. A failed save
     * leaves the in-memory version untouched and the job reusable.
     */
    public synchronized void adoptVersion(long newVersion) {
        if (newVersion < this.version) {
            throw new IllegalArgumentException(
                    "adoptVersion must not move version backwards: " + this.version + " -> " + newVersion);
        }
        this.version = newVersion;
    }

    // ---------------------------------------------------------------- engine snapshot

    /**
     * Returns an immutable point-in-time snapshot of the job — the value the
     * engine serializes. Taking the snapshot under the job's monitor prevents
     * concurrent mutation from producing a torn state.
     */
    public synchronized JobSnapshot snapshot() {
        return new JobSnapshot(
                id,
                spec,
                queue,
                priority,
                createdAt,
                cronTaskName,
                relationship,
                workflowRootId,
                concurrencyKey,
                concurrencyMode,
                List.copyOf(stateHistory),
                metadata.snapshot(),
                log.snapshot(),
                progress.snapshot().orElse(null),
                version,
                ownerNodeId,
                ownerHeartbeatAt,
                lastCheckinAt,
                scheduledFor,
                result,
                attempts);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for a newly-created job. */
    public static final class Builder {
        private JobId id = JobId.newId();
        private JobSpec spec;
        private Instant createdAt;
        private String queue;
        private int priority;
        private String cronTaskName;
        private JobRelationship relationship;
        private JobId workflowRootId;
        private String concurrencyKey;
        private ConcurrencyMode concurrencyMode;
        private JobState initialState = JobState.ENQUEUED;
        private final JobMetadata metadata = new JobMetadata();
        private long version = 0L;
        private Instant scheduledFor;
        private int attempts = 0;
        private final List<JobStateEntry> initialStateHistory = new ArrayList<>();
        private Clock clock = Clock.systemUTC();

        private Builder() {}

        public Builder id(JobId id) {
            this.id = Objects.requireNonNull(id, "id");
            return this;
        }

        public Builder spec(JobSpec spec) {
            this.spec = spec;
            return this;
        }

        public Builder queue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        public Builder cronTaskName(String cronTaskName) {
            this.cronTaskName = cronTaskName;
            return this;
        }

        public Builder relationship(JobRelationship relationship) {
            this.relationship = relationship;
            return this;
        }

        public Builder workflowRootId(JobId workflowRootId) {
            this.workflowRootId = workflowRootId;
            return this;
        }

        public Builder concurrencyKey(String concurrencyKey) {
            this.concurrencyKey = concurrencyKey;
            return this;
        }

        public Builder concurrencyMode(ConcurrencyMode concurrencyMode) {
            this.concurrencyMode = concurrencyMode;
            return this;
        }

        public Builder initialState(JobState state) {
            this.initialState = state;
            return this;
        }

        public Builder scheduledFor(Instant at) {
            this.scheduledFor = at;
            return this;
        }

        public Builder metadata(String key, String value) {
            this.metadata.put(key, value);
            return this;
        }

        public Builder version(long version) {
            this.version = version;
            return this;
        }

        public Builder attempts(int attempts) {
            this.attempts = attempts;
            return this;
        }

        public Builder createdAt(Instant at) {
            this.createdAt = at;
            return this;
        }

        public Builder clock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder withStateHistory(List<JobStateEntry> history) {
            this.initialStateHistory.clear();
            this.initialStateHistory.addAll(history);
            return this;
        }

        public Job build() {
            if (createdAt == null) {
                createdAt = clock.instant();
            }
            return new Job(this);
        }
    }

    private static String validateConcurrencyKey(String key) {
        if (key == null) {
            return null;
        }
        if (key.isBlank()) {
            throw new IllegalArgumentException("concurrencyKey must not be blank");
        }
        int bytes = key.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > 256) {
            throw new IllegalArgumentException("concurrencyKey must be at most 256 UTF-8 bytes");
        }
        return key;
    }
}
