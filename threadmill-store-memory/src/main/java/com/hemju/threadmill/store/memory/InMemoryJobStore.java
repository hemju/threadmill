package com.hemju.threadmill.store.memory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.EnqueueResult;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobReplacement;
import com.hemju.threadmill.core.JobReplacements;
import com.hemju.threadmill.core.JobSnapshot;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.JobStateEntry;
import com.hemju.threadmill.core.Names;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.core.store.JobStoreCapabilities;
import com.hemju.threadmill.core.store.NodeHeartbeat;

/**
 * Concurrency-safe in-memory {@link JobStore}.
 *
 * <p>Used for tests and for local development. <strong>Not a simplified
 * fake</strong> — it is held to the same contract test as the real stores
 * and must tell the truth, because later phases' tests depend on it.
 *
 * <p>The store keeps the persisted form as the serializer's wire string,
 * not as the in-memory {@code Job} object. This mirrors what the real
 * stores do, so the round-trip exercises the serializer for every test.
 */
public final class InMemoryJobStore implements JobStore {

    private static final class Entry {
        final String wire;
        final long version;
        final JobState state;
        final String queue;
        final int priority;
        final Instant scheduledFor;
        final Instant ownerHeartbeatAt;
        final Instant lastCheckinAt;
        final Instant currentStateAt;
        final String handlerType;
        final JobId workflowRootId;
        final String concurrencyKey;
        final ConcurrencyMode concurrencyMode;

        Entry(
                String wire,
                long version,
                JobState state,
                String queue,
                int priority,
                Instant scheduledFor,
                Instant ownerHeartbeatAt,
                Instant lastCheckinAt,
                Instant currentStateAt,
                String handlerType,
                JobId workflowRootId,
                String concurrencyKey,
                ConcurrencyMode concurrencyMode) {
            this.wire = wire;
            this.version = version;
            this.state = state;
            this.queue = queue;
            this.priority = priority;
            this.scheduledFor = scheduledFor;
            this.ownerHeartbeatAt = ownerHeartbeatAt;
            this.lastCheckinAt = lastCheckinAt;
            this.currentStateAt = currentStateAt;
            this.handlerType = handlerType;
            this.workflowRootId = workflowRootId;
            this.concurrencyKey = concurrencyKey;
            this.concurrencyMode = concurrencyMode;
        }
    }

    private static Comparator<Map.Entry<JobId, Entry>> byPriorityDescThenId() {
        return Comparator.<Map.Entry<JobId, Entry>>comparingInt(e -> -e.getValue().priority)
                .thenComparing(e -> e.getKey().asUuid());
    }

    private final ConcurrentHashMap<JobId, Entry> jobs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<NodeId, Instant> nodeHeartbeats = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CronTask> cronTasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CronTaskScheduleState> cronTaskStates = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<DedupKey, DedupRecord> dedupKeys = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, QueuePause> queuePauses = new ConcurrentHashMap<>();
    private final Object claimMutex = new Object();
    private final Object leaseMutex = new Object();
    private MaintenanceLease maintenanceLease;
    private final JobSerializer serializer;
    private final JobStoreCapabilities capabilities;

    private record MaintenanceLease(NodeId holder, Instant expiresAt) {}

    private record DedupKey(String queue, String key) {}

    private record DedupRecord(JobId jobId, Instant expiresAt) {}

    private record QueuePause(Instant pausedAt, String reason) {}

    public InMemoryJobStore() {
        this(new JsonJobSerializer(), JobStoreCapabilities.defaults());
    }

    public InMemoryJobStore(JobSerializer serializer, JobStoreCapabilities capabilities) {
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.capabilities = Objects.requireNonNull(capabilities, "capabilities");
    }

    // ---------------------------------------------------------------- capabilities

    @Override
    public JobStoreCapabilities capabilities() {
        return capabilities;
    }

    // ---------------------------------------------------------------- single-job

    @Override
    public void insert(Job job) {
        Objects.requireNonNull(job, "job");
        Names.requireName("queue", job.queue());
        long nextVersion = 1L;
        JobSnapshot snapshot = snapshotForInsert(job, nextVersion);
        String wire = serializer.serializeJob(snapshot, capabilities);
        Entry entry = entryFromSnapshot(snapshot, wire, nextVersion);

        Entry prior = jobs.putIfAbsent(job.id(), entry);
        if (prior != null) {
            throw new IllegalStateException("Job already exists: " + job.id());
        }
        job.adoptVersion(nextVersion);
    }

    @Override
    public List<JobId> insertAll(List<Job> jobsToInsert) {
        Objects.requireNonNull(jobsToInsert, "jobs");
        if (jobsToInsert.isEmpty()) return List.of();

        // Phase 1 — serialize every job first so a single OversizedJobException
        // rejects the whole batch before any state changes.
        var prepared = new ArrayList<PreparedInsert>(jobsToInsert.size());
        for (var j : jobsToInsert) {
            Objects.requireNonNull(j, "job");
            Names.requireName("queue", j.queue());
            JobSnapshot snap = snapshotForInsert(j, 1L);
            String wire = serializer.serializeJob(snap, capabilities);
            prepared.add(new PreparedInsert(j, snap, wire));
        }

        // Phase 2 — atomic insert under the claim mutex. Either all entries land
        // or (on duplicate-id detection) all earlier insertions are rolled back.
        var insertedIds = new ArrayList<JobId>(prepared.size());
        synchronized (claimMutex) {
            for (var p : prepared) {
                Entry entry = entryFromSnapshot(p.snapshot, p.wire, 1L);
                Entry prior = jobs.putIfAbsent(p.job.id(), entry);
                if (prior != null) {
                    for (JobId id : insertedIds) {
                        jobs.remove(id);
                    }
                    throw new IllegalStateException("Job already exists: " + p.job.id());
                }
                insertedIds.add(p.job.id());
            }
        }
        // Adopt versions only after the all-or-nothing write completes.
        for (var p : prepared) {
            p.job.adoptVersion(1L);
        }
        return List.copyOf(insertedIds);
    }

    private record PreparedInsert(Job job, JobSnapshot snapshot, String wire) {}

    @Override
    public EnqueueResult enqueueIfAbsent(Job job, String dedupKey, Duration ttl, Instant now) {
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(ttl, "ttl");
        Objects.requireNonNull(now, "now");
        Names.requireName("queue", job.queue());
        if (dedupKey == null || dedupKey.isBlank()) {
            throw new IllegalArgumentException("dedupKey must not be blank");
        }
        var key = new DedupKey(job.queue(), dedupKey);
        synchronized (claimMutex) {
            DedupRecord existing = dedupKeys.get(key);
            if (existing != null) {
                Entry existingJob = jobs.get(existing.jobId());
                if (existingJob != null && (existing.expiresAt().isAfter(now) || !isTerminal(existingJob.state))) {
                    return new EnqueueResult.Coalesced(existing.jobId());
                }
                dedupKeys.remove(key);
            }
            insert(job);
            dedupKeys.put(key, new DedupRecord(job.id(), now.plus(ttl)));
            return new EnqueueResult.Created(job.id());
        }
    }

    @Override
    public Optional<Job> findById(JobId id) {
        Entry e = jobs.get(id);
        return e == null ? Optional.empty() : Optional.of(serializer.deserializeJob(e.wire));
    }

    @Override
    public void saveAtomic(Job job, long expectedVersion) {
        Objects.requireNonNull(job, "job");
        long nextVersion = expectedVersion + 1;
        JobSnapshot snapshot = withVersion(job, nextVersion);
        String wire = serializer.serializeJob(snapshot, capabilities);
        // serializeJob throws OversizedJobException before we touch the store; the in-memory
        // version is unchanged. Only after a successful write do we call adoptVersion().
        Entry newEntry = entryFromSnapshot(snapshot, wire, nextVersion);

        var failure = new AtomicReference<RuntimeException>();
        jobs.compute(job.id(), (k, existing) -> {
            if (existing == null) {
                failure.set(new StaleJobException(job.id(), expectedVersion));
                return null;
            }
            if (existing.version != expectedVersion) {
                failure.set(new StaleJobException(job.id(), expectedVersion));
                return existing;
            }
            return newEntry;
        });
        RuntimeException err = failure.get();
        if (err != null) {
            throw err;
        }
        job.adoptVersion(nextVersion);
    }

    @Override
    public boolean softDelete(JobId id) {
        var changed = new AtomicReference<Boolean>(false);
        jobs.compute(id, (k, existing) -> {
            if (existing == null) {
                return null;
            }
            Job j = serializer.deserializeJob(existing.wire);
            if (j.currentState() == JobState.DELETED) {
                changed.set(false);
                return existing;
            }
            j.transitionTo(JobState.DELETED, Instant.now(), "user.delete", null);
            long nextVersion = existing.version + 1;
            JobSnapshot snap = withVersion(j, nextVersion);
            String wire = serializer.serializeJob(snap, capabilities);
            changed.set(true);
            return entryFromSnapshot(snap, wire, nextVersion);
        });
        return Boolean.TRUE.equals(changed.get());
    }

    // ---------------------------------------------------------------- claim & heartbeat

    @Override
    public List<Job> claimReady(NodeId nodeId, String queue, int max, Instant heartbeatAt) {
        Objects.requireNonNull(nodeId, "nodeId");
        Names.requireName("queue", queue);
        Objects.requireNonNull(heartbeatAt, "heartbeatAt");
        if (max <= 0) return List.of();
        if (queuePauses.containsKey(queue)) return List.of();
        int cap = Math.min(max, capabilities.maxClaimBatch());

        synchronized (claimMutex) {
            // Within a queue, higher priority wins; ties break by id (creation time).
            List<Map.Entry<JobId, Entry>> candidates = jobs.entrySet().stream()
                    .filter(e -> e.getValue().state == JobState.ENQUEUED && queue.equals(e.getValue().queue))
                    .sorted(byPriorityDescThenId())
                    .collect(Collectors.toList());

            List<Job> result = new ArrayList<>(Math.min(cap, candidates.size()));
            for (var ce : candidates) {
                if (result.size() >= cap) break;
                Entry existing = ce.getValue();
                if (!canClaim(existing)) continue;
                Job j = serializer.deserializeJob(existing.wire);
                j.transitionTo(JobState.PROCESSING, heartbeatAt, "engine.claim", null);
                j.assignOwner(nodeId, heartbeatAt);
                j.incrementAttempts();
                long nextVersion = existing.version + 1;
                JobSnapshot snap = withVersion(j, nextVersion);
                String wire = serializer.serializeJob(snap, capabilities);
                Entry updated = entryFromSnapshot(snap, wire, nextVersion);
                jobs.put(ce.getKey(), updated);
                Job loaded = serializer.deserializeJob(wire);
                result.add(loaded);
            }
            return result;
        }
    }

    @Override
    public void touchOwnerHeartbeat(NodeId nodeId, Instant now) {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(now, "now");
        // Heartbeat refresh is a non-state-changing operation; it must NOT bump
        // the optimistic-lock version, otherwise an in-flight worker's saveAtomic
        // (running on the version recorded at claim time) would fail with a
        // spurious StaleJobException.
        for (var e : jobs.entrySet()) {
            if (e.getValue().state != JobState.PROCESSING) continue;
            Job j = serializer.deserializeJob(e.getValue().wire);
            if (j.ownerNodeId().filter(o -> o.equals(nodeId)).isPresent()) {
                j.updateHeartbeat(now);
                long sameVersion = e.getValue().version;
                JobSnapshot snap = withVersion(j, sameVersion);
                String wire = serializer.serializeJob(snap, capabilities);
                jobs.put(e.getKey(), entryFromSnapshot(snap, wire, sameVersion));
            }
        }
    }

    @Override
    public boolean saveExecutionUpdate(Job job, NodeId nodeId) {
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(nodeId, "nodeId");
        var changed = new AtomicReference<Boolean>(false);
        jobs.compute(job.id(), (id, existing) -> {
            if (existing == null || existing.state != JobState.PROCESSING) return existing;
            Job persisted = serializer.deserializeJob(existing.wire);
            if (persisted.ownerNodeId().filter(nodeId::equals).isEmpty()) return existing;
            JobSnapshot snap = withVersion(job, existing.version);
            String wire = serializer.serializeJob(snap, capabilities);
            changed.set(true);
            return entryFromSnapshot(snap, wire, existing.version);
        });
        return Boolean.TRUE.equals(changed.get());
    }

    // ---------------------------------------------------------------- queue pauses

    @Override
    public void pauseQueue(String queue, String reason) {
        Names.requireName("queue", queue);
        queuePauses.put(queue, new QueuePause(Instant.now(), reason));
    }

    @Override
    public void resumeQueue(String queue) {
        Names.requireName("queue", queue);
        queuePauses.remove(queue);
    }

    @Override
    public Set<String> listPausedQueues() {
        return Set.copyOf(queuePauses.keySet());
    }

    @Override
    public void recordNodeHeartbeat(NodeId nodeId, Instant now) {
        nodeHeartbeats.put(nodeId, now);
    }

    @Override
    public Optional<Instant> readNodeHeartbeat(NodeId nodeId) {
        return Optional.ofNullable(nodeHeartbeats.get(nodeId));
    }

    @Override
    public boolean acquireOrRenewMaintenanceLease(NodeId nodeId, Duration leaseDuration) {
        Objects.requireNonNull(nodeId, "nodeId");
        com.hemju.threadmill.core.store.Mutexes.requirePositive(leaseDuration);
        synchronized (leaseMutex) {
            var now = Instant.now();
            if (maintenanceLease == null
                    || !maintenanceLease.expiresAt().isAfter(now)
                    || maintenanceLease.holder().equals(nodeId)) {
                maintenanceLease = new MaintenanceLease(nodeId, now.plus(leaseDuration));
                return true;
            }
            return false;
        }
    }

    @Override
    public void releaseMaintenanceLease(NodeId nodeId) {
        Objects.requireNonNull(nodeId, "nodeId");
        synchronized (leaseMutex) {
            if (maintenanceLease != null && maintenanceLease.holder().equals(nodeId)) {
                maintenanceLease = null;
            }
        }
    }

    @Override
    public Optional<NodeId> readMaintenanceLeaseOwner() {
        synchronized (leaseMutex) {
            if (maintenanceLease == null || !maintenanceLease.expiresAt().isAfter(Instant.now())) {
                maintenanceLease = null;
                return Optional.empty();
            }
            return Optional.of(maintenanceLease.holder());
        }
    }

    // ---------------------------------------------------------------- housekeeping

    @Override
    public List<Job> findDueForPromotion(Instant now, int max) {
        return jobs.entrySet().stream()
                .filter(e -> e.getValue().state == JobState.SCHEDULED)
                .filter(e -> e.getValue().scheduledFor != null
                        && !e.getValue().scheduledFor.isAfter(now))
                .sorted(Comparator.comparing(e -> e.getValue().scheduledFor))
                .limit(Math.max(0, max))
                .map(e -> serializer.deserializeJob(e.getValue().wire))
                .collect(Collectors.toList());
    }

    @Override
    public List<Job> findOrphaned(Instant heartbeatExpiry, int max) {
        return jobs.entrySet().stream()
                .filter(e -> e.getValue().state == JobState.PROCESSING)
                .filter(e -> processingLivenessAt(e.getValue()) != null
                        && !processingLivenessAt(e.getValue()).isAfter(heartbeatExpiry))
                .sorted(Comparator.comparing(e -> processingLivenessAt(e.getValue())))
                .limit(Math.max(0, max))
                .map(e -> serializer.deserializeJob(e.getValue().wire))
                .collect(Collectors.toList());
    }

    // ---------------------------------------------------------------- counts & search

    @Override
    public Map<JobState, Long> countsByState() {
        var counts = new EnumMap<JobState, Long>(JobState.class);
        for (JobState s : JobState.values()) counts.put(s, 0L);
        for (Entry e : jobs.values()) {
            counts.merge(e.state, 1L, Long::sum);
        }
        return Collections.unmodifiableMap(counts);
    }

    @Override
    public Map<String, Long> queueDepths() {
        Map<String, Long> depths = new HashMap<>();
        for (Entry e : jobs.values()) {
            if (e.state == JobState.ENQUEUED) {
                depths.merge(e.queue, 1L, Long::sum);
            }
        }
        return Collections.unmodifiableMap(depths);
    }

    @Override
    public List<String> listEnqueuedQueues() {
        return queueDepths().entrySet().stream()
                .filter(e -> e.getValue() > 0)
                .map(Map.Entry::getKey)
                .sorted()
                .collect(Collectors.toList());
    }

    @Override
    public Optional<Instant> oldestEnqueuedAt(String queue) {
        Names.requireName("queue", queue);
        return jobs.values().stream()
                .filter(e -> e.state == JobState.ENQUEUED)
                .filter(e -> queue.equals(e.queue))
                .map(e -> e.currentStateAt)
                .filter(Objects::nonNull)
                .min(Instant::compareTo);
    }

    @Override
    public Optional<Instant> oldestProcessingHeartbeat() {
        return jobs.values().stream()
                .filter(e -> e.state == JobState.PROCESSING)
                .map(e -> e.ownerHeartbeatAt)
                .filter(Objects::nonNull)
                .min(Instant::compareTo);
    }

    @Override
    public List<NodeHeartbeat> listNodeHeartbeats() {
        return nodeHeartbeats.entrySet().stream()
                .map(e -> new NodeHeartbeat(e.getKey(), e.getValue()))
                .sorted(Comparator.comparing(NodeHeartbeat::lastHeartbeatAt))
                .collect(Collectors.toList());
    }

    @Override
    public long deleteNodeHeartbeatsOlderThan(Instant cutoff) {
        Objects.requireNonNull(cutoff, "cutoff");
        var removed = new java.util.concurrent.atomic.AtomicLong();
        nodeHeartbeats.entrySet().removeIf(e -> {
            boolean stale = !e.getValue().isAfter(cutoff);
            if (stale) removed.incrementAndGet();
            return stale;
        });
        return removed.get();
    }

    @Override
    public long deleteExpiredDedupKeys(Instant now, int max) {
        Objects.requireNonNull(now, "now");
        if (max <= 0) return 0L;
        long[] removed = {0L};
        dedupKeys.entrySet().removeIf(e -> {
            if (removed[0] >= max || e.getValue().expiresAt().isAfter(now)) return false;
            Entry job = jobs.get(e.getValue().jobId());
            boolean removable = job == null || isTerminal(job.state);
            if (removable) removed[0]++;
            return removable;
        });
        return removed[0];
    }

    @Override
    public List<Job> findByHandlerSignature(String handlerType, int max) {
        Objects.requireNonNull(handlerType, "handlerType");
        return jobs.values().stream()
                .filter(e -> handlerType.equals(e.handlerType))
                .limit(Math.max(0, max))
                .map(e -> serializer.deserializeJob(e.wire))
                .collect(Collectors.toList());
    }

    // ---------------------------------------------------------------- retention

    @Override
    public long deleteFinishedOlderThan(Instant cutoff, JobState state, int max) {
        long[] removed = {0L};
        List<JobId> toRemove = new ArrayList<>();
        for (var e : jobs.entrySet()) {
            if (toRemove.size() >= max) break;
            if (e.getValue().state == state
                    && e.getValue().currentStateAt != null
                    && !e.getValue().currentStateAt.isAfter(cutoff)) {
                toRemove.add(e.getKey());
            }
        }
        for (JobId id : toRemove) {
            if (jobs.remove(id) != null) {
                removed[0]++;
            }
        }
        return removed[0];
    }

    // ---------------------------------------------------------------- relationships & mutexes

    private final ConcurrentHashMap<String, MutexHolder> mutexes = new ConcurrentHashMap<>();

    private record MutexHolder(String holder, Instant expiresAt) {}

    @Override
    public List<Job> findAwaitingByParent(JobId parentId, int max) {
        Objects.requireNonNull(parentId, "parentId");
        List<Job> out = new ArrayList<>();
        for (Entry e : jobs.values()) {
            if (out.size() >= max) break;
            if (e.state != JobState.AWAITING) continue;
            Job j = serializer.deserializeJob(e.wire);
            if (j.relationship().isPresent()
                    && j.relationship().get().parentId().equals(parentId)) {
                out.add(j);
            }
        }
        return out;
    }

    @Override
    public boolean tryAcquireMutex(String name, String holder, Duration leaseDuration) {
        Names.requireName("mutex", name);
        Objects.requireNonNull(holder, "holder");
        com.hemju.threadmill.core.store.Mutexes.requirePositive(leaseDuration);
        var now = Instant.now();
        Instant expires = now.plus(leaseDuration);
        MutexHolder updated = mutexes.compute(name, (k, prior) -> {
            if (prior == null || !prior.expiresAt.isAfter(now) || prior.holder.equals(holder)) {
                return new MutexHolder(holder, expires);
            }
            return prior;
        });
        return updated.holder.equals(holder) && updated.expiresAt.equals(expires);
    }

    @Override
    public void releaseMutex(String name, String holder) {
        Names.requireName("mutex", name);
        mutexes.computeIfPresent(name, (k, v) -> v.holder.equals(holder) ? null : v);
    }

    @Override
    public boolean replaceJob(JobId id, long expectedVersion, JobReplacement replacement) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(replacement, "replacement");
        var result = new AtomicReference<Boolean>(false);
        var stale = new AtomicReference<StaleJobException>();
        jobs.compute(id, (k, existing) -> {
            if (existing == null) {
                result.set(false);
                return null;
            }
            if (existing.version != expectedVersion) {
                stale.set(new StaleJobException(id, expectedVersion));
                return existing;
            }
            if (!isReplaceable(existing.state)) {
                result.set(false);
                return existing;
            }
            Job current = serializer.deserializeJob(existing.wire);
            Job replaced = JobReplacements.apply(current, replacement);
            long nextVersion = existing.version + 1;
            JobSnapshot snap = withVersion(replaced, nextVersion);
            String wire = serializer.serializeJob(snap, capabilities);
            result.set(true);
            return entryFromSnapshot(snap, wire, nextVersion);
        });
        if (stale.get() != null) throw stale.get();
        return Boolean.TRUE.equals(result.get());
    }

    private static boolean isReplaceable(JobState state) {
        return state == JobState.ENQUEUED || state == JobState.SCHEDULED || state == JobState.AWAITING;
    }

    // ---------------------------------------------------------------- cron tasks

    @Override
    public void upsertCronTask(CronTask task) {
        Objects.requireNonNull(task, "task");
        Names.requireName("cronTask", task.name());
        Names.requireName("queue", task.queue());
        cronTasks.put(task.name(), task);
    }

    @Override
    public Optional<CronTask> findCronTask(String name) {
        return Optional.ofNullable(cronTasks.get(name));
    }

    @Override
    public List<CronTask> listCronTasks() {
        return new ArrayList<>(cronTasks.values());
    }

    @Override
    public void deleteCronTask(String name) {
        cronTasks.remove(name);
        cronTaskStates.remove(name);
    }

    @Override
    public void upsertCronTaskState(CronTaskScheduleState state) {
        Objects.requireNonNull(state, "state");
        cronTaskStates.put(state.taskName(), state);
    }

    @Override
    public Optional<CronTaskScheduleState> findCronTaskState(String name) {
        return Optional.ofNullable(cronTaskStates.get(name));
    }

    // ---------------------------------------------------------------- helpers

    private Entry entryFromSnapshot(JobSnapshot snapshot, String wire, long version) {
        JobState state = snapshot.currentState();
        Instant currentStateAt = lastTransitionTime(snapshot, state);
        return new Entry(
                wire,
                version,
                state,
                snapshot.queue(),
                snapshot.priority(),
                snapshot.scheduledFor(),
                snapshot.ownerHeartbeatAt(),
                snapshot.lastCheckinAt(),
                currentStateAt,
                snapshot.spec().handlerType(),
                snapshot.workflowRootId(),
                snapshot.concurrencyKey(),
                snapshot.concurrencyMode());
    }

    private JobSnapshot snapshotForInsert(Job job, long version) {
        JobSnapshot s = withVersion(job, version);
        if (s.relationship() == null) {
            return s;
        }
        Entry parent = jobs.get(s.relationship().parentId());
        if (parent == null) {
            return s;
        }
        return new JobSnapshot(
                s.id(),
                s.spec(),
                s.queue(),
                s.priority(),
                s.createdAt(),
                s.cronTaskId(),
                s.relationship(),
                parent.workflowRootId,
                parent.concurrencyKey,
                parent.concurrencyMode,
                s.stateHistory(),
                new HashMap<>(s.metadata()),
                s.log(),
                s.progress(),
                version,
                s.ownerNodeId(),
                s.ownerHeartbeatAt(),
                s.lastCheckinAt(),
                s.scheduledFor(),
                s.result(),
                s.attempts());
    }

    private boolean canClaim(Entry candidate) {
        if (candidate.concurrencyKey == null) {
            return true;
        }
        if (hasActiveWorkflowHoldForRoot(candidate)) {
            return true;
        }
        if (candidate.concurrencyMode == ConcurrencyMode.EXCLUSIVE) {
            return noOtherInFlightForKey(candidate) && !hasEarlierPendingJob(candidate);
        }
        if (hasExclusiveInFlightForKey(candidate)) {
            return false;
        }
        return hasEarlierPendingExclusive(candidate) == false;
    }

    private boolean noOtherInFlightForKey(Entry candidate) {
        return jobs.values().stream()
                .noneMatch(e -> sameConcurrencyKey(candidate, e)
                        && !candidate.workflowRootId.equals(e.workflowRootId)
                        && contributesInFlight(e));
    }

    private boolean hasExclusiveInFlightForKey(Entry candidate) {
        return jobs.values().stream()
                .anyMatch(e -> sameConcurrencyKey(candidate, e)
                        && !candidate.workflowRootId.equals(e.workflowRootId)
                        && e.concurrencyMode == ConcurrencyMode.EXCLUSIVE
                        && contributesInFlight(e));
    }

    private boolean hasEarlierPendingExclusive(Entry candidate) {
        return jobs.values().stream()
                .anyMatch(e -> sameConcurrencyKey(candidate, e)
                        && e.concurrencyMode == ConcurrencyMode.EXCLUSIVE
                        && isPending(e.state)
                        && pendingBefore(e, candidate));
    }

    private boolean hasEarlierPendingJob(Entry candidate) {
        return jobs.values().stream()
                .anyMatch(e -> sameConcurrencyKey(candidate, e) && isPending(e.state) && pendingBefore(e, candidate));
    }

    private boolean contributesInFlight(Entry entry) {
        if (entry.state == JobState.PROCESSING) {
            return true;
        }
        if (entry.concurrencyKey == null || !isTerminal(entry.state)) {
            return false;
        }
        return jobs.values().stream()
                .anyMatch(other -> entry.workflowRootId.equals(other.workflowRootId)
                        && entry.concurrencyKey.equals(other.concurrencyKey)
                        && !isTerminal(other.state));
    }

    private boolean hasActiveWorkflowHoldForRoot(Entry candidate) {
        boolean acquired = jobs.values().stream()
                .anyMatch(e -> candidate.workflowRootId.equals(e.workflowRootId)
                        && sameConcurrencyKey(candidate, e)
                        && (e.state == JobState.PROCESSING || isTerminal(e.state)));
        if (!acquired) {
            return false;
        }
        return jobs.values().stream()
                .anyMatch(e -> candidate.workflowRootId.equals(e.workflowRootId)
                        && sameConcurrencyKey(candidate, e)
                        && !isTerminal(e.state));
    }

    private static boolean sameConcurrencyKey(Entry a, Entry b) {
        return a.concurrencyKey != null && a.concurrencyKey.equals(b.concurrencyKey);
    }

    private static boolean pendingBefore(Entry possibleEarlier, Entry candidate) {
        var left = possibleEarlier.currentStateAt;
        var right = candidate.currentStateAt;
        if (left == null && right == null) {
            return false;
        }
        if (left == null) {
            return false;
        }
        if (right == null) {
            return true;
        }
        return left.isBefore(right);
    }

    private static Instant lastTransitionTime(JobSnapshot snapshot, JobState state) {
        List<JobStateEntry> history = snapshot.stateHistory();
        for (int i = history.size() - 1; i >= 0; i--) {
            if (history.get(i).state() == state) return history.get(i).at();
        }
        return snapshot.createdAt();
    }

    private static Instant processingLivenessAt(Entry e) {
        if (e.lastCheckinAt == null) return e.ownerHeartbeatAt;
        if (e.ownerHeartbeatAt == null) return e.lastCheckinAt;
        return e.lastCheckinAt.isAfter(e.ownerHeartbeatAt) ? e.lastCheckinAt : e.ownerHeartbeatAt;
    }

    /**
     * Snapshot the job, but with a different version field. Used to compute
     * the wire form for the *next* version before we commit it.
     */
    private static JobSnapshot withVersion(Job job, long version) {
        JobSnapshot s = job.snapshot();
        return new JobSnapshot(
                s.id(),
                s.spec(),
                s.queue(),
                s.priority(),
                s.createdAt(),
                s.cronTaskId(),
                s.relationship(),
                s.workflowRootId(),
                s.concurrencyKey(),
                s.concurrencyMode(),
                s.stateHistory(),
                new HashMap<>(s.metadata()),
                s.log(),
                s.progress(),
                version,
                s.ownerNodeId(),
                s.ownerHeartbeatAt(),
                s.lastCheckinAt(),
                s.scheduledFor(),
                s.result(),
                s.attempts());
    }

    private static boolean isTerminal(JobState state) {
        return switch (state) {
            case SUCCEEDED, FAILED, DELETED, QUARANTINED -> true;
            case AWAITING, SCHEDULED, ENQUEUED, PROCESSING, PROCESSED -> false;
        };
    }

    private static boolean isPending(JobState state) {
        return switch (state) {
            case AWAITING, SCHEDULED, ENQUEUED -> true;
            case PROCESSING, PROCESSED, SUCCEEDED, FAILED, DELETED, QUARANTINED -> false;
        };
    }
}
