package com.hemju.threadmill.core.serialization;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobLog;
import com.hemju.threadmill.core.JobProgress;
import com.hemju.threadmill.core.JobRelationship;
import com.hemju.threadmill.core.JobResult;
import com.hemju.threadmill.core.JobSnapshot;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.JobStateEntry;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.OversizedJobException;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStoreCapabilities;

/**
 * JSON implementation of {@link JobSerializer}, backed by Jackson.
 *
 * <p>The wire format is intentionally explicit (typed) and human-readable.
 * It pins down every field of the {@link JobSnapshot} so a relational store
 * and a key-value store both round-trip the exact same bytes.
 *
 * <p>Full Unicode — including 4-byte (supplementary-plane) characters — is
 * supported and round-trips losslessly.
 */
public final class JsonJobSerializer implements JobSerializer {

    private final ObjectMapper mapper;
    private final TypeNameAliases typeNameAliases;
    private final PayloadMigrations payloadMigrations;

    public JsonJobSerializer() {
        this(defaultMapper(), TypeNameAliases.empty(), PayloadMigrations.empty());
    }

    /**
     * Construct a serializer backed by a caller-supplied {@link ObjectMapper}.
     * Hosts use this to share the mapper they already configure for the
     * application, avoiding two competing Jackson configurations.
     */
    public JsonJobSerializer(ObjectMapper mapper) {
        this(mapper, TypeNameAliases.empty(), PayloadMigrations.empty());
    }

    public JsonJobSerializer(TypeNameAliases typeNameAliases) {
        this(defaultMapper(), typeNameAliases, PayloadMigrations.empty());
    }

    public JsonJobSerializer(TypeNameAliases typeNameAliases, PayloadMigrations payloadMigrations) {
        this(defaultMapper(), typeNameAliases, payloadMigrations);
    }

    public JsonJobSerializer(ObjectMapper mapper, TypeNameAliases typeNameAliases) {
        this(mapper, typeNameAliases, PayloadMigrations.empty());
    }

    public JsonJobSerializer(
            ObjectMapper mapper, TypeNameAliases typeNameAliases, PayloadMigrations payloadMigrations) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.typeNameAliases = Objects.requireNonNull(typeNameAliases, "typeNameAliases");
        this.payloadMigrations = Objects.requireNonNull(payloadMigrations, "payloadMigrations");
    }

    public static ObjectMapper defaultMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    // ---------------------------------------------------------------- Job

    @Override
    public String serializeJob(JobSnapshot s, JobStoreCapabilities caps) {
        Objects.requireNonNull(s, "snapshot");
        Objects.requireNonNull(caps, "capabilities");
        return serializeJob(truncateForSerialization(s, caps), caps.maxSerializedJobBytes());
    }

    /**
     * Metadata key recording how many metadata entries were dropped at
     * serialization time to fit {@code maxMetadataBytes}.
     */
    public static final String TRUNCATED_METADATA_KEY = "threadmill.truncated.metadata";

    /**
     * Metadata key recording how many state-history entries were elided at
     * serialization time to fit {@code maxStateHistoryEntries}.
     */
    public static final String TRUNCATED_STATE_HISTORY_KEY = "threadmill.truncated.stateHistory";

    /**
     * Metadata key recording that an oversized {@code JobResult} was dropped
     * at terminal-save time instead of blocking the save.
     */
    public static final String TRUNCATED_RESULT_KEY = "threadmill.truncated.result";

    /**
     * Produce a snapshot whose {@code JobLog} fits {@code maxJobLogBytes},
     * whose FAILED / QUARANTINED state-history messages fit
     * {@code maxFailureMetadataBytes}, whose state history fits
     * {@code maxStateHistoryEntries} (creation entry plus the most recent
     * entries are kept), and whose metadata fits {@code maxMetadataBytes}
     * (largest user entries dropped first; {@code threadmill.}-prefixed
     * engine entries kept longest). Elisions are recorded under
     * {@link #TRUNCATED_METADATA_KEY} / {@link #TRUNCATED_STATE_HISTORY_KEY}.
     * The snapshot remains immutable — truncation builds a new copy.
     *
     * <p>Metadata and state-history-count elision apply only to
     * <em>terminal-state</em> snapshots: a terminal save must never be
     * blocked by user/engine-growable areas (the stuck-in-PROCESSING failure
     * mode), while non-terminal saves keep the §6 contract of rejecting
     * oversize jobs loudly at creation.
     */
    static JobSnapshot truncateForSerialization(JobSnapshot s, JobStoreCapabilities caps) {
        List<JobLog.Entry> trimmedLog = trimLog(s.log(), caps.maxJobLogBytes());
        List<JobStateEntry> trimmedHistory = trimFailureMessages(s.stateHistory(), caps.maxFailureMetadataBytes());
        boolean terminal = isTerminalSaveState(s.currentState());
        int elidedHistory = 0;
        int maxEntries = caps.maxStateHistoryEntries();
        if (terminal && maxEntries > 1 && trimmedHistory.size() > maxEntries) {
            elidedHistory = trimmedHistory.size() - maxEntries;
            var kept = new ArrayList<JobStateEntry>(maxEntries);
            kept.add(trimmedHistory.get(0));
            kept.addAll(trimmedHistory.subList(trimmedHistory.size() - (maxEntries - 1), trimmedHistory.size()));
            trimmedHistory = kept;
        }
        JobResult result = s.result();
        var markers = new HashMap<String, String>();
        if (elidedHistory > 0) {
            markers.put(TRUNCATED_STATE_HISTORY_KEY, elidedHistory + " entries omitted");
        }
        if (terminal && result != null) {
            long resultBytes = result.serialized().getBytes(StandardCharsets.UTF_8).length;
            if (resultBytes > caps.maxMetadataBytes()) {
                // An uncapped handler result must never block the terminal save.
                markers.put(TRUNCATED_RESULT_KEY, resultBytes + " bytes omitted");
                result = null;
            }
        }
        Map<String, String> trimmedMetadata =
                terminal ? trimMetadata(s.metadata(), markers, caps.maxMetadataBytes()) : s.metadata();
        if (trimmedLog == s.log()
                && trimmedHistory == s.stateHistory()
                && trimmedMetadata == s.metadata()
                && result == s.result()) {
            return s;
        }
        return new JobSnapshot(
                s.id(),
                s.spec(),
                s.queue(),
                s.priority(),
                s.createdAt(),
                s.cronTaskName(),
                s.relationship(),
                s.workflowRootId(),
                s.concurrencyKey(),
                s.concurrencyMode(),
                trimmedHistory,
                trimmedMetadata,
                trimmedLog,
                s.progress(),
                s.version(),
                s.ownerNodeId(),
                s.ownerHeartbeatAt(),
                s.lastCheckinAt(),
                s.scheduledFor(),
                result,
                s.attempts());
    }

    /**
     * States whose save is the end of a processing attempt: the single
     * failure code path (FAILED is terminal-pending, not {@code isTerminal})
     * plus the true terminal states.
     */
    private static boolean isTerminalSaveState(JobState state) {
        return state.isTerminal() || state == JobState.FAILED;
    }

    private static Map<String, String> trimMetadata(
            Map<String, String> metadata, Map<String, String> markers, int maxBytes) {
        Map<String, String> out = metadata;
        if (!markers.isEmpty()) {
            out = new HashMap<>(metadata);
            out.putAll(markers);
        }
        if (maxBytes <= 0 || out.isEmpty()) {
            return out;
        }
        long total = 0;
        for (var e : out.entrySet()) total += metadataByteCost(e);
        if (total <= maxBytes) {
            return out;
        }
        var mutable = out == metadata ? new HashMap<>(metadata) : (HashMap<String, String>) out;
        // Drop largest user entries first; engine ("threadmill.") entries and
        // the elision markers are kept longest.
        var dropOrder = mutable.entrySet().stream()
                .sorted(Comparator.comparing(
                                (Map.Entry<String, String> e) -> e.getKey().startsWith("threadmill."))
                        .thenComparing(e -> -metadataByteCost(e))
                        .thenComparing(Map.Entry::getKey))
                .map(e -> Map.entry(e.getKey(), e.getValue()))
                .toList();
        int omitted = 0;
        for (var e : dropOrder) {
            if (total <= maxBytes) break;
            if (e.getKey().startsWith("threadmill.truncated.")) continue;
            mutable.remove(e.getKey());
            total -= metadataByteCost(e);
            omitted++;
        }
        if (omitted > 0) {
            mutable.put(TRUNCATED_METADATA_KEY, omitted + " entries omitted");
        }
        return mutable;
    }

    private static long metadataByteCost(Map.Entry<String, String> e) {
        return e.getKey().getBytes(StandardCharsets.UTF_8).length
                + e.getValue().getBytes(StandardCharsets.UTF_8).length
                + 8L;
    }

    private static List<JobLog.Entry> trimLog(List<JobLog.Entry> log, int maxBytes) {
        if (log == null || log.isEmpty() || maxBytes <= 0) return log;
        long total = 0;
        for (var e : log) total += entryByteCost(e);
        if (total <= maxBytes) return log;
        var trimmed = new ArrayList<>(log);
        while (!trimmed.isEmpty() && total > maxBytes) {
            total -= entryByteCost(trimmed.removeFirst());
        }
        return trimmed;
    }

    private static long entryByteCost(JobLog.Entry e) {
        return e.message() == null ? 64L : e.message().getBytes(StandardCharsets.UTF_8).length + 64L;
    }

    private static List<JobStateEntry> trimFailureMessages(List<JobStateEntry> history, int maxBytes) {
        if (history == null || history.isEmpty() || maxBytes <= 0) return history;
        boolean changed = false;
        var out = new ArrayList<JobStateEntry>(history.size());
        for (var e : history) {
            if ((e.state() == JobState.FAILED || e.state() == JobState.QUARANTINED) && e.message() != null) {
                String capped = capFailureMessage(e.message(), maxBytes);
                if (capped != e.message()) {
                    out.add(new JobStateEntry(e.state(), e.at(), e.reason(), capped));
                    changed = true;
                    continue;
                }
            }
            out.add(e);
        }
        return changed ? out : history;
    }

    private static String capFailureMessage(String message, int maxBytes) {
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= maxBytes) return message;
        int keep = Math.max(
                0, maxBytes - truncationSuffix(bytes.length - maxBytes).getBytes(StandardCharsets.UTF_8).length);
        int charBudget = Math.min(message.length(), keep);
        while (charBudget > 0) {
            // Never split a surrogate pair: a trailing lone high surrogate is dropped.
            if (Character.isHighSurrogate(message.charAt(charBudget - 1))) {
                charBudget--;
                continue;
            }
            String prefix = message.substring(0, charBudget);
            int prefixBytes = prefix.getBytes(StandardCharsets.UTF_8).length;
            if (prefixBytes > keep) {
                charBudget--;
                continue;
            }
            String capped = prefix + truncationSuffix(bytes.length - prefixBytes);
            if (capped.getBytes(StandardCharsets.UTF_8).length <= maxBytes) {
                return capped;
            }
            charBudget--;
        }
        String suffixOnly = truncationSuffix(bytes.length);
        if (suffixOnly.getBytes(StandardCharsets.UTF_8).length <= maxBytes) {
            return suffixOnly;
        }
        // Budget smaller than the sentinel itself: clamp a bare ASCII marker.
        String marker = "<truncated>";
        return marker.length() <= maxBytes ? marker : marker.substring(0, Math.max(0, maxBytes));
    }

    private static String truncationSuffix(long omittedBytes) {
        return " <truncated, " + omittedBytes + " more bytes omitted>";
    }

    @Override
    public String serializeJob(JobSnapshot s, long maxBytes) {
        try {
            ObjectNode root = mapper.createObjectNode();
            root.put("id", s.id().toString());
            root.set("spec", writeSpec(s.spec()));
            root.put("queue", s.queue());
            root.put("priority", s.priority());
            root.put("createdAt", s.createdAt().toString());
            if (s.cronTaskName() != null) {
                root.put("cronTaskName", s.cronTaskName());
            }
            if (s.relationship() != null) {
                root.set("relationship", writeRelationship(s.relationship()));
            }
            root.put("workflowRootId", s.workflowRootId().toString());
            if (s.concurrencyKey() != null) {
                root.put("concurrencyKey", s.concurrencyKey());
                root.put("concurrencyMode", s.concurrencyMode().name());
            }
            root.set("stateHistory", writeStateHistory(s.stateHistory()));
            root.set("metadata", mapper.valueToTree(s.metadata()));
            root.set("log", writeLog(s.log()));
            if (s.progress() != null) {
                ObjectNode pn = mapper.createObjectNode();
                pn.put("fraction", s.progress().fraction());
                if (s.progress().message() != null) {
                    pn.put("message", s.progress().message());
                }
                root.set("progress", pn);
            }
            root.put("version", s.version());
            if (s.ownerNodeId() != null) {
                root.put("ownerNodeId", s.ownerNodeId().toString());
            }
            if (s.ownerHeartbeatAt() != null) {
                root.put("ownerHeartbeatAt", s.ownerHeartbeatAt().toString());
            }
            if (s.lastCheckinAt() != null) {
                root.put("lastCheckinAt", s.lastCheckinAt().toString());
            }
            if (s.scheduledFor() != null) {
                root.put("scheduledFor", s.scheduledFor().toString());
            }
            if (s.result() != null) {
                ObjectNode rn = mapper.createObjectNode();
                rn.put("typeTag", s.result().typeTag());
                rn.put("serialized", s.result().serialized());
                root.set("result", rn);
            }
            root.put("attempts", s.attempts());

            String wire = mapper.writeValueAsString(root);
            long byteLength = wire.getBytes(StandardCharsets.UTF_8).length;
            if (byteLength > maxBytes) {
                throw new OversizedJobException(byteLength, maxBytes);
            }
            return wire;
        } catch (OversizedJobException oversized) {
            throw oversized;
        } catch (IOException io) {
            throw new SerializationException("Failed to serialize job", io);
        }
    }

    @Override
    public Job deserializeJob(String wire) {
        try {
            ObjectNode root = (ObjectNode) mapper.readTree(wire);
            var id = JobId.parse(root.get("id").asText());
            JobSpec spec = readSpec(root.get("spec"));
            String queue = root.get("queue").asText();
            int priority = root.get("priority").asInt();
            var createdAt = Instant.parse(root.get("createdAt").asText());
            String cronTaskName =
                    root.hasNonNull("cronTaskName") ? root.get("cronTaskName").asText() : null;
            JobRelationship relationship =
                    root.hasNonNull("relationship") ? readRelationship(root.get("relationship")) : null;
            JobId workflowRootId = root.hasNonNull("workflowRootId")
                    ? JobId.parse(root.get("workflowRootId").asText())
                    : id;
            String concurrencyKey = root.hasNonNull("concurrencyKey")
                    ? root.get("concurrencyKey").asText()
                    : null;
            ConcurrencyMode concurrencyMode = root.hasNonNull("concurrencyMode")
                    ? ConcurrencyMode.valueOf(root.get("concurrencyMode").asText())
                    : null;
            List<JobStateEntry> history = readStateHistory(root.get("stateHistory"));
            Map<String, String> metadata = readMetadata(root.get("metadata"));
            long version = root.get("version").asLong();
            int attempts = root.has("attempts") ? root.get("attempts").asInt() : 0;
            Instant scheduledFor = root.hasNonNull("scheduledFor")
                    ? Instant.parse(root.get("scheduledFor").asText())
                    : null;

            Job.Builder b = Job.builder()
                    .id(id)
                    .spec(spec)
                    .queue(queue)
                    .priority(priority)
                    .createdAt(createdAt)
                    .version(version)
                    .attempts(attempts)
                    .withStateHistory(history)
                    .relationship(relationship)
                    .workflowRootId(workflowRootId)
                    .concurrencyKey(concurrencyKey)
                    .concurrencyMode(concurrencyMode);
            if (cronTaskName != null) b.cronTaskName(cronTaskName);
            if (scheduledFor != null) b.scheduledFor(scheduledFor);
            metadata.forEach(b::metadata);

            Job job = b.build();
            if (root.hasNonNull("ownerNodeId") && root.hasNonNull("ownerHeartbeatAt")) {
                job.assignOwner(
                        NodeId.parse(root.get("ownerNodeId").asText()),
                        Instant.parse(root.get("ownerHeartbeatAt").asText()));
            }
            if (root.hasNonNull("lastCheckinAt")) {
                job.restoreCheckIn(Instant.parse(root.get("lastCheckinAt").asText()));
            }
            if (root.hasNonNull("log")) {
                List<JobLog.Entry> entries = readLog(root.get("log"));
                job.log().replaceAll(entries);
            }
            if (root.hasNonNull("progress")) {
                double fraction = root.get("progress").get("fraction").asDouble();
                String message = root.get("progress").hasNonNull("message")
                        ? root.get("progress").get("message").asText()
                        : null;
                job.progress().replace(new JobProgress.Snapshot(fraction, message));
            }
            if (root.hasNonNull("result")) {
                job.setResult(new JobResult(
                        root.get("result").get("typeTag").asText(),
                        root.get("result").get("serialized").asText()));
            }
            return job;
        } catch (SerializationException e) {
            throw e;
        } catch (RuntimeException | IOException e) {
            // Structurally malformed-but-valid JSON surfaces as NPE / CCE /
            // DateTimeParseException; the documented contract is SerializationException.
            throw new SerializationException("Failed to deserialize job", e);
        }
    }

    // ---------------------------------------------------------------- arguments / payloads

    @Override
    public JobArgument serializeArgument(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Cannot serialize null argument; use a typed null wrapper");
        }
        try {
            String typeTag = value.getClass().getName();
            String serialized = mapper.writeValueAsString(value);
            return new JobArgument(typeTag, serialized);
        } catch (IOException io) {
            throw new SerializationException("Failed to serialize argument", io);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Trust boundary: the type tag is persisted data. The named class is
     * loaded <em>without initialization</em> and must implement
     * {@link JobPayload} before Jackson instantiates it — a tag naming any
     * other classpath type is rejected with a {@link SerializationException}
     * and its static initializers never run.
     */
    @Override
    public Object deserializeArgument(JobArgument argument) {
        Objects.requireNonNull(argument, "argument");
        JobArgument migrated = migrateArgument(argument);
        String resolvedType = resolveTypeTag(migrated.typeTag());
        try {
            Class<?> type = Class.forName(resolvedType, false, JsonJobSerializer.class.getClassLoader());
            if (!JobPayload.class.isAssignableFrom(type)) {
                throw new SerializationException("Argument type is not a JobPayload: " + resolvedType);
            }
            return mapper.readValue(migrated.serialized(), type);
        } catch (ClassNotFoundException notFound) {
            throw new SerializationException("Unknown argument type: " + migrated.typeTag(), notFound);
        } catch (IOException io) {
            throw new SerializationException("Failed to deserialize argument", io);
        }
    }

    @Override
    public String resolveTypeTag(String typeTag) {
        return typeNameAliases.resolve(typeTag);
    }

    @Override
    public JobArgument migrateArgument(JobArgument argument) {
        return payloadMigrations.migrate(argument).orElse(argument);
    }

    @Override
    public JobArgument serializePayload(JobPayload payload) {
        return serializeArgument(payload);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <P extends JobPayload> P deserializePayload(JobArgument argument, Class<P> type) {
        Objects.requireNonNull(argument, "argument");
        Objects.requireNonNull(type, "type");
        JobArgument migrated = migrateArgument(argument);
        try {
            return mapper.readValue(migrated.serialized(), type);
        } catch (IOException io) {
            throw new SerializationException("Failed to deserialize payload", io);
        }
    }

    // ---------------------------------------------------------------- helpers

    private ObjectNode writeSpec(JobSpec spec) {
        ObjectNode out = mapper.createObjectNode();
        out.put("handlerType", spec.handlerType());
        out.set("arguments", writeArguments(spec.arguments()));
        if (spec.dedupKey() != null) {
            out.put("dedupKey", spec.dedupKey());
            out.put("dedupTtl", spec.dedupTtl().toString());
        }
        return out;
    }

    private JobSpec readSpec(JsonNode node) {
        String handlerType = node.get("handlerType").asText();
        List<JobArgument> args = new ArrayList<>();
        for (JsonNode n : node.get("arguments")) {
            args.add(new JobArgument(
                    n.get("typeTag").asText(), n.get("serialized").asText()));
        }
        String dedupKey = node.hasNonNull("dedupKey") ? node.get("dedupKey").asText() : null;
        Duration dedupTtl = node.hasNonNull("dedupTtl")
                ? Duration.parse(node.get("dedupTtl").asText())
                : null;
        return new JobSpec(handlerType, args, dedupKey, dedupTtl);
    }

    private JsonNode writeArguments(List<JobArgument> args) {
        ArrayNode arr = mapper.createArrayNode();
        for (JobArgument arg : args) {
            ObjectNode o = mapper.createObjectNode();
            o.put("typeTag", arg.typeTag());
            o.put("serialized", arg.serialized());
            arr.add(o);
        }
        return arr;
    }

    private ObjectNode writeRelationship(JobRelationship r) {
        ObjectNode o = mapper.createObjectNode();
        o.put("parentId", r.parentId().toString());
        o.put("kind", r.kind().name());
        return o;
    }

    private JobRelationship readRelationship(JsonNode n) {
        return new JobRelationship(
                JobId.parse(n.get("parentId").asText()),
                JobRelationship.Kind.valueOf(n.get("kind").asText()));
    }

    private JsonNode writeStateHistory(List<JobStateEntry> history) {
        ArrayNode arr = mapper.createArrayNode();
        for (var e : history) {
            ObjectNode o = mapper.createObjectNode();
            o.put("state", e.state().name());
            o.put("at", e.at().toString());
            if (e.reason() != null) o.put("reason", e.reason());
            if (e.message() != null) o.put("message", e.message());
            arr.add(o);
        }
        return arr;
    }

    private List<JobStateEntry> readStateHistory(JsonNode node) {
        List<JobStateEntry> out = new ArrayList<>();
        for (JsonNode n : node) {
            out.add(new JobStateEntry(
                    JobState.valueOf(n.get("state").asText()),
                    Instant.parse(n.get("at").asText()),
                    n.hasNonNull("reason") ? n.get("reason").asText() : null,
                    n.hasNonNull("message") ? n.get("message").asText() : null));
        }
        return out;
    }

    private Map<String, String> readMetadata(JsonNode node) {
        Map<String, String> out = new LinkedHashMap<>();
        node.fieldNames().forEachRemaining(name -> out.put(name, node.get(name).asText()));
        return out;
    }

    private JsonNode writeLog(List<JobLog.Entry> log) {
        ArrayNode arr = mapper.createArrayNode();
        for (var e : log) {
            ObjectNode o = mapper.createObjectNode();
            o.put("at", e.at().toString());
            o.put("level", e.level().name());
            o.put("message", e.message());
            arr.add(o);
        }
        return arr;
    }

    private List<JobLog.Entry> readLog(JsonNode node) {
        List<JobLog.Entry> out = new ArrayList<>();
        for (JsonNode n : node) {
            out.add(new JobLog.Entry(
                    Instant.parse(n.get("at").asText()),
                    JobLog.Level.valueOf(n.get("level").asText()),
                    n.get("message").asText()));
        }
        return out;
    }
}
