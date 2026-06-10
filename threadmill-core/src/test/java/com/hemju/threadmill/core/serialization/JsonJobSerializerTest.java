package com.hemju.threadmill.core.serialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobLog;
import com.hemju.threadmill.core.JobRelationship;
import com.hemju.threadmill.core.JobResult;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.OversizedJobException;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStoreCapabilities;

class JsonJobSerializerTest {

    private final JsonJobSerializer serializer = new JsonJobSerializer();

    @Test
    void jobRoundTripsAllCoreFields() {
        Job j = Job.builder()
                .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.Long", "42")))
                .queue("high")
                .priority(3)
                .relationship(new JobRelationship(JobId.newId(), JobRelationship.Kind.WORKFLOW_STEP))
                .concurrencyKey("project:42")
                .concurrencyMode(ConcurrencyMode.EXCLUSIVE)
                .metadata("trace", "abc")
                .build();
        j.log().info("hello");
        j.progress().update(0.25, "quarter");
        j.assignOwner(NodeId.newId(), Instant.parse("2026-01-01T00:00:00Z"));
        j.setResult(new JobResult("java.lang.String", "\"ok\""));

        String wire = serializer.serializeJob(j.snapshot(), 1_000_000);
        Job loaded = serializer.deserializeJob(wire);

        assertThat(loaded.id()).isEqualTo(j.id());
        assertThat(loaded.spec().handlerType()).isEqualTo("com.example.H");
        assertThat(loaded.spec().arguments()).containsExactly(new JobArgument("java.lang.Long", "42"));
        assertThat(loaded.queue()).isEqualTo("high");
        assertThat(loaded.priority()).isEqualTo(3);
        assertThat(loaded.relationship()).isPresent();
        assertThat(loaded.workflowRootId()).isEqualTo(j.workflowRootId());
        assertThat(loaded.concurrencyKey()).contains("project:42");
        assertThat(loaded.concurrencyMode()).contains(ConcurrencyMode.EXCLUSIVE);
        assertThat(loaded.metadata().get("trace")).contains("abc");
        assertThat(loaded.log().snapshot()).hasSize(1);
        assertThat(loaded.log().snapshot().get(0).level()).isEqualTo(JobLog.Level.INFO);
        assertThat(loaded.progress().snapshot()).hasValueSatisfying(s -> {
            assertThat(s.fraction()).isEqualTo(0.25);
            assertThat(s.message()).isEqualTo("quarter");
        });
        assertThat(loaded.ownerNodeId()).isPresent();
        assertThat(loaded.result())
                .hasValueSatisfying(r -> assertThat(r.typeTag()).isEqualTo("java.lang.String"));
        assertThat(loaded.currentState()).isEqualTo(JobState.ENQUEUED);
    }

    @Test
    void ownerlessJobWithCheckinDoesNotFabricateOwnerHeartbeatOnRoundTrip() {
        Job j = Job.builder()
                .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.Long", "42")))
                .build();
        long version = j.version();
        j.transitionTo(JobState.PROCESSING, Instant.now(), "test", null);
        j.assignOwner(NodeId.newId(), Instant.parse("2026-01-01T00:00:00Z"));
        j.checkIn(Instant.parse("2026-01-01T00:00:05Z"));
        j.transitionTo(JobState.FAILED, Instant.now(), "test", "boom");
        j.clearOwner();
        j.adoptVersion(version + 1);

        String wire = serializer.serializeJob(j.snapshot(), 1_000_000);
        Job loaded = serializer.deserializeJob(wire);

        assertThat(loaded.lastCheckinAt()).contains(Instant.parse("2026-01-01T00:00:05Z"));
        assertThat(loaded.ownerNodeId()).isEmpty();
        assertThat(loaded.ownerHeartbeatAt()).isEmpty();

        // serialize -> deserialize -> serialize must be idempotent.
        assertThat(serializer.serializeJob(loaded.snapshot(), 1_000_000)).isEqualTo(wire);
    }

    @Test
    void fourByteUnicodeRoundTripsExactly() {
        String exotic = "✈🚀𐀀𐀁𐀂😀";
        Job j = Job.builder()
                .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"\"")))
                .metadata("note", exotic)
                .build();
        j.log().info(exotic);
        String wire = serializer.serializeJob(j.snapshot(), 1_000_000);
        Job loaded = serializer.deserializeJob(wire);
        assertThat(loaded.metadata().get("note")).contains(exotic);
        assertThat(loaded.log().snapshot().get(0).message()).isEqualTo(exotic);
    }

    @Test
    void oversizedFailureMetadataDoesNotBlockSave() {
        // Audit §6.3 — a FAILED state-history entry carrying a 200KB exception
        // message must still serialize under the body-size cap. The serializer's
        // capabilities-aware overload truncates the FAILED message in place.
        Job j = Job.builder()
                .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"\"")))
                .build();
        String hugeMessage = "big-error: " + "x".repeat(200 * 1024);
        j.transitionTo(JobState.PROCESSING, Instant.now(), "engine.claim", null);
        j.transitionTo(JobState.FAILED, Instant.now(), "engine.exception", hugeMessage);

        var caps = new JobStoreCapabilities(
                64L * 1024L, // overall cap
                16 * 1024, // log cap
                32 * 1024, // failure-metadata cap
                100,
                true,
                true,
                true,
                true);
        String wire = serializer.serializeJob(j.snapshot(), caps);
        Job loaded = serializer.deserializeJob(wire);

        // The truncated record preserves the leading fragment and the sentinel.
        assertThat(loaded.stateHistory())
                .anySatisfy(
                        e -> assertThat(e.message()).startsWith("big-error:").contains("truncated"));
    }

    @Test
    void capFailureMessageNeverSplitsSurrogatePairsAndRespectsMaxBytes() {
        Job j = Job.builder()
                .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"\"")))
                .build();
        j.transitionTo(JobState.PROCESSING, Instant.now(), "engine.claim", null);
        j.transitionTo(JobState.FAILED, Instant.now(), "engine.exception", "é".repeat(2) + "💥".repeat(100));

        var caps = new JobStoreCapabilities(64L * 1024L, 16 * 1024, 64, 100, true, true, true, true);
        var truncated = JsonJobSerializer.truncateForSerialization(j.snapshot(), caps);
        String message = truncated.stateHistory().stream()
                .filter(e -> e.state() == JobState.FAILED)
                .findFirst()
                .orElseThrow()
                .message();
        assertThat(message.getBytes(StandardCharsets.UTF_8).length).isLessThanOrEqualTo(64);
        assertNoLoneSurrogates(message);

        // A budget smaller than the sentinel suffix must still be respected.
        var tinyCaps = new JobStoreCapabilities(64L * 1024L, 16 * 1024, 10, 100, true, true, true, true);
        String tinyMessage = JsonJobSerializer.truncateForSerialization(j.snapshot(), tinyCaps).stateHistory().stream()
                .filter(e -> e.state() == JobState.FAILED)
                .findFirst()
                .orElseThrow()
                .message();
        assertThat(tinyMessage.getBytes(StandardCharsets.UTF_8).length).isLessThanOrEqualTo(10);
        assertNoLoneSurrogates(tinyMessage);
    }

    private static void assertNoLoneSurrogates(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (Character.isHighSurrogate(c)) {
                assertThat(i + 1 < s.length() && Character.isLowSurrogate(s.charAt(i + 1)))
                        .as("high surrogate at %d must be followed by a low surrogate", i)
                        .isTrue();
                i++;
            } else {
                assertThat(Character.isLowSurrogate(c))
                        .as("unpaired low surrogate at %d", i)
                        .isFalse();
            }
        }
    }

    @Test
    void malformedWireYieldsSerializationExceptionNotRawRuntimeExceptions() {
        assertThatThrownBy(() -> serializer.deserializeJob("[]")).isInstanceOf(SerializationException.class);
        assertThatThrownBy(() -> serializer.deserializeJob("{}")).isInstanceOf(SerializationException.class);
        assertThatThrownBy(() -> serializer.deserializeJob("{\"id\":\"not-a-uuid\"}"))
                .isInstanceOf(SerializationException.class);
    }

    @Test
    void hugeMetadataAndLongRetryHistoryStillFitTheTerminalSave() {
        Job j = Job.builder()
                .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"\"")))
                .metadata("threadmill.retry.maxAttempts", "5")
                .build();
        for (int i = 0; i < 200; i++) {
            j.metadata().put("user.bulk" + i, "x".repeat(2048));
        }
        for (int i = 0; i < 150; i++) {
            j.transitionTo(JobState.PROCESSING, Instant.now(), "engine.claim", null);
            j.transitionTo(JobState.FAILED, Instant.now(), "engine.exception", "attempt " + i + " failed");
            j.transitionTo(JobState.SCHEDULED, Instant.now(), "retry.backoff", null);
            j.transitionTo(JobState.ENQUEUED, Instant.now(), "engine.promote", null);
        }
        j.transitionTo(JobState.PROCESSING, Instant.now(), "engine.claim", null);
        j.transitionTo(JobState.FAILED, Instant.now(), "engine.exception", "final failure");

        var caps = new JobStoreCapabilities(64L * 1024L, 16 * 1024, 8 * 1024, 100, true, true, true, true);
        String wire = serializer.serializeJob(j.snapshot(), caps);
        assertThat(wire.getBytes(StandardCharsets.UTF_8).length).isLessThanOrEqualTo(64 * 1024);

        Job loaded = serializer.deserializeJob(wire);
        // The creation entry and the terminal failure detail survive the elision.
        assertThat(loaded.stateHistory().getFirst().state()).isEqualTo(JobState.ENQUEUED);
        assertThat(loaded.currentState()).isEqualTo(JobState.FAILED);
        assertThat(loaded.stateHistory().getLast().message()).isEqualTo("final failure");
        assertThat(loaded.stateHistory())
                .hasSizeLessThanOrEqualTo(JobStoreCapabilities.DEFAULT_MAX_STATE_HISTORY_ENTRIES);
        // Engine metadata survives; bulk user metadata is dropped with markers.
        assertThat(loaded.metadata().get("threadmill.retry.maxAttempts")).contains("5");
        assertThat(loaded.metadata().get(JsonJobSerializer.TRUNCATED_METADATA_KEY))
                .isPresent();
        assertThat(loaded.metadata().get(JsonJobSerializer.TRUNCATED_STATE_HISTORY_KEY))
                .isPresent();
    }

    @Test
    void oversizeThrowsAndDoesNotMutateSnapshotSource() {
        Job j = Job.builder()
                .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"\"")))
                .build();
        String chunk = "x".repeat(2048);
        for (int i = 0; i < 500; i++) j.metadata().put("k" + i, chunk);

        long versionBefore = j.version();
        assertThatThrownBy(() -> serializer.serializeJob(j.snapshot(), 16 * 1024L))
                .isInstanceOf(OversizedJobException.class);
        assertThat(j.version()).isEqualTo(versionBefore);
    }

    static final AtomicBoolean GADGET_INITIALIZED = new AtomicBoolean();

    public static final class NotAPayloadGadget {
        static {
            GADGET_INITIALIZED.set(true);
        }
    }

    @Test
    void deserializeArgumentRejectsNonPayloadTypesWithoutRunningTheirInitializers() {
        assertThatThrownBy(
                        () -> serializer.deserializeArgument(new JobArgument(NotAPayloadGadget.class.getName(), "{}")))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("not a JobPayload");
        assertThat(GADGET_INITIALIZED).isFalse();
    }

    public static final class SamplePayload implements JobPayload {
        public String name;
        public int count;

        public SamplePayload() {}

        public SamplePayload(String name, int count) {
            this.name = name;
            this.count = count;
        }
    }

    @Test
    void payloadRoundTrips() {
        var original = new SamplePayload("widgets", 7);
        JobArgument arg = serializer.serializePayload(original);
        SamplePayload back = serializer.deserializePayload(arg, SamplePayload.class);
        assertThat(back.name).isEqualTo("widgets");
        assertThat(back.count).isEqualTo(7);
    }

    @Test
    void payloadAliasDeserializesCompatibleOldTypeTag() {
        var aliases = TypeNameAliases.builder()
                .alias("com.example.OldPayload", SamplePayload.class.getName())
                .build();
        var aliased = new JsonJobSerializer(aliases);

        Object back =
                aliased.deserializeArgument(new JobArgument("com.example.OldPayload", "{\"name\":\"w\",\"count\":3}"));

        assertThat(back).isInstanceOf(SamplePayload.class);
        assertThat(((SamplePayload) back).name).isEqualTo("w");
    }

    @Test
    void payloadMigrationRewritesOldJsonBeforeDeserialization() {
        var migrations = PayloadMigrations.builder()
                .migration(
                        "com.example.LegacyPayload",
                        old -> new JobArgument(SamplePayload.class.getName(), "{\"name\":\"migrated\",\"count\":9}"))
                .build();
        var migrating = new JsonJobSerializer(JsonJobSerializer.defaultMapper(), TypeNameAliases.empty(), migrations);

        JobArgument migrated =
                migrating.migrateArgument(new JobArgument("com.example.LegacyPayload", "{\"ignored\":true}"));
        SamplePayload back = migrating.deserializePayload(migrated, SamplePayload.class);

        assertThat(migrated.typeTag()).isEqualTo(SamplePayload.class.getName());
        assertThat(back.name).isEqualTo("migrated");
        assertThat(back.count).isEqualTo(9);
    }
}
