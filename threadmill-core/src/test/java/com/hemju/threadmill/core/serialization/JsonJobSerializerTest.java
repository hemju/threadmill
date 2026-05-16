package com.hemju.threadmill.core.serialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;

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
}
