package com.hemju.threadmill.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.schedule.CronTaskId;

/**
 * Regression for the typed-ID JSON fix (the dashboard "React error #31"
 * bug): {@code JobId}, {@code NodeId}, and {@code CronTaskId} must
 * serialize as bare string scalars and round-trip through a plain
 * {@link ObjectMapper} — the {@code @JsonCreator}-on-compact-constructor
 * shape sits on Jackson's historically version-sensitive
 * PROPERTIES-vs-DELEGATING creator heuristic, so the wire form is pinned
 * here.
 */
class TypedIdJsonTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void typedIdsSerializeAsBareStringsAndRoundTrip() throws Exception {
        var jobId = JobId.newId();
        var nodeId = NodeId.newId();
        var cronTaskId = new CronTaskId("nightly-report");

        String jobJson = mapper.writeValueAsString(jobId);
        String nodeJson = mapper.writeValueAsString(nodeId);
        String cronJson = mapper.writeValueAsString(cronTaskId);

        // The wire form is a bare string scalar, never an object.
        assertThat(jobJson).isEqualTo("\"" + jobId + "\"");
        assertThat(nodeJson).isEqualTo("\"" + nodeId + "\"");
        assertThat(cronJson).isEqualTo("\"nightly-report\"");

        assertThat(mapper.readValue(jobJson, JobId.class)).isEqualTo(jobId);
        assertThat(mapper.readValue(nodeJson, NodeId.class)).isEqualTo(nodeId);
        assertThat(mapper.readValue(cronJson, CronTaskId.class)).isEqualTo(cronTaskId);
    }

    @Test
    void typedIdsRoundTripAsMapValuesAndRecordComponents() throws Exception {
        // Pins the embedded shape dashboards actually emit: ids nested in a
        // containing structure must stay string scalars there too.
        record Holder(JobId job, NodeId node, CronTaskId task) {}
        var holder = new Holder(JobId.newId(), NodeId.newId(), new CronTaskId("t"));

        String json = mapper.writeValueAsString(holder);
        assertThat(json)
                .contains("\"job\":\"" + holder.job() + "\"")
                .contains("\"node\":\"" + holder.node() + "\"")
                .contains("\"task\":\"t\"");

        Holder back = mapper.readValue(json, Holder.class);
        assertThat(back).isEqualTo(holder);
    }
}
