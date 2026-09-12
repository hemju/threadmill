package com.hemju.threadmill.core.serialization;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class LegacyWireCompatibilityTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "ready",
        "scheduled",
        "processing",
        "failed",
        "succeeded-parent",
        "awaiting-child",
        "deleted",
        "quarantined"
      })
  void version030WireRetainsEveryHistoricalFieldAndDefaultsNewRevisions(String name)
      throws Exception {
    String wire;
    try (var resource = getClass()
        .getResourceAsStream("/com/hemju/threadmill/test/compatibility/v0.3.0/" + name + ".json")) {
      assertThat(resource).isNotNull();
      wire = new String(resource.readAllBytes(), StandardCharsets.UTF_8);
    }
    var serializer = new JsonJobSerializer();
    var job = serializer.deserializeJob(wire);
    assertThat(job.version()).isEqualTo(7);
    assertThat(job.executionRevision()).isZero();
    assertThat(job.failureDecision()).isEmpty();
    assertThat(job.metadata().get("fixture")).contains("v0.3.0 😀");
    var mapper = new ObjectMapper();
    var original = mapper.readTree(wire);
    var upgraded = (ObjectNode) mapper.readTree(serializer.serializeJob(job.snapshot(), 262144));
    upgraded.remove("executionRevision");
    upgraded.remove("failureDecision");
    assertThat(upgraded).isEqualTo(original);
  }
}
