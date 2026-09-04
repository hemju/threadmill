package com.hemju.threadmill.dashboard.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.dashboard.api.DashboardApiException;

class SpringDashboardJobDefinitionValidatorTest {

  private final SpringDashboardJobDefinitionValidator validator =
      new SpringDashboardJobDefinitionValidator(
          getClass().getClassLoader(), Optional.of(new JsonJobSerializer()));

  @Test
  void compatiblePayloadIsAccepted() {
    var spec = JobSpec.of(
        ReportHandler.class.getName(),
        new JobArgument(ReportPayload.class.getName(), "{\"name\":\"daily\"}"));

    assertThatCode(() -> validator.validate(spec)).doesNotThrowAnyException();
  }

  @Test
  void concretePayloadSubtypeIsAcceptedForABaseTypeHandler() {
    var spec = JobSpec.of(
        BasePayloadHandler.class.getName(),
        new JobArgument(ConcretePayload.class.getName(), "{\"name\":\"daily\"}"));

    assertThatCode(() -> validator.validate(spec)).doesNotThrowAnyException();
  }

  @Test
  void incompatiblePayloadTypeIsRejectedBeforePersistence() {
    var spec = JobSpec.of(
        ReportHandler.class.getName(),
        new JobArgument(OtherPayload.class.getName(), "{\"count\":1}"));

    assertBadRequest(() -> validator.validate(spec), "not compatible");
  }

  @Test
  void malformedPayloadAndUnsupportedArgumentCountAreRejected() {
    var malformed = JobSpec.of(
        ReportHandler.class.getName(), new JobArgument(ReportPayload.class.getName(), "not-json"));
    var tooMany = new JobSpec(
        ReportHandler.class.getName(),
        List.of(
            new JobArgument(ReportPayload.class.getName(), "{\"name\":\"first\"}"),
            new JobArgument(ReportPayload.class.getName(), "{\"name\":\"second\"}")));

    assertBadRequest(() -> validator.validate(malformed), "cannot be deserialized");
    assertBadRequest(() -> validator.validate(tooMany), "exactly one payload argument");
  }

  @Test
  void nonHandlerClassIsRejected() {
    var spec = JobSpec.of(
        OtherPayload.class.getName(),
        new JobArgument(OtherPayload.class.getName(), "{\"count\":1}"));

    assertBadRequest(() -> validator.validate(spec), "does not implement JobHandler");
  }

  private static void assertBadRequest(Runnable action, String messagePart) {
    assertThatThrownBy(action::run).isInstanceOf(DashboardApiException.class).satisfies(error -> {
      var failure = (DashboardApiException) error;
      assertThat(failure.code()).isEqualTo(DashboardApiException.Code.BAD_REQUEST);
      assertThat(failure.getMessage()).contains(messagePart);
    });
  }

  private record ReportPayload(String name) implements JobPayload {}

  private record OtherPayload(int count) implements JobPayload {}

  private interface BasePayload extends JobPayload {}

  private record ConcretePayload(String name) implements BasePayload {}

  private static final class ReportHandler implements JobHandler<ReportPayload> {
    @Override
    public void run(ReportPayload payload, JobExecutionContext ctx) {}
  }

  private static final class BasePayloadHandler implements JobHandler<BasePayload> {
    @Override
    public void run(BasePayload payload, JobExecutionContext ctx) {}
  }
}
