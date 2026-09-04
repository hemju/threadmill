package com.hemju.threadmill.dashboard.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.context.support.GenericApplicationContext;

import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.dashboard.api.DashboardApiException;

class SpringDashboardJobDefinitionValidatorTest {

  private final SpringDashboardJobDefinitionValidator validator =
      new SpringDashboardJobDefinitionValidator(
          getClass().getClassLoader(), new JsonJobSerializer());

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

  @Test
  void rawJobHandlerAndMissingPayloadAreRejected() {
    var rawSpec = JobSpec.of(
        RawHandler.class.getName(),
        new JobArgument(ReportPayload.class.getName(), "{\"name\":\"daily\"}"));
    var indirectRawSpec = JobSpec.of(
        IndirectRawHandler.class.getName(),
        new JobArgument(ReportPayload.class.getName(), "{\"name\":\"daily\"}"));
    var missingPayload = new JobSpec(ReportHandler.class.getName(), List.of());

    assertBadRequest(() -> validator.validate(rawSpec), "implements raw JobHandler");
    assertBadRequest(() -> validator.validate(indirectRawSpec), "implements raw JobHandler");
    assertBadRequest(() -> validator.validate(missingPayload), "requires exactly one payload");
  }

  @Test
  void missingOrAmbiguousSerializerSecurelyDisablesDefinitionReplacement() {
    var config = new ThreadmillDashboardApiConfiguration();
    var spec = JobSpec.of(
        ReportHandler.class.getName(),
        new JobArgument(ReportPayload.class.getName(), "{\"name\":\"daily\"}"));
    try (var context = new GenericApplicationContext()) {
      context.refresh();
      var disabled = config.threadmillDashboardJobDefinitionValidator(
          context, context.getBeanProvider(JobSerializer.class));

      assertUnsupported(() -> disabled.validate(spec));
    }
    try (var context = new GenericApplicationContext()) {
      context.registerBean("firstSerializer", JobSerializer.class, () -> new JsonJobSerializer());
      context.registerBean("secondSerializer", JobSerializer.class, () -> new JsonJobSerializer());
      context.refresh();
      var disabled = config.threadmillDashboardJobDefinitionValidator(
          context, context.getBeanProvider(JobSerializer.class));

      assertUnsupported(() -> disabled.validate(spec));
    }
  }

  private static void assertBadRequest(Runnable action, String messagePart) {
    assertThatThrownBy(action::run).isInstanceOf(DashboardApiException.class).satisfies(error -> {
      var failure = (DashboardApiException) error;
      assertThat(failure.code()).isEqualTo(DashboardApiException.Code.BAD_REQUEST);
      assertThat(failure.getMessage()).contains(messagePart);
    });
  }

  private static void assertUnsupported(Runnable action) {
    assertThatThrownBy(action::run).isInstanceOf(DashboardApiException.class).satisfies(error -> {
      var failure = (DashboardApiException) error;
      assertThat(failure.code()).isEqualTo(DashboardApiException.Code.NOT_SUPPORTED);
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

  @SuppressWarnings("rawtypes")
  private static final class RawHandler implements JobHandler {
    @Override
    public void run(JobPayload payload, JobExecutionContext ctx) {}
  }

  @SuppressWarnings("rawtypes")
  private interface RawHandlerBase extends JobHandler {}

  private static final class IndirectRawHandler implements RawHandlerBase {
    @Override
    public void run(JobPayload payload, JobExecutionContext ctx) {}
  }
}
