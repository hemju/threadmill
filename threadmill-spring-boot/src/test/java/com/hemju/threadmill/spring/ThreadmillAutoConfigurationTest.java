package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.QueueLane;
import com.hemju.threadmill.core.handler.JobAction;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.handler.NoPayload;
import com.hemju.threadmill.core.schedule.CronTask;

class ThreadmillAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ThreadmillAutoConfiguration.class))
            .withPropertyValues("threadmill.enabled=false");

    @Test
    void defaultsToTransactionAwareJobScheduler() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(JobScheduler.class)).isInstanceOf(TransactionAwareJobScheduler.class);
        });
    }

    @Test
    void enqueueAfterCommitFalseUsesPlainJobScheduler() {
        contextRunner
                .withPropertyValues("threadmill.spring.enqueue-after-commit=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(JobScheduler.class))
                            .isInstanceOf(JobScheduler.class)
                            .isNotInstanceOf(TransactionAwareJobScheduler.class);
                });
    }

    @Test
    void ambiguousAnnotatedHandlersFailStartupWithBothNames() {
        contextRunner.withBean(FirstHandler.class).withBean(SecondHandler.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure())
                    .hasMessageContaining("Multiple Threadmill handlers")
                    .hasMessageContaining(FirstHandler.class.getName())
                    .hasMessageContaining(SecondHandler.class.getName())
                    .hasMessageContaining(Payload.class.getName());
        });
    }

    @Test
    void processingNodeLanesAreDerivedFromAnnotatedQueues() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ThreadmillAutoConfiguration.class))
                .withBean(QueueAHandler.class)
                .withBean(QueueBHandler.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    ProcessingNode node = context.getBean(ProcessingNode.class);
                    assertThat(node.lanes())
                            .extracting(QueueLane::queue)
                            .containsExactlyInAnyOrder("default", "alpha", "beta");
                });
    }

    @Test
    void processingNodeAlwaysIncludesDefaultLaneEvenWithNoHandlers() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ThreadmillAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    ProcessingNode node = context.getBean(ProcessingNode.class);
                    assertThat(node.lanes()).extracting(QueueLane::queue).containsExactly("default");
                });
    }

    @Test
    void processingNodeDoesNotDuplicateDefaultLaneWhenHandlerUsesDefaultQueue() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ThreadmillAutoConfiguration.class))
                .withBean(DefaultQueueHandler.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    ProcessingNode node = context.getBean(ProcessingNode.class);
                    assertThat(node.lanes()).extracting(QueueLane::queue).containsExactly("default");
                });
    }

    @Test
    void intervalAnnotationRegistersRecurringHandlerAsNoPayloadTask() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ThreadmillAutoConfiguration.class))
                .withBean(RecurringIntervalHandler.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var registrar = context.getBean(ThreadmillRecurringRegistrar.class);
                    assertThat(registrar.recurring()).hasSize(1);
                    var registration = registrar.recurring().get(0);
                    assertThat(registration.recurring().name()).isEqualTo(RecurringIntervalHandler.class.getName());
                    assertThat(registration.recurring().trigger()).isInstanceOf(CronTask.Trigger.Interval.class);
                    assertThat(registration.recurring().missedRunPolicy()).isEqualTo(CronTask.MissedRunPolicy.DROP);
                });
    }

    @Test
    void cronAnnotationRegistersRecurringHandler() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ThreadmillAutoConfiguration.class))
                .withBean(RecurringCronHandler.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var registrar = context.getBean(ThreadmillRecurringRegistrar.class);
                    assertThat(registrar.recurring())
                            .singleElement()
                            .satisfies(r ->
                                    assertThat(r.recurring().trigger()).isInstanceOf(CronTask.Trigger.CronExpr.class));
                });
    }

    @Test
    void explicitRecurringNameOverridesHandlerClassName() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ThreadmillAutoConfiguration.class))
                .withBean(NamedRecurringHandler.class)
                .run(context -> {
                    var registrar = context.getBean(ThreadmillRecurringRegistrar.class);
                    assertThat(registrar.recurring())
                            .singleElement()
                            .satisfies(r -> assertThat(r.recurring().name()).isEqualTo("locked-name"));
                });
    }

    @Test
    void intervalAndCronTogetherFailsStartup() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ThreadmillAutoConfiguration.class))
                .withBean(BothScheduleFieldsHandler.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasMessageContaining("both interval and cron");
                });
    }

    @Test
    void jobActionIsRegisteredAsNoPayloadJobHandler() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ThreadmillAutoConfiguration.class))
                .withBean(SomeAction.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var registrar = context.getBean(ThreadmillRecurringRegistrar.class);
                    assertThat(registrar.recurring())
                            .singleElement()
                            .satisfies(r -> assertThat(r.payloadType()).isEqualTo(NoPayload.class));
                });
    }

    @Test
    void enqueueWakesLocalDispatcherViaWakeBus() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ThreadmillAutoConfiguration.class))
                .withBean(QueueAHandler.class)
                .withPropertyValues("threadmill.spring.enqueue-after-commit=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var bus = context.getBean(LocalWakeBus.class);
                    var calls = new java.util.concurrent.CopyOnWriteArrayList<String>();
                    bus.register(calls::add);

                    var scheduler = context.getBean(JobScheduler.class);
                    scheduler.enqueue(QueueAHandler.class, new PayloadA());

                    // The auto-config also registers ProcessingNode::wake as a sink, so we
                    // verify our captured sink saw the wake — proves the producer-side
                    // signal fired on the right queue without coupling to dispatcher internals.
                    assertThat(calls).contains("alpha");
                });
    }

    @Test
    void rawJobHandlerIsRejectedWithGuidance() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ThreadmillAutoConfiguration.class))
                .withBean(RawHandler.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasMessageContaining("implements raw JobHandler")
                            .hasMessageContaining("Implement JobAction for no-payload handlers");
                });
    }

    @Test
    void annotationOnNonNoPayloadHandlerFailsStartup() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ThreadmillAutoConfiguration.class))
                .withBean(RecurringWithCustomPayloadHandler.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasMessageContaining("Annotation-driven recurring requires JobHandler<NoPayload>");
                });
    }

    @Test
    void invalidIntervalFailsStartupAtRegistryBuild() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ThreadmillAutoConfiguration.class))
                .withBean(InvalidIntervalHandler.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasMessageContaining("invalid interval");
                });
    }

    public static final class Payload implements JobPayload {}

    public static final class PayloadA implements JobPayload {
        public String tag = "wake";
    }

    public static final class PayloadB implements JobPayload {}

    public static final class PayloadC implements JobPayload {}

    public static final class PayloadD implements JobPayload {}

    @Job
    static final class FirstHandler implements JobHandler<Payload> {
        @Override
        public void run(Payload payload, JobExecutionContext ctx) {}
    }

    @Job
    static final class SecondHandler implements JobHandler<Payload> {
        @Override
        public void run(Payload payload, JobExecutionContext ctx) {}
    }

    @Job(queue = "alpha")
    static final class QueueAHandler implements JobHandler<PayloadA> {
        @Override
        public void run(PayloadA payload, JobExecutionContext ctx) {}
    }

    @Job(queue = "beta")
    static final class QueueBHandler implements JobHandler<PayloadB> {
        @Override
        public void run(PayloadB payload, JobExecutionContext ctx) {}
    }

    @Job
    static final class DefaultQueueHandler implements JobHandler<PayloadC> {
        @Override
        public void run(PayloadC payload, JobExecutionContext ctx) {}
    }

    @Job
    @Recurring(interval = "PT5S")
    static final class RecurringIntervalHandler implements JobHandler<NoPayload> {
        @Override
        public void run(NoPayload payload, JobExecutionContext ctx) {}
    }

    @Job
    @Recurring(cron = "*/5 * * * *")
    static final class RecurringCronHandler implements JobHandler<NoPayload> {
        @Override
        public void run(NoPayload payload, JobExecutionContext ctx) {}
    }

    @Job
    @Recurring(interval = "PT5S", recurringName = "locked-name")
    static final class NamedRecurringHandler implements JobHandler<NoPayload> {
        @Override
        public void run(NoPayload payload, JobExecutionContext ctx) {}
    }

    @Job
    @Recurring(interval = "PT5S", cron = "*/5 * * * *")
    static final class BothScheduleFieldsHandler implements JobHandler<NoPayload> {
        @Override
        public void run(NoPayload payload, JobExecutionContext ctx) {}
    }

    @Job
    @Recurring(interval = "PT5S")
    static final class RecurringWithCustomPayloadHandler implements JobHandler<PayloadD> {
        @Override
        public void run(PayloadD payload, JobExecutionContext ctx) {}
    }

    @Job
    @Recurring(interval = "ten-seconds")
    static final class InvalidIntervalHandler implements JobHandler<NoPayload> {
        @Override
        public void run(NoPayload payload, JobExecutionContext ctx) {}
    }

    @Job(queue = "alpha")
    @Recurring(interval = "PT5S")
    static final class SomeAction implements JobAction {
        @Override
        public void run(JobExecutionContext ctx) {}
    }

    @Job
    @SuppressWarnings("rawtypes")
    static final class RawHandler implements JobHandler {
        @Override
        public void run(JobPayload payload, JobExecutionContext ctx) {}
    }
}
