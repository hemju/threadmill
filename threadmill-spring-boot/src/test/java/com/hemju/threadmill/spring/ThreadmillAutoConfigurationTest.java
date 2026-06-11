package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.ZoneId;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.hemju.threadmill.core.engine.LocalWakeBus;
import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.engine.QueueLane;
import com.hemju.threadmill.core.engine.RemoteWakeChannel;
import com.hemju.threadmill.core.handler.JobAction;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.handler.NoPayload;
import com.hemju.threadmill.core.schedule.CronExpression;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.store.JobStore;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

class ThreadmillAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    ThreadmillRedisAutoConfiguration.class,
                    ThreadmillPostgresAutoConfiguration.class,
                    ThreadmillAutoConfiguration.class))
            .withPropertyValues("threadmill.enabled=false");

    @Test
    void defaultsToTransactionAwareJobScheduler() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(JobScheduler.class)).isInstanceOf(TransactionAwareJobScheduler.class);
        });
    }

    @Test
    void immediateEnqueueModeUsesPlainJobScheduler() {
        contextRunner
                .withPropertyValues("threadmill.spring.enqueue-mode=immediate")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(JobScheduler.class))
                            .isInstanceOf(JobScheduler.class)
                            .isNotInstanceOf(TransactionAwareJobScheduler.class);
                });
    }

    @Test
    void joinTransactionModeFailsFastWithoutPostgresStore() {
        contextRunner
                .withPropertyValues("threadmill.spring.enqueue-mode=join_transaction")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasMessageContaining("join_transaction requires");
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
                .withConfiguration(AutoConfigurations.of(
                        ThreadmillRedisAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
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
                .withConfiguration(AutoConfigurations.of(
                        ThreadmillRedisAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    ProcessingNode node = context.getBean(ProcessingNode.class);
                    assertThat(node.lanes()).extracting(QueueLane::queue).containsExactly("default");
                });
    }

    @Test
    void processingNodeDoesNotDuplicateDefaultLaneWhenHandlerUsesDefaultQueue() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ThreadmillRedisAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
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
                .withConfiguration(AutoConfigurations.of(
                        ThreadmillRedisAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
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
                .withConfiguration(AutoConfigurations.of(
                        ThreadmillRedisAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
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
                .withConfiguration(AutoConfigurations.of(
                        ThreadmillRedisAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
                .withBean(NamedRecurringHandler.class)
                .run(context -> {
                    var registrar = context.getBean(ThreadmillRecurringRegistrar.class);
                    assertThat(registrar.recurring())
                            .singleElement()
                            .satisfies(r -> assertThat(r.recurring().name()).isEqualTo("locked-name"));
                });
    }

    @Test
    void recurringNamespaceDeletesStaleOwnedTasks() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ThreadmillRedisAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
                .withBean(JobStore.class, () -> {
                    var store = new InMemoryJobStore();
                    store.upsertCronTask(testCronTask("stale-task"));
                    store.recordCronTaskOwnership("billing", "stale-task");
                    return store;
                })
                .withBean(RecurringIntervalHandler.class)
                .withPropertyValues("spring.application.name=billing", "threadmill.enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    JobStore store = context.getBean(JobStore.class);
                    assertThat(store.findCronTask("stale-task")).isEmpty();
                    assertThat(store.listCronTaskNamesOwnedBy("billing"))
                            .containsExactly(RecurringIntervalHandler.class.getName());
                });
    }

    @Test
    void explicitRecurringNameReconcilesWithoutCreatingSecondTask() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ThreadmillRedisAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
                .withBean(JobStore.class, () -> {
                    var store = new InMemoryJobStore();
                    store.upsertCronTask(testCronTask("locked-name"));
                    store.recordCronTaskOwnership("billing", "locked-name");
                    return store;
                })
                .withBean(NamedRecurringHandler.class)
                .withPropertyValues("threadmill.spring.recurring-namespace=billing", "threadmill.enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    JobStore store = context.getBean(JobStore.class);
                    assertThat(store.findCronTask("locked-name")).isPresent();
                    assertThat(store.listCronTasks()).extracting(CronTask::name).containsExactly("locked-name");
                });
    }

    @Test
    void missingRecurringNamespaceLeavesStaleTasksUntouched() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ThreadmillRedisAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
                .withBean(JobStore.class, () -> {
                    var store = new InMemoryJobStore();
                    store.upsertCronTask(testCronTask("stale-task"));
                    store.recordCronTaskOwnership("billing", "stale-task");
                    return store;
                })
                .withBean(RecurringIntervalHandler.class)
                .withPropertyValues("threadmill.enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(JobStore.class).findCronTask("stale-task"))
                            .isPresent();
                });
    }

    @Test
    void renamingARecurringHandlerWithoutAnExplicitNameDeletesTheOldOwnedTaskAndRegistersTheNew() {
        // Simulates: a previous deployment registered a @Recurring handler whose default name was
        // com.example.OldReportHandler. The class was renamed to com.example.NewReportHandler and
        // the new deployment registers it again. Without an explicit recurringName, the old row
        // is now an orphan that would fire forever. The auto-prune path must delete it.
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ThreadmillRedisAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
                .withBean(JobStore.class, () -> {
                    var store = new InMemoryJobStore();
                    store.upsertCronTask(testCronTask("com.example.OldReportHandler"));
                    store.recordCronTaskOwnership("billing", "com.example.OldReportHandler");
                    return store;
                })
                .withBean(RecurringIntervalHandler.class)
                .withPropertyValues("spring.application.name=billing", "threadmill.enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    JobStore store = context.getBean(JobStore.class);
                    String currentHandlerName = RecurringIntervalHandler.class.getName();
                    assertThat(store.findCronTask("com.example.OldReportHandler"))
                            .isEmpty();
                    assertThat(store.findCronTask(currentHandlerName)).isPresent();
                    assertThat(store.listCronTaskNamesOwnedBy("billing")).containsExactly(currentHandlerName);
                });
    }

    @Test
    void intervalAndCronTogetherFailsStartup() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ThreadmillRedisAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
                .withBean(BothScheduleFieldsHandler.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasMessageContaining("both interval and cron");
                });
    }

    @Test
    void jobActionIsRegisteredAsNoPayloadJobHandler() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ThreadmillRedisAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
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
    void multipleAnnotationDrivenRecurringJobActionsAreSupported() {
        // Regression for the registry's old "one handler per payload type" rule, which
        // tripped on the second @Recurring JobAction because every JobAction declares
        // the shared NoPayload type. Two JobActions on distinct queues must both
        // register and the registrar must materialize one CronTask per handler.
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ThreadmillRedisAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
                .withBean(SomeAction.class)
                .withBean(OtherAction.class)
                .withPropertyValues("threadmill.enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var registry = context.getBean(ThreadmillJobRegistry.class);
                    assertThat(registry.registrations())
                            .extracting(r -> r.handlerType().getSimpleName())
                            .containsExactlyInAnyOrder("OtherAction", "SomeAction");
                    assertThat(registry.registrationFor(SomeAction.class).queue())
                            .isEqualTo("alpha");
                    assertThat(registry.registrationFor(OtherAction.class).queue())
                            .isEqualTo("beta");
                    JobStore store = context.getBean(JobStore.class);
                    assertThat(store.listCronTasks())
                            .extracting(CronTask::handlerType)
                            .containsExactlyInAnyOrder(SomeAction.class.getName(), OtherAction.class.getName());
                });
    }

    @Test
    void duplicateRecurringNamesFailStartupWithBothHandlerNames() {
        // Recurring tasks are keyed by name in the store; a duplicate
        // recurringName previously last-won silently and one schedule never
        // ran. Mirrors the payload-collision startup failure.
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ThreadmillRedisAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
                .withBean(FirstClashingAction.class)
                .withBean(SecondClashingAction.class)
                .withPropertyValues("threadmill.enabled=false")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .rootCause()
                            .hasMessageContaining("shared-name")
                            .hasMessageContaining(FirstClashingAction.class.getName())
                            .hasMessageContaining(SecondClashingAction.class.getName());
                });
    }

    @Test
    void enqueueWakesLocalDispatcherViaWakeBus() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ThreadmillRedisAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
                .withBean(QueueAHandler.class)
                .withPropertyValues("threadmill.spring.enqueue-mode=immediate")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var bus = context.getBean(LocalWakeBus.class);
                    var calls = new CopyOnWriteArrayList<String>();
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
    void localWakePublishesToConfiguredRemoteWakeChannel() {
        var remote = new RecordingRemoteWakeChannel();
        contextRunner
                .withBean(RemoteWakeChannel.class, () -> remote)
                .withBean(QueueAHandler.class)
                .withPropertyValues("threadmill.spring.enqueue-mode=immediate")
                .run(context -> {
                    assertThat(context).hasNotFailed();

                    var scheduler = context.getBean(JobScheduler.class);
                    scheduler.enqueue(QueueAHandler.class, new PayloadA());

                    assertThat(remote.published).contains("alpha");
                    assertThat(remote.started.get()).isZero();
                });
    }

    @Test
    void remoteWakeDisabledDoesNotCreateChannel() {
        contextRunner.withPropertyValues("threadmill.remote-wake.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(RemoteWakeChannel.class);
        });
    }

    @Test
    void redisResetOnStartRequiresExplicitDestructiveResetFlag() {
        contextRunner
                .withPropertyValues(
                        "threadmill.store.redis.uri=redis://localhost:1", "threadmill.store.redis.reset-on-start=true")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasMessageContaining("allow-destructive-reset=true");
                });
    }

    @Test
    void inMemoryStoreDoesNotAutoCreateRemoteWakeChannel() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(RemoteWakeChannel.class);
        });
    }

    @Test
    void userProvidedWakeChannelIsClosedOnlyByItsOwnBeanDestruction() {
        var channel = new RecordingRemoteWakeChannel();
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ThreadmillAutoConfiguration.class))
                .withBean(RemoteWakeChannel.class, () -> channel)
                .withPropertyValues("threadmill.enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(channel.closeCount.get()).isZero();
                });
        // The runner's context close destroys the user bean exactly once;
        // historically Threadmill's wrapper destroy added extra closes on a
        // bean it does not own.
        assertThat(channel.closeCount.get()).isEqualTo(1);
    }

    @Test
    void managedWakeChannelClosesExactlyOnceAcrossLifecycleAndDestroy() {
        var channel = new RecordingRemoteWakeChannel();
        var managed = ThreadmillRemoteWakeChannels.ofManaged(channel);
        // SmartLifecycle stop and the inferred destroy method both call close().
        managed.close();
        managed.close();
        assertThat(channel.closeCount.get()).isEqualTo(1);
    }

    @Test
    void remoteWakeLifecycleStartsListenerOnlyWhenNodeRuns() {
        var remote = new RecordingRemoteWakeChannel();
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ThreadmillRedisAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
                .withBean(RemoteWakeChannel.class, () -> remote)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(remote.started.get()).isEqualTo(1);

                    remote.wakeSink.accept("default");

                    assertThat(remote.receivedBySink).contains("default");
                });
    }

    @Test
    void rawJobHandlerIsRejectedWithGuidance() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ThreadmillRedisAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
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
                .withConfiguration(AutoConfigurations.of(
                        ThreadmillRedisAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
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
                .withConfiguration(AutoConfigurations.of(
                        ThreadmillRedisAutoConfiguration.class,
                        ThreadmillPostgresAutoConfiguration.class,
                        ThreadmillAutoConfiguration.class))
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

    private static final class RecordingRemoteWakeChannel implements RemoteWakeChannel {
        private final CopyOnWriteArrayList<String> published = new CopyOnWriteArrayList<>();
        private final CopyOnWriteArrayList<String> receivedBySink = new CopyOnWriteArrayList<>();
        private final AtomicInteger started = new AtomicInteger();
        private final AtomicInteger closeCount = new AtomicInteger();
        private Consumer<String> wakeSink;

        @Override
        public void publish(String queue) {
            published.add(queue);
        }

        @Override
        public void start(Consumer<String> wakeSink) {
            this.wakeSink = queue -> {
                wakeSink.accept(queue);
                receivedBySink.add(queue);
            };
            started.incrementAndGet();
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }
    }

    private static CronTask testCronTask(String name) {
        return new CronTask(
                name,
                new CronTask.Trigger.CronExpr(CronExpression.parse("* * * * *")),
                "com.example.OldHandler",
                new JobArgument(NoPayload.class.getName(), "{}"),
                "default",
                0,
                CronTask.MissedRunPolicy.DROP,
                ZoneId.of("UTC"),
                true);
    }

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
    @Recurring(interval = "PT5S", recurringName = "shared-name")
    static final class FirstClashingAction implements JobAction {
        @Override
        public void run(JobExecutionContext ctx) {}
    }

    @Job(queue = "beta")
    @Recurring(interval = "PT7S", recurringName = "shared-name")
    static final class SecondClashingAction implements JobAction {
        @Override
        public void run(JobExecutionContext ctx) {}
    }

    @Job(queue = "alpha")
    @Recurring(interval = "PT5S")
    static final class SomeAction implements JobAction {
        @Override
        public void run(JobExecutionContext ctx) {}
    }

    @Job(queue = "beta")
    @Recurring(interval = "PT7S")
    static final class OtherAction implements JobAction {
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
