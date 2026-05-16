package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;

class ThreadmillAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ThreadmillAutoConfiguration.class))
            .withPropertyValues("threadmill.enabled=false");

    @Test
    void defaultsToTransactionAwareJobEnqueuer() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(JobEnqueuer.class)).isInstanceOf(TransactionAwareJobEnqueuer.class);
        });
    }

    @Test
    void enqueueAfterCommitFalseUsesPlainJobEnqueuer() {
        contextRunner
                .withPropertyValues("threadmill.spring.enqueue-after-commit=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(JobEnqueuer.class))
                            .isInstanceOf(JobEnqueuer.class)
                            .isNotInstanceOf(TransactionAwareJobEnqueuer.class);
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

    public static final class Payload implements JobPayload {}

    @ThreadmillJob
    static final class FirstHandler implements JobHandler<Payload> {
        @Override
        public void run(Payload payload, JobExecutionContext ctx) {}
    }

    @ThreadmillJob
    static final class SecondHandler implements JobHandler<Payload> {
        @Override
        public void run(Payload payload, JobExecutionContext ctx) {}
    }
}
