package com.hemju.threadmill.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Component;

import com.hemju.threadmill.core.engine.ProcessingNode;
import com.hemju.threadmill.core.handler.JobExecutionContext;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;

/**
 * Boots a Spring context, enqueues a job through the auto-configured
 * {@link JobScheduler}, and asserts the auto-configured {@link ProcessingNode}
 * runs it. Also asserts the handler is resolved as a Spring bean.
 */
@SpringBootTest(
        classes = SpringBootIntegrationTest.TestApp.class,
        properties = {
            "threadmill.workerCount=2",
            "threadmill.pollInterval=PT0.05S",
            "threadmill.jobTimeout=PT2S",
            "threadmill.store.memory.enabled=true",
            // spring-boot-jdbc is on the test classpath for the ordering
            // regression; this in-memory app must not auto-configure a pool.
            "spring.autoconfigure.exclude=org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration"
        })
class SpringBootIntegrationTest {

    @Autowired
    JobScheduler enqueuer;

    @Autowired
    ProcessingNode node;

    @Autowired
    TestHandler handler;

    @Test
    void contextStartsAndJobIsProcessed() {
        assertThat(node).isNotNull();
        assertThat(enqueuer).isNotNull();
        enqueuer.enqueue(TestHandler.class, new HelloPayload("spring"));
        await().atMost(Duration.ofSeconds(5)).until(() -> handler.invocations.contains("spring"));
    }

    public static final class HelloPayload implements JobPayload {
        public String tag;

        public HelloPayload() {}

        public HelloPayload(String tag) {
            this.tag = tag;
        }
    }

    @Component
    @Job(queue = "default", timeout = "PT2S")
    public static final class TestHandler implements JobHandler<HelloPayload> {
        public final ConcurrentLinkedQueue<String> invocations = new ConcurrentLinkedQueue<>();

        @Override
        public void run(HelloPayload p, JobExecutionContext c) {
            invocations.add(p.tag);
        }
    }

    @SpringBootConfiguration
    @AutoConfigurationPackage
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    @ComponentScan(basePackageClasses = {SpringBootIntegrationTest.TestHandler.class})
    public static class TestApp {}
}
