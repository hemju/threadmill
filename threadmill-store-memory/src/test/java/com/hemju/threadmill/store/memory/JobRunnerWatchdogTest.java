package com.hemju.threadmill.store.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.NodeId;
import com.hemju.threadmill.core.engine.JobInterceptors;
import com.hemju.threadmill.core.engine.JobRunner;
import com.hemju.threadmill.core.engine.ProcessingNodeConfig;
import com.hemju.threadmill.core.handler.ReflectiveJobHandlerResolver;
import com.hemju.threadmill.core.serialization.JsonJobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;

/**
 * The per-job timeout watchdog uses a ScheduledThreadPoolExecutor with
 * removeOnCancelPolicy enabled, so a completed job's cancelled watchdog (whose
 * initial delay can be minutes out) is dropped from the delay queue immediately
 * instead of retaining the captured Job graph until its scheduled time.
 */
class JobRunnerWatchdogTest {

    private final JsonJobSerializer serializer = new JsonJobSerializer();

    @Test
    @DisplayName("cancelled watchdog tasks are removed from the delay queue, not retained")
    void cancelledWatchdogTasksDoNotAccumulate() throws Exception {
        var store = new InMemoryJobStore();
        var nodeId = NodeId.newId();
        var runner = new JobRunner(
                store,
                nodeId,
                new ReflectiveJobHandlerResolver(),
                serializer,
                new JobInterceptors(),
                ProcessingNodeConfig.builder().jobTimeout(Duration.ofMinutes(5)).build());
        try {
            var field = JobRunner.class.getDeclaredField("timeoutExecutor");
            field.setAccessible(true);
            var executor = (ScheduledThreadPoolExecutor) field.get(runner);
            assertThat(executor.getRemoveOnCancelPolicy()).isTrue();

            for (int i = 0; i < 200; i++) {
                JobArgument arg = serializer.serializePayload(new EngineTestHandlers.HelloPayload("x"));
                Job job = Job.builder()
                        .spec(new JobSpec(EngineTestHandlers.CountingHandler.class.getName(), List.of(arg)))
                        .build();
                store.insert(job);
                Job claimed =
                        store.claimReady(nodeId, "default", 1, Instant.now()).get(0);
                runner.run(claimed);
            }

            // With removeOnCancelPolicy, each completed job's watchdog leaves the
            // queue at once; it would otherwise hold ~200 entries for 5 minutes.
            await().atMost(Duration.ofSeconds(5))
                    .until(() -> executor.getQueue().isEmpty());
            assertThat(executor.getQueue()).isEmpty();
        } finally {
            runner.shutdown();
        }
    }
}
