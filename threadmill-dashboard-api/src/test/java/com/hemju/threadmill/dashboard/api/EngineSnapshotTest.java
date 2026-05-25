package com.hemju.threadmill.dashboard.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.schedule.CronExpression;
import com.hemju.threadmill.core.schedule.CronTask;
import com.hemju.threadmill.core.schedule.CronTaskScheduleState;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.store.memory.InMemoryJobStore;

class EngineSnapshotTest {

    @Test
    void snapshotReturnsAllStatesEvenWhenZero() {
        var snap = EngineSnapshot.of(new InMemoryJobStore());
        assertThat(snap.countsByState().keySet()).contains(JobState.values());
        assertThat(snap.cronTasks()).isEmpty();
        assertThat(snap.capabilities()).isNotNull();
    }

    @Test
    void countsReflectInsertedJobs() {
        var store = new InMemoryJobStore();
        for (int i = 0; i < 3; i++) {
            store.insert(Job.builder()
                    .spec(JobSpec.of("com.example.H", new JobArgument("java.lang.String", "\"x\"")))
                    .build());
        }
        var snap = EngineSnapshot.of(store);
        assertThat(snap.count(JobState.ENQUEUED)).isEqualTo(3L);
        assertThat(snap.count(JobState.SUCCEEDED)).isZero();
    }

    @Test
    void pausedQueuesAreExposedThroughTheSnapshot() {
        var store = new InMemoryJobStore();
        store.pauseQueue("default", "ops");
        store.pauseQueue("system", null);

        var snap = EngineSnapshot.of(store);
        assertThat(snap.pausedQueues()).containsExactlyInAnyOrder("default", "system");
    }

    @Test
    void includesRegisteredCronTasks() {
        var store = new InMemoryJobStore();
        var task = new CronTask(
                "daily-report",
                new CronTask.Trigger.CronExpr(CronExpression.parse("0 9 * * 1-5")),
                "com.example.Report",
                new JobArgument("java.lang.String", "\"\""),
                "default",
                0,
                CronTask.MissedRunPolicy.DROP,
                ZoneId.of("UTC"),
                true);
        store.upsertCronTask(task);
        store.upsertCronTaskState(
                CronTaskScheduleState.initial("daily-report", Instant.now().plus(Duration.ofMinutes(5))));

        var snap = EngineSnapshot.of(store);
        assertThat(snap.cronTasks()).extracting(CronTask::name).containsExactly("daily-report");
    }
}
