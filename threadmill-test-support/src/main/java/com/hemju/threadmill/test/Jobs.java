package com.hemju.threadmill.test;

import java.time.Instant;

import com.hemju.threadmill.core.ConcurrencyMode;
import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobRelationship;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;

/**
 * Tiny factory for jobs used across the contract suite. Keeps the tests
 * focused on the contract rather than on how to build a job.
 */
public final class Jobs {

    private Jobs() {}

    public static Job enqueued(String handler) {
        return Job.builder()
                .spec(JobSpec.of(handler, new JobArgument("java.lang.String", "\"hello\"")))
                .build();
    }

    public static Job scheduled(String handler, Instant scheduledFor) {
        Job j = Job.builder()
                .spec(JobSpec.of(handler, new JobArgument("java.lang.String", "\"hello\"")))
                .initialState(JobState.SCHEDULED)
                .scheduledFor(scheduledFor)
                .createdAt(scheduledFor)
                .build();
        return j;
    }

    public static Job onQueue(String handler, String queue) {
        return Job.builder()
                .spec(JobSpec.of(handler, new JobArgument("java.lang.String", "\"hello\"")))
                .queue(queue)
                .build();
    }

    public static Job withConcurrency(String handler, String key, ConcurrencyMode mode) {
        return Job.builder()
                .spec(JobSpec.of(handler, new JobArgument("java.lang.String", "\"hello\"")))
                .concurrencyKey(key)
                .concurrencyMode(mode)
                .build();
    }

    public static Job awaitingWorkflowStep(String handler, Job parent) {
        return Job.builder()
                .spec(JobSpec.of(handler, new JobArgument("java.lang.String", "\"hello\"")))
                .relationship(new JobRelationship(parent.id(), JobRelationship.Kind.WORKFLOW_STEP))
                .initialState(JobState.AWAITING)
                .build();
    }
}
