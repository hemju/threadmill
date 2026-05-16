package com.hemju.threadmill.core.engine;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import com.hemju.threadmill.core.Job;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.handler.JobExecutionContext;

/**
 * Composite that invokes a chain of {@link JobInterceptor}s in registration
 * order. Failures inside an interceptor are logged via the engine's slf4j
 * logger but never propagate — one bad interceptor can't break the engine.
 */
public final class JobInterceptors implements JobInterceptor {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(JobInterceptors.class);

    private final List<JobInterceptor> chain = new CopyOnWriteArrayList<>();

    public JobInterceptors add(JobInterceptor interceptor) {
        Objects.requireNonNull(interceptor, "interceptor");
        chain.add(interceptor);
        return this;
    }

    public List<JobInterceptor> snapshot() {
        return List.copyOf(chain);
    }

    @Override
    public void onProcessingStarting(Job job, JobExecutionContext ctx) {
        for (JobInterceptor i : chain) safe(() -> i.onProcessingStarting(job, ctx), i);
    }

    @Override
    public void onProcessingSucceeded(Job job, JobExecutionContext ctx) {
        for (JobInterceptor i : chain) safe(() -> i.onProcessingSucceeded(job, ctx), i);
    }

    @Override
    public void onProcessingFailed(Job job, JobExecutionContext ctx, Throwable cause, FailureCause kind) {
        for (JobInterceptor i : chain) safe(() -> i.onProcessingFailed(job, ctx, cause, kind), i);
    }

    @Override
    public void onStateChange(Job job, JobState from, JobState to) {
        for (JobInterceptor i : chain) safe(() -> i.onStateChange(job, from, to), i);
    }

    private static void safe(Runnable r, JobInterceptor source) {
        try {
            r.run();
        } catch (Throwable t) {
            LOG.warn("Interceptor {} threw — continuing", source.getClass().getName(), t);
        }
    }
}
