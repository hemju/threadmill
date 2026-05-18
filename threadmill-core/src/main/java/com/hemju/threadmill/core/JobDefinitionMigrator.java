package com.hemju.threadmill.core;

import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;

import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.core.store.JobStore;

/**
 * Utility for rewriting already-persisted, non-running job definitions after
 * handler or payload classes have been renamed or structurally migrated.
 */
public final class JobDefinitionMigrator {

    private final JobStore store;

    public JobDefinitionMigrator(JobStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    /**
     * Rewrite jobs whose handler signature is {@code oldHandlerType}. Only
     * {@code ENQUEUED}, {@code SCHEDULED}, and {@code AWAITING} jobs are
     * changed; running and terminal jobs are left untouched.
     *
     * @return number of jobs successfully rewritten
     */
    public long migrateHandlerSignature(
            String oldHandlerType, String newHandlerType, UnaryOperator<JobSpec> specMigration, int max) {
        Objects.requireNonNull(oldHandlerType, "oldHandlerType");
        Objects.requireNonNull(newHandlerType, "newHandlerType");
        Objects.requireNonNull(specMigration, "specMigration");
        if (max <= 0) return 0L;
        // The by-handler lookup includes running and terminal jobs. Fetch the
        // whole signature set before applying the batch limit, otherwise an
        // old non-replaceable row can permanently hide later pending jobs.
        List<Job> candidates = store.findByHandlerSignature(oldHandlerType, Integer.MAX_VALUE);
        long migrated = 0L;
        for (Job job : candidates) {
            if (migrated >= max) {
                break;
            }
            if (!isReplaceable(job.currentState())) {
                continue;
            }
            JobSpec migratedSpec = Objects.requireNonNull(specMigration.apply(job.spec()), "migratedSpec");
            if (!newHandlerType.equals(migratedSpec.handlerType())) {
                migratedSpec = new JobSpec(
                        newHandlerType, migratedSpec.arguments(), migratedSpec.dedupKey(), migratedSpec.dedupTtl());
            }
            if (store.replaceJob(job.id(), job.version(), JobReplacement.ofSpec(migratedSpec))) {
                migrated++;
            }
        }
        return migrated;
    }

    private static boolean isReplaceable(JobState state) {
        return state == JobState.ENQUEUED || state == JobState.SCHEDULED || state == JobState.AWAITING;
    }
}
