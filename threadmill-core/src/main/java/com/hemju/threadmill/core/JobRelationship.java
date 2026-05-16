package com.hemju.threadmill.core;

import java.util.Objects;

/**
 * Optional link from a job to a related job. The job model carries
 * relationships <strong>from day one</strong> so that workflows, batches,
 * and external-trigger jobs can be added in later phases without a model
 * migration.
 *
 * @param parentId the related job's id
 * @param kind     the nature of the relationship
 */
public record JobRelationship(JobId parentId, Kind kind) {

    public JobRelationship {
        Objects.requireNonNull(parentId, "parentId");
        Objects.requireNonNull(kind, "kind");
    }

    /** The defined relationship kinds. */
    public enum Kind {
        /** The job is one step in a workflow rooted at the parent. */
        WORKFLOW_STEP,

        /** The job is a member of a batch; the parent represents the batch. */
        BATCH_MEMBER,

        /** The job is waiting on an external trigger held by the parent. */
        EXTERNAL_TRIGGER,

        /** The job is a continuation of the parent (custom). */
        CONTINUATION,
    }
}
