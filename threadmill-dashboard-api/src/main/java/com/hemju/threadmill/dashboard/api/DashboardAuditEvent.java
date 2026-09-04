package com.hemju.threadmill.dashboard.api;

import java.time.Instant;

/**
 * Immutable audit event for a dashboard operation.
 *
 * <p>Job placement edits use action {@code replace_job} with {@link
 * DashboardPermission#REPLACE_JOB}; executable handler or argument edits use {@code
 * replace_job_definition} with {@link DashboardPermission#ADMIN}, making the privilege boundary
 * visible to audit consumers.
 */
public record DashboardAuditEvent(
    Instant at,
    String actor,
    DashboardPermission permission,
    String action,
    String target,
    String outcome) {}
