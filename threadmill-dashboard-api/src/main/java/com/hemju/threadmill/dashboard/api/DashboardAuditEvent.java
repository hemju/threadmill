package com.hemju.threadmill.dashboard.api;

import java.time.Instant;

/** Immutable audit event for a dashboard mutation. */
public record DashboardAuditEvent(
        Instant at, String actor, DashboardPermission permission, String action, String target, String outcome) {}
