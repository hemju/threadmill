package com.hemju.threadmill.dashboard.api;

/** Permission names enforced by the dashboard API. */
public enum DashboardPermission {
    READ,
    VIEW_SENSITIVE_DETAILS,
    PAUSE_QUEUE,
    RESUME_QUEUE,
    REQUEUE_JOB,
    DELETE_JOB,
    REPLACE_JOB,
    TRIGGER_RECURRING,
    UPDATE_RECURRING,
    DELETE_RECURRING,
    ADMIN
}
