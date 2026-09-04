package com.hemju.threadmill.dashboard.api;

/** Receives operator-action audit events. */
@FunctionalInterface
public interface DashboardAuditSink {

  void record(DashboardAuditEvent event);

  static DashboardAuditSink noop() {
    return NoopDashboardAuditSink.INSTANCE;
  }
}

enum NoopDashboardAuditSink implements DashboardAuditSink {
  INSTANCE;

  @Override
  public void record(DashboardAuditEvent event) {}
}
