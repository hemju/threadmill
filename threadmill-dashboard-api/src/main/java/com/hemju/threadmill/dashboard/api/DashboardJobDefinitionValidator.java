package com.hemju.threadmill.dashboard.api;

import com.hemju.threadmill.core.spec.JobSpec;

/**
 * Validates a job definition supplied through the dashboard before it is persisted.
 *
 * <p>Changing a handler or its serialized payload is a code-execution boundary, not a routine
 * scheduling edit. Framework adapters should provide a validator that understands their handler
 * and serialization model. The portable service denies definition replacement when no validator
 * is configured; queue, priority, and schedule replacement remain available.
 */
@FunctionalInterface
public interface DashboardJobDefinitionValidator {

  /** Validate {@code replacement}; throw a {@link DashboardApiException} when it is unsafe. */
  void validate(JobSpec replacement);

  /** Secure default for adapters that have not supplied handler and payload validation. */
  static DashboardJobDefinitionValidator denyAll() {
    return DenyAllDashboardJobDefinitionValidator.INSTANCE;
  }
}

final class DenyAllDashboardJobDefinitionValidator implements DashboardJobDefinitionValidator {

  static final DashboardJobDefinitionValidator INSTANCE =
      new DenyAllDashboardJobDefinitionValidator();

  private DenyAllDashboardJobDefinitionValidator() {}

  @Override
  public void validate(JobSpec replacement) {
    throw DashboardApiException.unsupported(
        "executable-definition editing is disabled because no DashboardJobDefinitionValidator is configured");
  }
}
